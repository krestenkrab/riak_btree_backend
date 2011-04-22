%% -------------------------------------------------------------------
%%
%% riak_kv_btree_backend: storage engine based on CouchDB B+ Tree
%%
%% Copyright (c) 2011 Trifork A/S  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc riak_kv_btree_backend is a Riak storage backend using btree.

-module(riak_kv_btree_backend).
-behavior(riak_kv_backend).
-behavior(gen_server).

-include("couch_db.hrl").
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% riak_kv_backend exports
-export([start/2,stop/1,get/2,put/3,list/1,list_bucket/2,fold_bucket_keys/4,
         delete/2,fold/3, is_empty/1, drop/1, callback/3]).

%% gen_server exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

% @type state() = term().
-record(state, {btree, path}).

% @spec start(Partition :: integer(), Config :: proplist()) ->
%                        {ok, state()} | {{error, Reason :: term()}, state()}
start(Partition, Config) ->
    gen_server:start_link(?MODULE, [Partition, Config], []).

init([Partition, Config]) ->
    ConfigRoot = proplists:get_value(riak_kv_btree_backend_root, Config),
    if ConfigRoot =:= undefined ->
            riak:stop("riak_kv_btree_backend_root unset, failing.~n");
       true -> ok
    end,

    TablePath = filename:join([ConfigRoot, integer_to_list(Partition)]),
    case filelib:ensure_dir(TablePath) of
        ok -> ok;
        _Error ->
            riak:stop("riak_kv_btree_backend could not ensure"
                      " the existence of its root directory")
    end,

    BtreeName = list_to_atom(integer_to_list(Partition)),
    BtreeFileName = filename:join(TablePath, BtreeName),

    case couch_file:open(BtreeFileName, [sys_db]) of

        {ok, Fd} -> %% open existing file
            {ok, #db_header{local_docs_btree_state = HeaderBtree}} =
                couch_file:read_header(Fd),
            {ok, Bt} = couch_btree:open(HeaderBtree, Fd, []),
            {ok, #state{ btree=Bt, path=BtreeFileName }};

        {error, enoent} ->
            case couch_file:open(BtreeFileName, [create,sys_db]) of
                {ok, Fd} ->
                    Header = #db_header{},
                    ok = couch_file:write_header(Fd, Header),
                    {ok, Bt} = couch_btree:open(nil, Fd, []),
                    {ok, #state{ btree=Bt, path=BtreeFileName }};

                {error, _} = Error ->
                    Error;
                Error ->
                    {error, Error}
            end;

        {error, _} = Error ->
            Error;
        Error ->
            {error, Error}
    end
.

%% @private
handle_cast(_, State) -> {noreply, State}.

%% @private
handle_call(get_btree,_From,State) ->
    {reply, State#state.btree, State};
handle_call(stop,_From,State) ->
    {reply, srv_stop(State), State};
handle_call({put,BKey,Val},_From,State) ->
    srv_put(State,BKey,Val);
handle_call({delete,BKey},_From,State) ->
    srv_delete(State,BKey);
handle_call(drop, _From, State) ->
    srv_drop(State).

get_btree(SrvRef) ->
    gen_server:call(SrvRef,get_btree).

optional_header_update(Bt, Bt) -> ok;
optional_header_update(#btree{fd = Fd}, Bt2) ->
    ok = couch_file:write_header(Fd,
				 #db_header{local_docs_btree_state =
						couch_btree:get_state(Bt2)}).


% @spec stop(state()) -> ok | {error, Reason :: term()}
stop(SrvRef) ->
    gen_server:call(SrvRef, stop).
srv_stop(#state{btree=#btree{fd=Fd}}) ->
    couch_file:close(Fd).

% get(state(), riak_object:bkey()) ->
%   {ok, Val :: binary()} | {error, Reason :: term()}
% key must be 160b
get(SrvRef,BKey) ->
    Bt = get_btree(SrvRef),
    Key = sext:encode(BKey),
    case couch_btree:lookup(Bt, [Key]) of
        [not_found] ->
            {error, notfound};
        [{ok, {_Key, Found}}] ->
            {ok, Found}
    end.

% put(state(), riak_object:bkey(), Val :: binary()) ->
%   ok | {error, Reason :: term()}
% key must be 160b
put(SrvRef,BKey,Val) ->
    gen_server:call(SrvRef, {put, BKey,Val}).
srv_put(#state{btree=Bt},BKey,Val) ->
    Key = sext:encode(BKey),
    {ok, Bt2} = couch_btree:add_remove(Bt, [{Key, Val}], [Key]),
    ok = optional_header_update(Bt, Bt2),
    {reply, ok, #state{btree=Bt2}}.

% delete(state(), riak_object:bkey()) ->
%   ok | {error, Reason :: term()}
% key must be 160b
delete(SrvRef,BKey) ->
    gen_server:call(SrvRef, {delete, BKey}).
srv_delete(#state{btree=Bt}, BKey) ->
    Key = sext:encode(BKey),
    {ok, Bt2} = couch_btree:add_remove(Bt, [], [Key]),
    ok = optional_header_update(Bt, Bt2),
    {reply, ok, #state{btree=Bt2}}.

% list(state()) -> [riak_object:bkey()]
list(SrvRef) ->
    fold(SrvRef,
         fun(BK, _V, Acc) ->
                 [BK | Acc]
         end,
         []).

list_bucket(SrvRef, {filter, Bucket, Fun0}) ->
    Bt = get_btree(SrvRef),
    Prefix = sext:prefix({Bucket, '_'}),
    Fun = fun({BinKey,_Value},Acc) ->
                  case sext:decode(BinKey) of
                      {Bucket, K} ->
                          case Fun0(K) of
                              true  -> {ok, [K|Acc]};
                              false -> {ok, Acc}
                          end;

                      {_, _} ->
                          {stop, Acc}
                  end
          end,
    {ok, _, Acc2} = couch_btree:fold(Bt, Fun, [], [{start_key, Prefix}]),
    Acc2;

list_bucket(SrvRef, '_') ->
    Bt = get_btree(SrvRef),
    loop_list_buckets(Bt, sext:prefix({'_','_'}), []);

list_bucket(SrvRef, Bucket) ->
    Bt = get_btree(SrvRef),
    Prefix = sext:prefix({Bucket, '_'}),
    Fun = fun({BinKey,_Value},Acc) ->
                  case sext:decode(BinKey) of
                      {Bucket, K} ->
                          {ok, [K|Acc]};
                      {_, _} ->
                          {stop, Acc}
                  end
          end,
    {ok, _, Acc2} = couch_btree:fold(Bt, Fun, [], [{start_key, Prefix}]),
    Acc2.

loop_list_buckets(Bt, From, List) ->
    Fun = fun({BinKey,_Value}, notfound) ->
                  {Bucket,_Key} = sext:decode(BinKey),
                  {stop, Bucket}
          end,

    case couch_btree:fold(Bt, Fun, notfound, [{start_key, From}]) of
        {ok, _, notfound} ->
            List;
        {ok, _, Bucket} ->
            NextFrom = sext:prefix({<<Bucket/binary,0>>,'_'}),
            loop_list_buckets(Bt,NextFrom,[Bucket|List])
    end.

fold_bucket_keys(SrvRef, Bucket, Fun0, Acc0) ->
    Bt = get_btree(SrvRef),
    Prefix = sext:prefix({Bucket, '_'}),
    Fun = fun({BinKey,_Value},Acc) ->
                  case sext:decode(BinKey) of
                      {Bucket, K} ->
                          {ok, Fun0(K,Acc)};
                      {_, _} ->
                          {stop, Acc}
                  end
          end,
    {ok, _, Acc2} = couch_btree:fold(Bt, Fun, Acc0, [{start_key, Prefix}]),
    Acc2.

fold(SrvRef,Fun0,Acc0) ->
    Bt = get_btree(SrvRef),
    Fun = fun({BinKey,V},Acc) ->
                  BK = sext:decode(BinKey),
                  {ok, Fun0(BK, V, Acc)}
          end,
    {ok, _, Acc2} = couch_btree:fold(Bt, Fun, Acc0, []),
    Acc2.

is_empty(SrvRef) ->
    try fold(SrvRef,
             fun(_, _, _) -> throw(not_empty) end,
             empty)
    of
        empty ->
            true
    catch
        _:not_empty ->
            false
    end.

drop(SrvRef) ->
    gen_server:call(SrvRef, drop).
srv_drop(#state{btree=#btree{fd=Fd}, path=P}) ->
    ok = couch_file:close(Fd),
    ok = file:delete(P),
    {reply, ok, #state{}}.

%% Ignore callbacks for other backends so multi backend works
callback(_State, _Ref, _Msg) ->
    ok.

%% @private
handle_info(_Msg, State) -> {noreply, State}.

%% @private
terminate(_Reason, _State) -> ok.

%% @private
code_change(_OldVsn, State, _Extra) -> {ok, State}.

-ifdef(TEST).
%%
%% Test
%%

simple_test() ->
    ?assertCmd("rm -rf test/btree-backend"),
    Config = [{riak_kv_btree_backend_root, "test/btree-backend"}],
    riak_kv_backend:standard_test(?MODULE, Config).

list_bucket_test() ->
    ?assertCmd("rm -rf test/btree-backend"),
    Config = [{riak_kv_btree_backend_root, "test/btree-backend"}],

    {ok, S} = ?MODULE:start(42, Config),
    ok = ?MODULE:put(S, {<<"b1">>,<<"k1">>}, <<"v1">>),
    ok = ?MODULE:put(S, {<<"b1">>,<<"k2">>}, <<"v1">>),
    ok = ?MODULE:put(S, {<<"b11">>,<<"k1">>}, <<"v1">>),
    ok = ?MODULE:put(S, {<<"b11">>,<<"k2">>}, <<"v1">>),
    ok = ?MODULE:put(S, {<<"b2">>,<<"k1">>}, <<"v2">>),
    [<<"b1">>,<<"b11">>,<<"b2">>] = lists:sort(list_bucket(S,'_')),
    [<<"k1">>,<<"k2">>] = lists:sort(list_bucket(S,<<"b11">>)).


-ifdef(EQC).

eqc_test_() ->
    {timeout, 60,
     [{"eqc test", ?_test(eqc_test_inner())}]}.

eqc_test_inner() ->
    Cleanup =
        fun(State, OldS) ->
                case State of
                    #state{} ->
                        drop(State);
                    _ ->
                        ok
                end,
                [file:delete(S#state.path) || S <- OldS]
        end,
    Config = [{riak_kv_btree_backend_root, "test/btree-backend"}],
    ?assertCmd("rm -rf test/btree-backend"),
    ?assertEqual(true, backend_eqc:test(?MODULE, false, Config, Cleanup)).
-endif. % EQC
-endif. % TEST
