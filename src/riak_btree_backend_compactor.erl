%% -------------------------------------------------------------------
%%
%% riak_btree_backend: storage engine based on CouchDB B+ Tree
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

%% @doc riak_btree_backend is a Riak storage backend using btree.
%% <pre>
%%  Backend                  Compactor
%%    ..
%%    ..  ------ start ------>   ..
%%
%%    ..
%%    ..                         .. <--- copy_more --+
%%    ..  --> did_put            ..                  |
%%    ..  --> did_delete         ..                  |
%%    ..                         .. -----------------+
%%    ..
%%
%%    ..  <-- finish_compaction  ..
%%    ..  --> complete_compation .. terminate
%% </pre>
%% @end

-module(riak_btree_backend_compactor).
-author('Kresten Krab Thorup <krab@trifork.com>').

-behavior(gen_server).

-include("couch_db.hrl").

%% API exports
-export([start/3, did_put/4, did_delete/3, complete_compaction/2]).

%% gen_server exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {in :: #btree{},
                out :: #btree{},
                out_name :: string(),
                next_key= <<"">> :: binary(),
                done_copying = false,
                srv :: pid()}).

start(SrvRef,Bt,FileName) ->
    gen_server:start_link(?MODULE, [SrvRef,Bt,FileName], []).

did_delete(CompactorPID, BinKey, BtIn) ->
    gen_server:cast(CompactorPID, {did_delete, BinKey, BtIn}).

did_put(CompactorPID, BinKey, BinValue, BtIn) ->
    gen_server:cast(CompactorPID, {did_put, BinKey, BinValue, BtIn}).

complete_compaction(CompactorPID, InFile) ->
    gen_server:call(CompactorPID, {complete_compaction, self(), InFile}, infinity).


init([SrvRef, BtIn, FileName]) ->
    CompactFile = FileName ++ ".compact",
    case couch_file:open(CompactFile, [create,overwrite,sys_db]) of
        {ok, FdOut} ->
            Header = #db_header{},
            ok = couch_file:write_header(FdOut, Header),
            {ok, BtOut} = couch_btree:open(nil, FdOut, []),
            gen_server:cast(self(), copy_more),
            {ok, #state{ in=BtIn, out=BtOut#btree{less=BtIn#btree.less},
                         next_key= <<>>,
                         srv=SrvRef,
                         out_name=CompactFile  }};
        {error, _} = Error ->
            Error
    end.

handle_cast({did_delete, BinKey, #btree{less=Less}=BtIn}, #state{out=BtOut,next_key=NextKey} = State) ->
    case State#state.done_copying orelse Less(BinKey, NextKey) of
        true ->
            {ok, BtOut2} = couch_btree:add_remove(BtOut, [], [BinKey]),
            {noreply, State#state{in=BtIn, out=BtOut2}};
        false ->
            {noreply, State#state{in=BtIn}}
    end;

handle_cast({did_put, BinKey, BinVal, #btree{less=Less}=BtIn}, #state{out=BtOut,next_key=NextKey}=State) ->
    case State#state.done_copying orelse Less(BinKey, NextKey) of
        true ->
            {ok, BtOut2} = couch_btree:add_remove(BtOut, [{BinKey,BinVal}], [BinKey]),
            {noreply, State#state{in=BtIn, out=BtOut2}};
        false ->
            {noreply, State#state{in=BtIn}}
    end;

handle_cast(copy_more, #state{next_key=NextKey,out=BtOut,in=BtIn}=State) ->

    Fun = fun ({K,_V},{acc, List, 0}) ->
                  {stop, {limit_exhausted, K, List}};
              (KV, {acc, List, Count}) ->
                  {ok, {acc, [KV|List], Count-1}}
          end,

    case couch_btree:fold(BtIn, Fun, {acc, [], 1000}, [{start_key, NextKey}]) of
        {ok, _, {limit_exhausted, NextStartKey, KVList}} ->
            ReverseKVList = lists:reverse(KVList),
            {ok, BtOut2} = couch_btree:add_remove(BtOut, ReverseKVList, []),
            gen_server:cast(self(), copy_more),
            {noreply, State#state{out=BtOut2, next_key=NextStartKey}};

        {ok, _, {acc, KVList,_Count}} ->
            ReverseKVList = lists:reverse(KVList),
            {ok, BtOut2} = couch_btree:add_remove(BtOut, ReverseKVList, []),
            riak_btree_backend:finish_compact(State#state.srv),
            {noreply, State#state{out=BtOut2, done_copying=true}}
    end.

handle_call({complete_compaction, SrvRef, InFile},
            _From,
            #state{out=BtOut, out_name=CompactFile, srv=SrvRef}=State) ->

    %% commit outfile
    #btree{fd=FdOut} = BtOut,
    ok = couch_file:write_header(FdOut,
				 #db_header{local_docs_btree_state =
						couch_btree:get_state(BtOut)}),
    ok = couch_file:sync(FdOut),
    ok = couch_file:close(FdOut),

    file:rename(CompactFile, InFile),

    {stop,normal,ok,State}.


%% @private
handle_info(_Msg, State) -> {noreply, State}.

%% @private
terminate(_Reason, _State) -> ok.

%% @private
code_change(_OldVsn, State, _Extra) -> {ok, State}.

