riak_btree_backend
==================

This module is a storage backend for [Riak](http://wiki.basho.com),
which uses `couch_btree` to store data.  `couch_btree` is an
append-only format, and so just like CouchDB, it runs a compaction
periodically.

Compared to other backends, this has the following properties:

- Does not keep keys in memory (that is, it uses the file system
  buffers to keep keys)
- Stores keys sorted so operations like list_buckets and
  list_keys_in_bucket have reasonable performance
- Keys and values are stored together (there is no separate index or
  hint files) which may slow down key scans.
- Is pure Erlang (makes me and Erjang happy)

Compaction should really be triggered by some measure of fragmentation
in the data file, so there is something to implement some other time.
Right now it runs once an hour, but can be configured using the
`compaction_interval` environment setting.

To configure Riak to use `riak_btree_backend`, edit your `app.config`
to use the btree backend.

    {riak_kv, [
        {storage_backend, riak_btree_backend},
        ...

    {riak_btree_backend, [
             {data_root, "data/btree"},

             %% sync strategy is one of: none (default), {seconds, N}, sync or o_sync
             {sync_strategy, o_sync},

             %% how often to do copy compaction
             {compaction_interval, {minutes, 10}}
        ]},

The easiest way to pull in all the right things in your riak is to
build your own, adding this to the `deps` section of riak's
`riak/rebar.config`.

    {riak_btree_backend, "0.1.*",
        {git, "git://github.com/krestenkrab/riak_btree_backend",
        {branch, "master"}}},

And, then update `riak/rel/reltool.config` to include
`riak_btree_backend` in your release.


License
-------

The code is Copyright (C) 2011 by [Trifork](http://www.trifork.com),
and made available under Apache 2.0 License.  Pleas credit Trifork
appropriately if you use this.


Thanks
------

Acknowledgements go to the CouchDB team, the fine folks at Basho (the
module borrows heavily from `riak_kv_ets_backend` and
`riak_kv_bitcask_backend`), and to Ulf Wiger at Erlang Solutions for
writing the beautiful little `sext` module.


Happy hacking!

Kresten