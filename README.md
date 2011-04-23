riak_kv_btree_backend
=====================

This module is a storage backend for [Riak](http://wiki.basho.com),
which uses `couch_btree` to store data.  `couch_btree` is an
append-only format, and this module provides no means to do
"compaction", but that could easily be added.

Compared to other backends, this has the following properties:

- Does not keep keys in memory
- Stores keys sorted so list_buckets and list_keys_in_bucket
  have decent performance
- Is pure Erlang (makes me and Erjang happy)

As is, this could be a fine format for storing log records that you'll
never delete anyway; but if you need compaction then you'd have to
implement it.

To configure riak to use this, edit your `app.config` to use the btree
backend.

    {riak_kv, [
        {storage_backend, riak_btree_backend},
        ...

    {riak_btree_backend, [
             {data_root, "data/btree"}
        ]},

The easiest way to pull in all the right things in your riak is to
build your own, adding this to the `deps` section of riak's
`riak/rebar.config`.

    {riak_btree_backend, "0.1.*",
        {git, "git://github.com/krestenkrab/riak_btree_backend",
        {branch, "master"}}},

And, then update `riak/rel/reltool.config` to include
`riak_btree_backend` in your release.


Thanks
------

Acknowledgements go to the CouchDB team, the fine folks at Basho (the
module borrows heavily from `riak_kv_ets_backend`), and to Ulf Wiger
at Erlang Solutions for writing the beautiful little `sext` module.


Happy hacking!

Kresten