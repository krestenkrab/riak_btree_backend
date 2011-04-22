riak_kv_btree_backend
=====================

This module is a storage backend for [Riak](http://wiki.basho.com),
which uses `couch_btree` to store data.  `couch_btree` is an
append-only format, and this module provides no means to do
"compaction", but that could easily be added.

Compared to other backends, this has the following properties:

- keeps no keys in memory
- is pure Erlang (makes me, and Erjang happy)

As is, this could be a fine format for storing log records that you'll
never delete anyway; but if you need compaction then you'd have to
implement it.

To configure riak to use this, edit your `app.config` to use the btree
backend.

    {riak_kv, [
        {storage_backend, riak_kv_btree_backend},
        ...

Happy hacking!

Kresten