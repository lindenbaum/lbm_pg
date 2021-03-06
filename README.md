[![Build Status](https://travis-ci.org/lindenbaum/lbm_pg.png?branch=master)](https://travis-ci.org/lindenbaum/lbm_pg)

lbm_pg
======

Another approach to process groups. `lbm_pg` offers a process group
implementation similar to `pg2` without the use of `global` with integrated
facilities to (reliably) send messages to group members with failover, timeouts,
member caching and more.

Since group names can be arbitrary terms, `lbm_pg` can also act as a simple
process registry. The distribution backend is similar to `pg2` and thus has the
same robustness, e.g. when it comes to netsplits. Every process implementing
either the `gen_server` or `gen_fsm` behaviour can be joined into a group.

More Information
----------------

For more information look at the comprehensive inline EDoc documentation.

Dependencies
------------

`lbm_pg` uses [worker_pool](https://github.com/inaka/worker_pool) to pool
worker processes for asynchronous sends. Don't be afraid, the dependencies of
the `worker_pool` project itself are test only. This means you don't have to
include them in your release.

Example
-------

Join a `gen_server` into the `1337` group:
```erlang
init([]) ->
        lbm_pg:join_server(1337),
        {ok, #state{}}.
```

Handle group messages in the `gen_server`:
```erlang
handle_call(the_group_message, _From, State) ->
        {reply, the_answer, State}.
```

Send a message to a group member from an arbitrary process:
```erlang
TheAnswer = lbm_pg:sync_send(1337, the_group_message).
```

Send an asynchronous message to a group member with error feedback:
```erlang
ok = lbm_pg:send(1337, the_group_message, 100, [error_feedback]),
%% NOTE: This is a bad example for error handling
receive
        ?LBM_PG_ERROR(1337, the_group_message, Reason) -> {error, Reason}
after 1000 ->
        ok
end.
```
