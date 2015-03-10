[![Build Status](https://travis-ci.org/lindenbaum/lbm_nomq.png?branch=master)](https://travis-ci.org/lindenbaum/lbm_nomq)

lbm_nomq
========

A simple, distributed, message distribution framework with message queue
semantics (publish/subscribe), that is actually not really a message queue.
It uses Erlang terms and distributed Erlang to work across an Erlang cluster.

`lbm_nomq` is based on the principle, that messages are safest when they
reside in the originator until they have been delivered to/processed by a
subscriber. While introducing broker processes is a nice way to speed up the
pushing side, it also raises the danger of loosing every single message when the
broker process exits. So, the worst place for unprocessed messages is the
message queue of a process that is not the message originator. `lbm_nomq`
utilizes a mechanism similar to a blocking queue. The originator will be blocked
until the message has been received (and eventually handled) by exactly one
subscriber. Thus, this mechanism is well-suited for applications with many
concurrent producers that produce a moderate amount of messages each.

In a nutshell `lbm_nomq` allows sending terms over logical, topic-based
channels to subscribed MFAs. In the case, the subscribed MFAs adheres to
`gen:call/4` semantics, message distribution is guaranteed to be reliable.

It is possible to have multiple subscribers for a topic, however, `lbm_nomq`
will deliver a message to exactly one of the subscribed MFAs (randomly
chosen). This is useful to support active/active redundant subscribers
sharing a common state.

How it works
------------

To know if a message was handled successfully, an `lbm_nomq:push` is always a
synchronous operation (blocks until the applied `MFAs` return). The semantic
and mechanism of `lbm_nomq:push` is very similar to `gen_server:call/3`.

`lbm_nomq` provides total location transparency for pushers and subscribers.
Neither pushers nor subscribers know where the sender/receiver of a message is
located. A pusher does not care if a subscriber fails in the middle of message
processing, as long as there are other subscribers for the respective topic. It
is also transparent how many subscribers a certain topic has. There's no
unsubscribe in `lbm_nomq`. Dead or not available subscribers will be sorted out
automatically when a push fails. Subscribers even don't have to be processes. A
subscriber can be any `MFAs`.

`lbm_nomq` is made with distributed Erlang clusters in mind. It automatically
handles dynamic clusters (nodes joining/leaving) and has the same robustness
than `pg2' when it comes to netsplits.

For more information refer to the extensive `edoc` found in the `lbm_nomq`
module.

When to use?
------------

Of course, `lbm_nomq` is not general purpose, it is designed to give you
simple message passing by blocking senders until the message has successfully
been consumed. Therefore, `lbm_nomq` is best suited to be used by many,
concurrent pushing processes in combination with few subscribers per topic.
