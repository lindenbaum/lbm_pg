%%%=============================================================================
%%%
%%%               |  o __   _|  _  __  |_   _       _ _   (TM)
%%%               |_ | | | (_| (/_ | | |_) (_| |_| | | |
%%%
%%% @copyright (C) 2015, Lindenbaum GmbH
%%%
%%% Permission to use, copy, modify, and/or distribute this software for any
%%% purpose with or without fee is hereby granted, provided that the above
%%% copyright notice and this permission notice appear in all copies.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
%%% WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
%%% MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
%%% ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
%%% WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
%%% ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
%%% OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
%%%
%%% @doc
%%% The implementation of the synchronous send command. This code is
%%% library-only.
%%% @end
%%%=============================================================================

-module(lbm_pg_sync).

%% API
-export([send/4,
         remaining_millis/2]).

-include("lbm_pg.hrl").

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% See {@link lbm_pg:sync_send/4} for detailed documentation.
%% @end
%%------------------------------------------------------------------------------
-spec send(lbm_pg:name(), term(), timeout(), [lbm_pg:send_option()]) -> any().
send(Group, Message, Timeout, Options) ->
    Args = {Group, Message, Timeout, Options},
    sync_send_loop(members(Group, Options), [], Args, Timeout).

%%------------------------------------------------------------------------------
%% @doc
%% Calculate the remaining value for `Timeout' given a start timestamp.
%% @end
%%------------------------------------------------------------------------------
-spec remaining_millis(timeout(), erlang:timestamp()) -> timeout().
remaining_millis(infinity, _StartTimestamp) ->
    infinity;
remaining_millis(Timeout, StartTimestamp) ->
    Timeout - (to_millis(os:timestamp()) - to_millis(StartTimestamp)).

%%%=============================================================================
%%% internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
members(Group, Options) ->
    case lists:member(no_cache, Options) of
        false -> lbm_pg_dist:members(Group, true);
        true  -> shuffle(lbm_pg_dist:members(Group, false))
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
shuffle(L) when is_list(L) ->
    shuffle(L, length(L)).
shuffle(L, Len) ->
    [E || {_, E} <- lists:sort([{crypto:rand_uniform(0, Len), E} || E <- L])].

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
maybe_cache_member(Group, Member, Options) ->
    case lists:member(no_cache, Options) of
        false -> lbm_pg_dist:cache_member(Group, Member);
        true  -> ok
    end.

%%------------------------------------------------------------------------------
%% @private
%% Try to sync send a message to exactly one member. If no valid members can be
%% found, the loop will block the caller until either new members join
%% and handle the message or the given timeout expires (unless the `no_wait'
%% option is specified).
%%------------------------------------------------------------------------------
sync_send_loop([], BadMs, Args = {Group, _, _, Opts}, Timeout) ->
    ok = exit_if_no_wait(Opts, Args),
    StartTimestamp = os:timestamp(),
    case lbm_pg_dist:add_waiting(Group, BadMs) of
        {ok, SyncRef} when is_reference(SyncRef) ->
            TimerRef = send_after(Timeout, Group, SyncRef),
            NewTimeout = remaining_millis(Timeout, StartTimestamp),
            case wait(Group, SyncRef, NewTimeout, TimerRef) of
                {ok, {Members, NextTimeout}} ->
                    sync_send_loop(Members, [], Args, NextTimeout);
                {error, timeout} ->
                    exit({timeout, {lbm_pg, sync_send, tuple_to_list(Args)}})
            end;
        {ok, Members} when is_list(Members) ->
            NewTimeout = remaining_millis(Timeout, StartTimestamp),
            sync_send_loop(Members, [], Args, NewTimeout)
    end;
sync_send_loop([M | Ms], BadMs, Args = {Group, Msg, _, Opts}, Timeout) ->
    StartTimestamp = os:timestamp(),
    try apply_sync(M, Msg, Timeout) of
        Result ->
            ok = lbm_pg_dist:leave(Group, BadMs),
            ok = maybe_cache_member(Group, M, Opts),
            Result
    catch
        exit:{timeout, _} ->
            %% member is not dead, only overloaded... anyway Timeout is over
            exit({timeout, {lbm_pg, sync_send, tuple_to_list(Args)}});
        _:_  ->
            NewTimeout = remaining_millis(Timeout, StartTimestamp),
            sync_send_loop(Ms, [M | BadMs], Args, NewTimeout)
    end.

%%------------------------------------------------------------------------------
%% @private
%% Wait for either a timeout message (created with {@link send_after/3}) or a
%% member update from {@link lbm_pg_dist}. This will also try to leave the
%% callers message queue in a consistent state avoiding ghost messages beeing
%% send to the calling process.
%%------------------------------------------------------------------------------
wait(Group, UpdateRef, Timeout, TimerRef) ->
    receive
        ?UPDATE_MSG(UpdateRef, Group, timeout) ->
            ok = lbm_pg_dist:del_waiting(Group, UpdateRef),
            ok = flush_updates(Group, UpdateRef),
            {error, timeout};
        ?UPDATE_MSG(UpdateRef, Group, Subscribers) ->
            Time = cancel_timer(TimerRef, Timeout, Group, UpdateRef),
            {ok, {Subscribers, Time}}
    end.

%%------------------------------------------------------------------------------
%% @private
%% Flush all pending update messages from the callers message queue.
%%------------------------------------------------------------------------------
flush_updates(Group, UpdateRef) ->
    receive
        ?UPDATE_MSG(UpdateRef, Group, _) ->
            flush_updates(Group, UpdateRef)
    after
        50 -> ok
    end.

%%------------------------------------------------------------------------------
%% @private
%% Start a timer for the calling process, does nothing if `Timeout' is set to
%% infinity.
%%------------------------------------------------------------------------------
send_after(Timeout, Group, UpdateRef) when is_integer(Timeout) ->
    erlang:send_after(Timeout, self(), ?UPDATE_MSG(UpdateRef, Group, timeout));
send_after(infinity, _Group, _Ref) ->
    erlang:make_ref().

%%------------------------------------------------------------------------------
%% @private
%% Cancel a timer created with {@link send_after/3}. And reports the remaining
%% time. If the timer already expired the function tries to remove the timeout
%% message from the process message queue.
%%------------------------------------------------------------------------------
cancel_timer(TimerRef, Timeout, Group, UpdateRef) ->
    case erlang:cancel_timer(TimerRef) of
        false when is_integer(Timeout) ->
            %% cleanup the message queue in case timer was already delivered
            receive ?UPDATE_MSG(UpdateRef, Group, _) -> 0 after 0 -> 0 end;
        false when Timeout =:= infinity ->
            Timeout;
        Time when is_integer(Time) ->
            Time
    end.

%%------------------------------------------------------------------------------
%% @private
%% Exits the calling process, if the `no_wait' option is specified.
%%------------------------------------------------------------------------------
exit_if_no_wait(Opts, Args) ->
    case lists:member(no_wait, Opts) of
        true ->
            exit({no_members, {lbm_pg, sync_send, tuple_to_list(Args)}});
        false ->
            ok
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
apply_sync(#lbm_pg_member{b = gen_server, p = Pid}, Msg, Timeout) ->
    gen_server:call(Pid, Msg, Timeout);
apply_sync(#lbm_pg_member{b = gen_fsm, p = Pid}, Msg, Timeout) ->
    gen_fsm:sync_send_event(Pid, Msg, Timeout).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
to_millis({MegaSecs, Secs, MicroSecs}) ->
    MegaSecs * 1000000000 + Secs * 1000 + MicroSecs div 1000.
