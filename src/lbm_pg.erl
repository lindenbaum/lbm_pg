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
%%% @end
%%%=============================================================================

-module(lbm_pg).

-behaviour(application).
-behaviour(supervisor).

%% API
-export([join_server/1,
         join_fsm/1,
         join/2,
         join/3,
         unjoin/1,
         unjoin/2,
         members/1,
         sync/2,
         sync/3,
         sync/4,
         info/0]).

%% Application callbacks
-export([start/2, stop/1]).

%% supervisor callbacks
-export([init/1]).

-type name()             :: any().
-type backend()          :: gen_server | gen_fsm.
-type sync_option()      :: no_wait | cache.

-export_type([name/0, backend/0, sync_option/0]).

-include("lbm_pg.hrl").

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link join/2} with `Backend' set to `gen_server'.
%% @end
%%------------------------------------------------------------------------------
-spec join_server(name()) -> ok | {error, term()}.
join_server(Group) -> join(Group, gen_server).

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link join/2} with `Backend' set to `gen_fsm'.
%% @end
%%------------------------------------------------------------------------------
-spec join_fsm(name()) -> ok | {error, term()}.
join_fsm(Group) -> join(Group, gen_fsm).

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link join/3} with `Pid' set to `self()'.
%% @end
%%------------------------------------------------------------------------------
-spec join(name(), backend()) -> ok | {error, term()}.
join(Group, Backend) -> join(Group, Backend, self()).

%%------------------------------------------------------------------------------
%% @doc
%% TODO
%% @end
%%------------------------------------------------------------------------------
-spec join(name(), backend(), pid() | atom()) -> ok | {error, term()}.
join(Group, Backend, Name) when is_atom(Name) ->
    case erlang:whereis(Name) of
        Pid when is_pid(Pid) -> join(Group, Backend, Pid);
        _                    -> {error, {not_registered, Name}}
    end;
join(Group, Backend, Pid) when is_pid(Pid) ->
    lbm_pg_dist:join(Group, new_member(Backend, Pid)).

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link unjoin/2} with `Pid' set to `self()'.
%% @end
%%------------------------------------------------------------------------------
-spec unjoin(name()) -> ok.
unjoin(Group) -> unjoin(Group, self()).

%%------------------------------------------------------------------------------
%% @doc
%% Explicitly unjoin a member from a certain group.
%% @end
%%------------------------------------------------------------------------------
-spec unjoin(name(), pid() | atom()) -> ok.
unjoin(Group, Name) when is_atom(Name) ->
    case erlang:whereis(Name) of
        Pid when is_pid(Pid) -> unjoin(Group, Pid);
        _                    -> ok
    end;
unjoin(Group, Pid) when is_pid(Pid) ->
    lbm_pg_dist:unjoin(Group, Pid).

%%------------------------------------------------------------------------------
%% @doc
%% Return the members currently joined to a certain group. The returned list may
%% contain members that are already dead and will be sorted out shortly after.
%% @end
%%------------------------------------------------------------------------------
-spec members(name()) -> [#lbm_pg_member{}].
members(Group) -> lbm_pg_dist:members(Group).

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link sync/3} with `Timeout' set to `5000'.
%% @end
%%------------------------------------------------------------------------------
-spec sync(name(), term()) -> any().
sync(Group, Message) -> sync(Group, Message, 5000).

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link sync/4} with `Options' set to `[]'.
%% @end
%%------------------------------------------------------------------------------
-spec sync(name(), term(), timeout()) -> any().
sync(Group, Message, Timeout) -> sync(Group, Message, Timeout, []).

%%------------------------------------------------------------------------------
%% @doc
%% TODO
%% @end
%%------------------------------------------------------------------------------
-spec sync(name(), term(), timeout(), [sync_option()]) -> any().
sync(Group, Message, Timeout, Options) ->
    Args = {Group, Message, Timeout, Options},
    sync_loop(get_members(Group, Options), [], Args, Timeout).

%%------------------------------------------------------------------------------
%% @doc
%% Print group and membership info to stdout.
%% @end
%%------------------------------------------------------------------------------
-spec info() -> ok.
info() -> lbm_pg_dist:info().

%%%=============================================================================
%%% Application callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
start(_StartType, _StartArgs) -> supervisor:start_link(?MODULE, []).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
stop(_State) -> ok.

%%%=============================================================================
%%% supervisor callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([]) -> {ok, {{one_for_one, 0, 1}, [lbm_pg_dist:spec()]}}.

%%%=============================================================================
%%% internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
new_member(Backend, Pid) when Backend =:= gen_fsm; Backend =:= gen_server ->
    #lbm_pg_member{b = Backend, p = Pid}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
get_members(Group, Options) ->
    Members = members(Group),
    case get_chached(Group, Options) of
        Member = #lbm_pg_member{} ->
            [Member | shuffle(lists:delete(Member, Members))];
        _ ->
            shuffle(Members)
    end.

%%------------------------------------------------------------------------------
%% @private
%% Try to sync send a message to exactly one subscriber. If no good subscribers
%% can be found, the loop will block the caller until either new members join
%% and handle the message or the given timeout expires (unless the `no_wait'
%% option is specified).
%%------------------------------------------------------------------------------
sync_loop([], BadMs, Args = {Group, _, _, Opts}, Timeout) ->
    ok = exit_if_no_wait(Opts, Args),
    StartTimestamp = os:timestamp(),
    case lbm_pg_dist:add_waiting(Group, BadMs) of
        {ok, SyncRef} when is_reference(SyncRef) ->
            TimerRef = send_after(Timeout, Group, SyncRef),
            NewTimeout = remaining_millis(Timeout, StartTimestamp),
            case wait(Group, SyncRef, NewTimeout, TimerRef) of
                {ok, {Members, NextTimeout}} ->
                    sync_loop(Members, [], Args, NextTimeout);
                {error, timeout} ->
                    exit({timeout, {?MODULE, sync, tuple_to_list(Args)}})
            end;
        {ok, Members} when is_list(Members) ->
            NewTimeout = remaining_millis(Timeout, StartTimestamp),
            sync_loop(Members, [], Args, NewTimeout)
    end;
sync_loop([M | Ms], BadMs, Args = {Group, Msg, _, Opts}, Timeout) ->
    StartTimestamp = os:timestamp(),
    try apply_sync(M, Msg, Timeout) of
        Result ->
            ok = lbm_pg_dist:unjoin(Group, BadMs),
            maybe_cache(Group, M, Opts),
            Result
    catch
        exit:{timeout, _} ->
            %% member is not dead, only overloaded... anyway Timeout is over
            exit({timeout, {?MODULE, sync, tuple_to_list(Args)}});
        _:_  ->
            NewTimeout = remaining_millis(Timeout, StartTimestamp),
            sync_loop(Ms, [M | BadMs], Args, NewTimeout)
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
%%------------------------------------------------------------------------------
shuffle(L) when is_list(L) ->
    shuffle(L, length(L)).
shuffle(L, Len) ->
    [E || {_, E} <- lists:sort([{crypto:rand_uniform(0, Len), E} || E <- L])].

%%------------------------------------------------------------------------------
%% @private
%% Exits the calling process, if the `no_wait' option is specified.
%%------------------------------------------------------------------------------
exit_if_no_wait(Opts, Args) ->
    case lists:member(no_wait, Opts) of
        true ->
            exit({no_members, {?MODULE, sync, tuple_to_list(Args)}});
        false ->
            ok
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
get_chached(Group, Opts) ->
    case lists:member(cache, Opts) of
        true  -> get({?MODULE, Group});
        false -> undefined
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
maybe_cache(Group, Member, Opts) ->
    case lists:member(cache, Opts) of
        true  -> put({?MODULE, Group}, Member);
        false -> ok
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
%% Calculate the remaining value for `Timeout' given a start timestamp.
%%------------------------------------------------------------------------------
remaining_millis(Timeout, StartTimestamp) ->
    Timeout - (to_millis(os:timestamp()) - to_millis(StartTimestamp)).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
to_millis({MegaSecs, Secs, MicroSecs}) ->
    MegaSecs * 1000000000 + Secs * 1000 + MicroSecs div 1000.
