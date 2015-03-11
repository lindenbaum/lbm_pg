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
%%% TODO
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
         leave/1,
         leave/2,
         members/1,
         sync_send/2,
         sync_send/3,
         sync_send/4,
         info/0]).

%% Application callbacks
-export([start/2, stop/1]).

%% supervisor callbacks
-export([init/1]).

-type name()             :: any().
-type backend()          :: gen_server | gen_fsm.
-type send_option()      :: no_wait | no_cache.

-export_type([name/0, backend/0, send_option/0]).

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
%% Joins a process with the specified backend as a member of the given group.
%% The process will be monitored by {@link lbm_pg_dist}, so there is no need to
%% explicitly leave a group before a member exits. However, a member can leave
%% a group explicitly using {@link leave/1,2}.
%%
%% It is allowed to use local registered names when joining a group. However,
%% the name will be resolved to the current process id before joining and this
%% process id will be used to monitor and send messages to the member.
%%
%% The only supported backend modules are `gen_server' and `gen_fsm'. This
%% limitiation has been chosen to force all members to adhere to the {@link gen}
%% protocol for synchronous calls. Additionally, since `lbm_pg' is distributed
%% and processes will call the backend module to send messages, the called
%% modules must be available/loaded on all nodes with potential senders. For OTP
%% behaviours this is always the case (if using proper OTP releases).
%%
%% When a process sends a message to a group member this message should be
%% handled in the respective callback function of the behaviour backend used.
%% E.g. if the backend is `gen_server', the message must be handled in the
%% member processes' `handle_call/3' function. In case of `gen_fsm' the message
%% will arrive in `StateName/3'.
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
%% Similar to {@link leave/2} with `Pid' set to `self()'.
%% @end
%%------------------------------------------------------------------------------
-spec leave(name()) -> ok.
leave(Group) -> leave(Group, self()).

%%------------------------------------------------------------------------------
%% @doc
%% Explicitly leave a member from a certain group.
%% @end
%%------------------------------------------------------------------------------
-spec leave(name(), pid() | atom()) -> ok.
leave(Group, Name) when is_atom(Name) ->
    case erlang:whereis(Name) of
        Pid when is_pid(Pid) -> leave(Group, Pid);
        _                    -> ok
    end;
leave(Group, Pid) when is_pid(Pid) ->
    lbm_pg_dist:leave(Group, Pid).

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
%% Similar to {@link sync_send/3} with `Timeout' set to `5000'.
%% @end
%%------------------------------------------------------------------------------
-spec sync_send(name(), term()) -> any().
sync_send(Group, Message) -> sync_send(Group, Message, 5000).

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link sync_send/4} with `Options' set to `[]'.
%% @end
%%------------------------------------------------------------------------------
-spec sync_send(name(), term(), timeout()) -> any().
sync_send(Group, Message, Timeout) -> sync_send(Group, Message, Timeout, []).

%%------------------------------------------------------------------------------
%% @doc
%% Sends a message synchronously to exactly one member of a group (if any).
%% The calling process will be blocked until either the message has been
%% processed by exactly one member or `Timeout' millis elapsed. If a member
%% fails to process the request and the remaining time allows it, `lbm_pg' will
%% try to send the message to another member.
%%
%% If there are no members joined to the specified group, the process will wait
%% for new members to join (until `Timeout' expires). This can explicitly be
%% turned off by specifying the `no_wait' option. When `no_wait' is specified
%% and there are no members in the specified group the calling process will be
%% exited immediately with
%% `exit({no_members, {lbm_pg, sync_send, [Group, Msg, Timeout, Options]}})'.
%%
%% When a `sync_send' operation is successful, the chosen member is cached in
%% the callers process dictionary. The next time this processes uses `sync_send'
%% for the same group this member is tried first. This allows a calling
%% processes to be sure to always send to the same member of a group as long as
%% this member is alive and joined to the respective group. To force a new
%% member selection for each `sync_send' call the option `no_cache' must be
%% specified.
%%
%% If a `sync_send' fails finally, the caller will be exited with
%% `exit({timeout, {lbm_pg, sync_send, [Group, Msg, Timeout, Options]}})'. If
%% the calling process decides to catch this error and a member is just late
%% with the reply, it may arrive at any time later into the caller's message
%% queue. The caller must in this case be prepared for this and discard any such
%% garbage messages.
%% @end
%%------------------------------------------------------------------------------
-spec sync_send(name(), term(), timeout(), [send_option()]) -> any().
sync_send(Group, Message, Timeout, Options) ->
    Args = {Group, Message, Timeout, Options},
    sync_send_loop(get_members(Group, Options), [], Args, Timeout).

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
                    exit({timeout, {?MODULE, sync_send, tuple_to_list(Args)}})
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
            maybe_cache(Group, M, Opts),
            Result
    catch
        exit:{timeout, _} ->
            %% member is not dead, only overloaded... anyway Timeout is over
            exit({timeout, {?MODULE, sync_send, tuple_to_list(Args)}});
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
            exit({no_members, {?MODULE, sync_send, tuple_to_list(Args)}});
        false ->
            ok
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
get_chached(Group, Opts) ->
    case lists:member(no_cache, Opts) of
        false -> get({?MODULE, Group});
        true  -> undefined
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
maybe_cache(Group, Member, Opts) ->
    case lists:member(no_cache, Opts) of
        false -> put({?MODULE, Group}, Member);
        true  -> ok
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
remaining_millis(infinity, _StartTimestamp) ->
    infinity;
remaining_millis(Timeout, StartTimestamp) ->
    Timeout - (to_millis(os:timestamp()) - to_millis(StartTimestamp)).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
to_millis({MegaSecs, Secs, MicroSecs}) ->
    MegaSecs * 1000000000 + Secs * 1000 + MicroSecs div 1000.
