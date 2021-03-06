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
%%% Another approach to process groups.
%%%
%%% `lbm_pg' offers a process group implementation similar to `pg2' without the
%%% use of `global' with integrated facilities to (reliably) send messages to
%%% group members with failover, timeouts, member caching and more.
%%%
%%% To see member management details look at the {@link lbm_pg_dist} backend.
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
         send/2,
         send/3,
         send/4,
         info/0]).

%% Application callbacks
-export([start/2, stop/1]).

%% supervisor callbacks
-export([init/1]).

-type name()             :: any().
-type backend()          :: gen_server | gen_fsm.
-type send_option()      :: no_wait | no_cache | error_feedback.

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
members(Group) -> lbm_pg_dist:members(Group, false).

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
    lbm_pg_sync:send(Group, Message, Timeout, Options).

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link send/3} with `Timeout' set to `5000'.
%% @end
%%------------------------------------------------------------------------------
-spec send(name(), term()) -> ok.
send(Group, Message) -> send(Group, Message, 5000).

%%------------------------------------------------------------------------------
%% @doc
%% Similar to {@link send/4} with `Options' set to `[]'. For a description of
%% timeout refer to {@link send/4}.
%% @end
%%------------------------------------------------------------------------------
-spec send(name(), term(), timeout()) -> ok.
send(Group, Message, Timeout) -> send(Group, Message, Timeout, []).

%%------------------------------------------------------------------------------
%% @doc
%% Sends a message asynchronously to exactly one member of a group (if any).
%% This does not block the calling process. However, {@link lbm_pg:sync_send/4}
%% will be used under the hood with the specified arguments but the actual call
%% is made from a worker process taken from a pool of workers. This should
%% explain why there is a `Timeout' argument necessary for an asynchronous
%% function.
%%
%% If the message cannot be delivered and the `error_feedback' option is given
%% a message of the form ?LBM_PG_ERROR/3 will be sent back to the sending
%% process asynchronously notifying about the failed send request. If the option
%% is not specified and the message cannot be delivered, the respective worker
%% will exit (and be restarted). In this case errors will not be visible to the
%% implementation but can be observed, e.g. by viewing the applications log.
%%
%% The `error_feedback' can also be enabled globally by specifying
%% `{error_feedback, true}' in the application environment.
%%
%% For a description of options and semantics see {@link sync_send/4}.
%% @end
%%------------------------------------------------------------------------------
-spec send(name(), term(), timeout(), [send_option()]) -> ok.
send(Group, Message, Timeout, Options) ->
    lbm_pg_async:send(Group, Message, Timeout, Options).

%%------------------------------------------------------------------------------
%% @doc
%% Print group and membership info to stdout.
%% @end
%%------------------------------------------------------------------------------
-spec info() -> ok.
info() ->
    lbm_pg_async:info(),
    lbm_pg_dist:info().

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
init([]) ->
    Workers = application:get_env(?MODULE, workers, 50),
    Specs = [lbm_pg_async:spec(Workers), lbm_pg_dist:spec()],
    {ok, {{one_for_one, 0, 1}, Specs}}.

%%%=============================================================================
%%% internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
new_member(Backend, Pid) when Backend =:= gen_fsm; Backend =:= gen_server ->
    #lbm_pg_member{b = Backend, p = Pid}.
