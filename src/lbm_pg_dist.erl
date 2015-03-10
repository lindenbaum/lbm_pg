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
%%% Defines the behaviour for the distributed state backend used to manage
%%% members. This module also delegates to the configured backend.
%%% @end
%%%=============================================================================

-module(lbm_pg_dist).

-include("lbm_pg.hrl").

%% lbm_pg_dist callbacks
-export([spec/0,
         join/2,
         unjoin/2,
         members/1,
         add_waiting/2,
         del_waiting/2,
         info/0]).

-define(BACKEND, lbm_pg_ets).

%%%=============================================================================
%%% Behaviour
%%%=============================================================================

-callback spec(atom()) -> supervisor:child_spec().
%% Return the supervisor child spec that has to be used to start the backend.

-callback join(atom(), lbm_pg:name(), #lbm_pg_member{}) -> ok.
%% Add a member to the process group with name `Name'.

-callback unjoin(atom(), lbm_pg:name(), pid() | [#lbm_pg_member{}]) -> ok.
%% Report a list of bad/exited members for the group with name `Name'.

-callback members(atom(), lbm_pg:name()) -> [#lbm_pg_member{}].
%% Returns a list of all members for the group with name `Name'.

-callback add_waiting(atom(), lbm_pg:name(), [#lbm_pg_member{}]) ->
    {ok, reference() | [#lbm_pg_member{}]}.
%% Add a process to the processes waiting for members of a specific group.
%% The process may report bad members along with the registration. The
%% process will receive a message of the form
%% `?UPDATE_MSG(reference(), lbm_pg:name(), [#lbm_pg_member{}])'
%% when new members are available. The reference contained in the message
%% will be returned from this function call.

-callback del_waiting(atom(), lbm_pg:name(), reference()) -> ok.
%% Removes a process from the list of processes waiting for a group. Calling
%% this function is only necessary if the process gives up waiting for members.
%% The wait entry will be removed automatically, when a member update is
%% sent from the server.

-callback info(atom()) -> ok.
%% Print the current group and membership info to stdout.

%%%=============================================================================
%%% lbm_pg_dist callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec spec() -> supervisor:child_spec().
spec() -> ?BACKEND:spec(?MODULE).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec join(lbm_pg:name(), #lbm_pg_member{}) -> ok.
join(Group, Member) -> ?BACKEND:join(?MODULE, Group, Member).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec unjoin(lbm_pg:name(), pid() | [#lbm_pg_member{}]) -> ok.
unjoin(Group, Members) -> ?BACKEND:unjoin(?MODULE, Group, Members).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec members(lbm_pg:name()) -> [#lbm_pg_member{}].
members(Group) -> ?BACKEND:members(?MODULE, Group).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec add_waiting(lbm_pg:name(), [#lbm_pg_member{}]) ->
                         {ok, reference() | [#lbm_pg_member{}]}.
add_waiting(Group, BadMembers) ->
    ?BACKEND:add_waiting(?MODULE, Group, BadMembers).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec del_waiting(lbm_pg:name(), reference()) -> ok.
del_waiting(Group, Reference) ->
    ?BACKEND:del_waiting(?MODULE, Group, Reference).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec info() -> ok.
info() -> ?BACKEND:info(?MODULE).
