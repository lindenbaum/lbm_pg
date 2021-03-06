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
%%% The distributed state backend used to manage members based on a local
%%% `ETS' table. This is quite similar to how `pg2' distributes its internal
%%% state without `global' locks.
%%%
%%% The table ?MODULE contains the following terms:
%%% `{{Group, cached, #lbm_pg_member{}}}': the cached member for a group
%%% `{{Group, member, #lbm_pg_member{}}}': an ordinary group member
%%% @end
%%%=============================================================================

-module(lbm_pg_dist).

-behaviour(gen_server).

%% API
-export([spec/0,
         join/2,
         leave/2,
         members/2,
         add_waiting/2,
         del_waiting/2,
         cache_member/2,
         info/0]).

%% Internal API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1,
         handle_cast/2,
         handle_call/3,
         handle_info/2,
         code_change/3,
         terminate/2]).

-include("lbm_pg.hrl").

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Return the supervisor child spec that has to be used to start the backend.
%% @end
%%------------------------------------------------------------------------------
-spec spec() -> supervisor:child_spec().
spec() ->
    {?MODULE, {?MODULE, start_link, []}, permanent, 1000, worker, [?MODULE]}.

%%------------------------------------------------------------------------------
%% @doc
%% Add a member to the process group with name `Name'.
%% @end
%%------------------------------------------------------------------------------
-spec join(lbm_pg:name(), #lbm_pg_member{}) -> ok.
join(Group, Member = #lbm_pg_member{}) -> multi_call({join, Group, Member}).

%%------------------------------------------------------------------------------
%% @doc
%% Leave a list of bad/exited members for the group with name `Name'.
%% @end
%%------------------------------------------------------------------------------
-spec leave(lbm_pg:name(), pid() | [#lbm_pg_member{}]) -> ok.
leave(_Group, []) ->
    ok;
leave(Group, BadMembers) ->
    multi_cast([node() | nodes()], {leave, Group, BadMembers}).

%%------------------------------------------------------------------------------
%% @doc
%% Returns a list of all members for the group with name `Name'.
%% @end
%%------------------------------------------------------------------------------
-spec members(lbm_pg:name(), IncludeCached :: boolean()) -> [#lbm_pg_member{}].
members(Group, true)  -> members_by_type('_', Group);
members(Group, false) -> members_by_type(member, Group).

%%------------------------------------------------------------------------------
%% @doc
%% Add a process to the processes waiting for members of a specific group.
%% The process may report bad members along with the registration. The
%% process will receive a message of the form
%% `?LBM_PG_UPDATE(reference(), lbm_pg:name(), [#lbm_pg_member{}])'
%% when new members are available. The reference contained in the message
%% will be returned from this function call.
%% @end
%%------------------------------------------------------------------------------
-spec add_waiting(lbm_pg:name(), [#lbm_pg_member{}]) ->
                         {ok, reference() | [#lbm_pg_member{}]}.
add_waiting(Group, BadMembers) when is_list(BadMembers) ->
    gen_server:call(?MODULE, {add_waiting, Group, self(), BadMembers}).

%%------------------------------------------------------------------------------
%% @doc
%% Removes a process from the list of processes waiting for a group. Calling
%% this function is only necessary if the process gives up waiting for members.
%% The wait entry will be removed automatically, when a member update is
%% sent from the server.
%% @end
%%------------------------------------------------------------------------------
-spec del_waiting(lbm_pg:name(), reference()) -> ok.
del_waiting(_Group, Reference) ->
    gen_server:cast(?MODULE, {del_waiting, Reference}).

%%------------------------------------------------------------------------------
%% @doc
%% Cache a specific member of a group.
%% @end
%%------------------------------------------------------------------------------
-spec cache_member(lbm_pg:name(), #lbm_pg_member{}) -> ok.
cache_member(Group, Member) -> gen_server:cast(?MODULE, {cache, Group, Member}).

%%------------------------------------------------------------------------------
%% @doc
%% Print the current group and membership info to stdout.
%% @end
%%------------------------------------------------------------------------------
-spec info() -> ok.
info() ->
    Groups = groups(),
    io:format("~w Groups:~n", [length(Groups)]),
    [begin
         Cached = members_by_type(cached, Group),
         Members = members(Group, false),
         io:format(" * ~w (~w Members):~n", [Group, length(Members)]),
         [io:format("   * ~w (Backend:~s, Cached:true)~n", [Pid, Backend])
          || #lbm_pg_member{b = Backend, p = Pid} <- Cached],
         [io:format("   * ~w (Backend:~s, Cached:false)~n", [Pid, Backend])
          || #lbm_pg_member{b = Backend, p = Pid} <- Members -- Cached]
     end || Group <- Groups],
    ok.

%%%=============================================================================
%%% Internal API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

-define(ETS_OPTS, [ordered_set, protected, named_table]).

-define(key(Type, Group, Member), {Group, Type, Member}).
-define(entry(Key), {Key}).

-record(waiting, {
          ref   :: reference(),
          group :: lbm_pg:name(),
          pid   :: pid()}).

-record(state, {
          monitors = [] :: [{pid(), reference()}],
          waiting  = [] :: [#waiting{}]}).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([]) ->
    Nodes = nodes(),
    ok = net_kernel:monitor_nodes(true),
    lists:foreach(
      fun(Node) ->
              {?MODULE, Node} ! {new, ?MODULE, node()},
              self() ! {nodeup, Node}
      end, Nodes),
    ?MODULE = ets:new(?MODULE, [{read_concurrency, true} | ?ETS_OPTS]),
    {ok, #state{}}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_call({join, Group, Member}, _From, State) ->
    case member_join(Group, Member, State) of
        {true, NewState} ->
            {reply, ok, waiting_notify({Group, [Member]}, NewState)};
        {false, NewState} ->
            {reply, ok, NewState}
    end;
handle_call({add_waiting, Group, Pid, BadMembers}, _From, State) ->
    NewState1 = members_leave(Group, BadMembers, State),
    case members(Group, false) of
        [] ->
            {Reference, NewState2} = waiting_add(Group, Pid, NewState1),
            {reply, {ok, Reference}, NewState2};
        Members ->
            {reply, {ok, Members}, NewState1}
    end;
handle_call(Request, From, State) ->
    error_logger:warning_msg(
      "Received unexpected call ~w from ~w",
      [Request, From]),
    {reply, undef, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_cast({leave, Group, Members}, State) ->
    {noreply, members_leave(Group, Members, State)};
handle_cast({del_waiting, Reference}, State) ->
    {noreply, waiting_remove(Reference, State)};
handle_cast({cache, Group, Member}, State) ->
    {noreply, cache_member(Group, Member, State)};
handle_cast({merge, Memberships}, State) ->
    {noreply, memberships_merge(Memberships, State)};
handle_cast(Request, State) ->
    error_logger:warning_msg("Received unexpected cast ~w", [Request]),
    {noreply, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_info({'DOWN', Reference, process, Pid, _Reason}, State) ->
    {noreply, process_down(Pid, Reference, State)};
handle_info({nodedown, Node}, State) ->
    {noreply, members_leave('_', Node, State)};
handle_info({nodeup, Node}, State) ->
    gen_server:cast({?MODULE, Node}, {merge, memberships()}),
    {noreply, State};
handle_info({new, ?MODULE, Node}, State) ->
    gen_server:cast({?MODULE, Node}, {merge, memberships()}),
    {noreply, State};
handle_info(Info, State) ->
    error_logger:warning_msg("Received unexpected info ~w", [Info]),
    {noreply, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
terminate(_Reason, _State) -> to_ok(ets:delete(?MODULE)).

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%% Process a 'DOWN' message, either from a group member or a waiting process.
%%------------------------------------------------------------------------------
process_down(Pid, Reference, State) ->
    process_down_impl(Pid, State, waiting_remove(Reference, State)).
process_down_impl(Pid, State, State) ->
    ok = multi_cast(nodes(), {leave, '_', Pid}),
    members_leave('_', Pid, State);
process_down_impl(_Pid, _OldState, NewState) ->
    NewState.

%%------------------------------------------------------------------------------
%% @private
%% Add a waiting process to state.
%%------------------------------------------------------------------------------
waiting_add(Group, Pid, State = #state{waiting = Ws}) ->
    Reference = erlang:monitor(process, Pid),
    Waiting = #waiting{ref = Reference, group = Group, pid = Pid},
    {Reference, State#state{waiting = [Waiting | Ws]}}.

%%------------------------------------------------------------------------------
%% @private
%% Remove a waiting process from state.
%%------------------------------------------------------------------------------
waiting_remove(Reference, State = #state{waiting = Ws}) ->
    true = erlang:demonitor(Reference),
    State#state{waiting = lists:keydelete(Reference, #waiting.ref, Ws)}.

%%------------------------------------------------------------------------------
%% @private
%% Notify waiting processes about (new) group members.
%%------------------------------------------------------------------------------
waiting_notify({Group, Members}, State = #state{waiting = Ws}) ->
    {ToNotify, NewWs} = lists:partition(waiting_for_group_fun(Group), Ws),
    ok = lists:foreach(waiting_notify_fun(Group, Members), ToNotify),
    State#state{waiting = NewWs}.

%%------------------------------------------------------------------------------
%% @private
%% Notify a waiting process about (new) group members.
%%------------------------------------------------------------------------------
waiting_notify_fun(Group, Members) ->
    fun(#waiting{pid = Pid, ref = Ref}) ->
            Pid ! ?LBM_PG_UPDATE(Ref, Group, Members)
    end.

%%------------------------------------------------------------------------------
%% @private
%% Returns a predicate fun matching all waiting processes for the given group.
%%------------------------------------------------------------------------------
waiting_for_group_fun(Group) -> fun(#waiting{group = G}) -> G =:= Group end.

%%------------------------------------------------------------------------------
%% @private
%% Join a single member into a certain group. This will monitor the member if it
%% resides on the local node.
%%------------------------------------------------------------------------------
member_join(Group, Member, State = #state{monitors = Ms}) ->
    case member_insert(Group, Member) of
        true ->
            Pid = Member#lbm_pg_member.p,
            case node(Pid) =:= node() of
                true ->
                    Ref = erlang:monitor(process, Pid),
                    {true, State#state{monitors = [{Pid, Ref} | Ms]}};
                false ->
                    {true, State}
            end;
        false ->
            {false, State}
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
member_insert(Group, Member) ->
    ets:insert_new(?MODULE, ?entry(?key(member, Group, Member))).

%%------------------------------------------------------------------------------
%% @private
%% Leave one or more members from a group. Members are identified either by
%% pid, node or member record(s). This will also demonitor the members if
%% they resided on the local node.
%%------------------------------------------------------------------------------
members_leave(Group, Members, State) ->
    case members_delete(Group, Members) of
        N when N > 0 ->
            members_demonitor(Members, State);
        _ ->
            State
    end.

%%------------------------------------------------------------------------------
%% @private
%% Demonitor one or more members identified either by pid, node or member
%% record(s).
%%------------------------------------------------------------------------------
members_demonitor(Node, State) when is_atom(Node) ->
    State;
members_demonitor(Pid, State = #state{monitors = Ms}) when is_pid(Pid) ->
    case node(Pid) =:= node() of
        true ->
            case lists:keytake(Pid, 1, Ms) of
                {value, {Pid, Ref}, NewMs} ->
                    true = erlang:demonitor(Ref),
                    State#state{monitors = NewMs};
                false ->
                    State
            end;
        false ->
            State
    end;
members_demonitor(#lbm_pg_member{p = Pid}, State) ->
    members_demonitor(Pid, State);
members_demonitor(Members, State) when is_list(Members) ->
    lists:foldl(fun members_demonitor/2, State, Members).

%%------------------------------------------------------------------------------
%% @private
%% Delete one or more members from a group, returning the number of actual
%% deletes performed. Members are identified either by pid, node or member
%% record(s).
%%------------------------------------------------------------------------------
members_delete(Group, Node) when is_atom(Node) ->
    Entry = ?entry(?key('_', Group, #lbm_pg_member{b = '_', p = '$1'})),
    Guards = [{'=:=', {node, '$1'}, Node}],
    ets:select_delete(?MODULE, [{Entry, Guards, [true]}]);
members_delete(Group, Pid) when is_pid(Pid) ->
    members_delete(Group, #lbm_pg_member{b = '_', p = Pid});
members_delete(Group, Member = #lbm_pg_member{}) ->
    Entry = ?entry(?key('_', Group, Member)),
    ets:select_delete(?MODULE, [{Entry, [], [true]}]);
members_delete(Group, Members) when is_list(Members) ->
    lists:sum([members_delete(Group, Member) || Member <- Members]).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
members_by_type(Type, Group) ->
    [Member || [Member] <- ets:match(?MODULE, ?entry(?key(Type, Group, '$1')))].

%%------------------------------------------------------------------------------
%% @private
%% Return all known memberships.
%%------------------------------------------------------------------------------
memberships() ->
    [Key || ?entry(Key = ?key(member, _, _)) <- ets:tab2list(?MODULE)].
%%    [Entry || [Entry] <- ets:match(?MODULE, ?entry(member, '_', '_'))].

%%------------------------------------------------------------------------------
%% @private
%% Merge the given memberships with local ones, notifying eventually waiting
%% processes.
%%------------------------------------------------------------------------------
memberships_merge(Memberships, State) ->
    lists:foldl(
      fun waiting_notify/2, State,
      [{Group, members(Group, false)}
       || Group <- memberships_insert(Memberships)]).

%%------------------------------------------------------------------------------
%% @private
%% Join the given memberships returning a list with groups that now have new
%% members.
%%------------------------------------------------------------------------------
memberships_insert(Memberships) ->
    lists:usort(
      lists:foldl(
        fun(?key(member, Group, Member), Acc) ->
                case member_insert(Group, Member) of
                    true  -> [Group | Acc];
                    false -> Acc
                end
        end, [], Memberships)).

%%------------------------------------------------------------------------------
%% @private
%% Cache the member only if it is currently joined to the group.
%%------------------------------------------------------------------------------
cache_member(Group, Member, State) ->
    [begin
         ets:match_delete(?MODULE, ?entry(?key(cached, Group, '_'))),
         ets:insert(?MODULE, ?entry(?key(cached, Group, Member)))
     end || lists:member(Member, members(Group, false))],
    State.

%%------------------------------------------------------------------------------
%% @private
%% Return all known groups.
%%------------------------------------------------------------------------------
groups() ->
    Key = ?key(member, '$1', '_'),
    lists:usort([G || [G] <- ets:match(?MODULE, ?entry(Key))]).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
multi_call(Message) -> to_ok(catch gen_server:multi_call(?MODULE, Message)).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
multi_cast(Nodes, Message) -> to_ok(gen_server:abcast(Nodes, ?MODULE, Message)).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
to_ok(_) -> ok.

%%%=============================================================================
%%% Internal tests
%%%=============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

ets_test_() -> {spawn, fun ets_matches_working/0}.

ets_matches_working() ->
    ?MODULE = ets:new(?MODULE, ?ETS_OPTS),

    Pid = spawn(fun() -> ok end),
    Member1 = #lbm_pg_member{b = m, p = Pid},
    Member2 = #lbm_pg_member{b = m, p = self()},

    ?assertEqual([], groups()),

    ?assert(member_insert(group, Member1)),
    ?assert(not member_insert(group, Member1)),
    ?assertEqual([?key(member, group, Member1)], memberships()),
    ?assert(member_insert(group, Member2)),
    ?assert(not member_insert(group, Member2)),
    ?assertEqual([group], groups()),

    ?assertEqual(1, members_delete(group, [Member1])),
    ?assertEqual([Member2], members(group, false)),
    ?assertEqual([?key(member, group, Member2)], memberships()),
    ?assertEqual(1, members_delete(group, Member2)),
    ?assertEqual([], members(group, false)),
    ?assertEqual([], groups()),

    ?assert(member_insert(group, Member1)),
    ?assert(not member_insert(group, Member1)),
    ?assert(member_insert(group, Member2)),
    ?assert(not member_insert(group, Member2)),
    ?assertEqual([group], groups()),

    ?assertEqual(1, members_delete('_', self())),
    ?assertEqual([group], groups()),
    ?assertEqual([Member1], members(group, false)),
    ?assertEqual(1, members_delete('_', node())),
    ?assertEqual([], groups()),
    ?assertEqual([], members(group, false)),

    ?assertEqual([], members_by_type(cached, group)),
    ?assert(cache_member(group, Member1, true)),
    ?assertEqual([], members_by_type(cached, group)),
    ?assert(member_insert(group, Member1)),
    ?assert(cache_member(group, Member1, true)),
    ?assertEqual([Member1], members_by_type(cached, group)),
    ?assertEqual(0, members_delete(group, Member2)),
    ?assertEqual([Member1], members_by_type(cached, group)),
    ?assertEqual(2, members_delete(group, Member1)),
    ?assertEqual([], members_by_type(cached, group)).

-endif.
