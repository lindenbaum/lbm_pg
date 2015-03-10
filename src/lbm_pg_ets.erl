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
%%% An implementation of the {@link lbm_pg_dist} behaviour based on a local
%%% `ETS' table using global locks to distribute subscriptions. This is quite
%%% similar to how `pg2' distributes its internal state.
%%%
%%% The table ?MODULE contains the following terms:
%%% `{{member, Group, #lbm_pg_member{}}}': a group member
%%% @end
%%%=============================================================================

-module(lbm_pg_ets).

-behaviour(gen_server).
-behaviour(lbm_pg_dist).

%% Internal API
-export([start_link/1]).

%% lbm_pg_dist callbacks
-export([spec/1,
         join/3,
         unjoin/3,
         members/2,
         add_waiting/3,
         del_waiting/3,
         info/1]).

%% gen_server callbacks
-export([init/1,
         handle_cast/2,
         handle_call/3,
         handle_info/2,
         code_change/3,
         terminate/2]).

-include("lbm_pg.hrl").

%%%=============================================================================
%%% Internal API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%% Simply start the server (registered).
%%------------------------------------------------------------------------------
-spec start_link(atom()) -> {ok, pid()} | {error, term()}.
start_link(Name) -> gen_server:start_link({local, Name}, ?MODULE, [Name], []).

%%%=============================================================================
%%% lbm_pg_dist callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec spec(atom()) -> supervisor:child_spec().
spec(Name) ->
    {Name, {?MODULE, start_link, [Name]}, permanent, 1000, worker, [?MODULE]}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec join(atom(), lbm_pg:name(), #lbm_pg_member{}) -> ok.
join(Name, Group, Member = #lbm_pg_member{}) ->
    multi_call(Name, {join, Group, Member}).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec unjoin(atom(), lbm_pg:name(), pid() | [#lbm_pg_member{}]) -> ok.
unjoin(_Name, _Group, []) ->
    ok;
unjoin(Name, Group, BadMembers) ->
    multi_cast(Name, {leave, Group, BadMembers}).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec members(atom(), lbm_pg:name()) -> [#lbm_pg_member{}].
members(_Name, Group) -> members(Group).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec add_waiting(atom(), lbm_pg:name(), [#lbm_pg_member{}]) ->
                         {ok, reference() | [#lbm_pg_member{}]}.
add_waiting(Name, Group, BadMembers) when is_list(BadMembers) ->
    gen_server:call(Name, {add_waiting, Group, self(), BadMembers}).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec del_waiting(atom(), lbm_pg:name(), reference()) -> ok.
del_waiting(Name, _Group, Reference) ->
    gen_server:cast(Name, {del_waiting, Reference}).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-spec info(atom()) -> ok.
info(_Name) ->
    Groups = groups(),
    io:format("~w Groups:~n", [length(Groups)]),
    [begin
         Members = members(Group),
         io:format(" * ~w (~w Members):~n", [Group, length(Members)]),
         [io:format("   * ~w (~s)~n", [Pid, Backend])
          || #lbm_pg_member{b = Backend, p = Pid} <- Members]
     end || Group <- Groups],
    ok.

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

-record(waiting, {
          ref   :: reference(),
          group :: lbm_pg:name(),
          pid   :: pid()}).

-record(state, {
          name          :: atom(),
          monitors = [] :: [{pid(), reference()}],
          waiting  = [] :: [#waiting{}]}).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([Name]) ->
    Nodes = nodes(),
    ok = net_kernel:monitor_nodes(true),
    lists:foreach(
      fun(Node) ->
              {Name, Node} ! {new, ?MODULE, node()},
              self() ! {nodeup, Node}
      end, Nodes),
    ?MODULE = ets:new(?MODULE, [ordered_set, protected, named_table]),
    {ok, #state{name = Name}}.

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
    NewState1 = members_unjoin(Group, BadMembers, State),
    case members(Group) of
        [] ->
            {Reference, NewState2} = waiting_add(Group, Pid, NewState1),
            {reply, {ok, Reference}, NewState2};
        Members ->
            {reply, {ok, Members}, NewState1}
    end;
handle_call(_Request, _From, State) ->
    {reply, undef, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_cast({leave, Group, Members}, State) ->
    {noreply, members_unjoin(Group, Members, State)};
handle_cast({del_waiting, Reference}, State) ->
    {noreply, waiting_remove(Reference, State)};
handle_cast({merge, Memberships}, State) ->
    {noreply, memberships_merge(Memberships, State)};
handle_cast(_Request, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_info({'DOWN', _Ref, process, Pid, _Reason}, State) ->
    {noreply, members_unjoin('_', Pid, State)};
handle_info({nodedown, Node}, State) ->
    {noreply, members_unjoin('_', Node, State)};
handle_info({nodeup, Node}, State = #state{name = Name}) ->
    gen_server:cast({Name, Node}, {merge, memberships()}),
    {noreply, State};
handle_info({new, ?MODULE, Node}, State = #state{name = Name}) ->
    gen_server:cast({Name, Node}, {merge, memberships()}),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
terminate(_Reason, _State) ->
    true = ets:delete(?MODULE),
    ok.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

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
            Pid ! ?UPDATE_MSG(Ref, Group, Members)
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
    ets:insert_new(?MODULE, {{member, Group, Member}}).

%%------------------------------------------------------------------------------
%% @private
%% Unjoin one or more members from a group. Members are identified either by
%% pid, node or member record(s). This will also demonitor the members if
%% they resided on the local node.
%%------------------------------------------------------------------------------
members_unjoin(Group, Members, State) ->
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
    Key = {member, Group, #lbm_pg_member{b = '_', p = '$1'}},
    Guards = [{'=:=', {node, '$1'}, Node}],
    ets:select_delete(?MODULE, [{{Key}, Guards, [true]}]);
members_delete(Group, Pid) when is_pid(Pid) ->
    members_delete(Group, #lbm_pg_member{b = '_', p = Pid});
members_delete(Group, Member = #lbm_pg_member{}) ->
    Key = {member, Group, Member},
    ets:select_delete(?MODULE, [{{Key}, [], [true]}]);
members_delete(Group, Members) when is_list(Members) ->
    lists:sum([members_delete(Group, Member) || Member <- Members]).

%%------------------------------------------------------------------------------
%% @private
%% Return all current members of a group.
%%------------------------------------------------------------------------------
members(Group) -> [M || [M] <- ets:match(?MODULE, {{member, Group, '$1'}})].

%%------------------------------------------------------------------------------
%% @private
%% Return all known memberships.
%%------------------------------------------------------------------------------
memberships() ->
    lists:sort([Member || {Member = {member, _, _}} <- ets:tab2list(?MODULE)]).

%%------------------------------------------------------------------------------
%% @private
%% Merge the given memberships with local ones, notifying eventually waiting
%% processes.
%%------------------------------------------------------------------------------
memberships_merge(Memberships, State) ->
    lists:foldl(
      fun waiting_notify/2, State,
      [{Group, members(Group)} || Group <- memberships_insert(Memberships)]).

%%------------------------------------------------------------------------------
%% @private
%% Join the given memberships returning a list with groups that now have new
%% members.
%%------------------------------------------------------------------------------
memberships_insert(Memberships) ->
    lists:usort(
      lists:foldl(
        fun({member, Group, Member}, Acc) ->
                case member_insert(Group, Member) of
                    true  -> [Group | Acc];
                    false -> Acc
                end
        end, [], Memberships)).

%%------------------------------------------------------------------------------
%% @private
%% Return all known groups.
%%------------------------------------------------------------------------------
groups() ->
    lists:usort([G || [G] <- ets:match(?MODULE, {{member, '$1', '_'}})]).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
multi_call(Name, Message) ->
    catch gen_server:multi_call(Name, Message),
    ok.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
multi_cast(Name, Message) ->
    gen_server:abcast(Name, Message),
    ok.

%%%=============================================================================
%%% Internal tests
%%%=============================================================================

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

ets_test_() -> {spawn, fun ets_matches_working/0}.

ets_matches_working() ->
    ?MODULE = ets:new(?MODULE, [ordered_set, protected, named_table]),

    Pid = spawn(fun() -> ok end),
    Member1 = #lbm_pg_member{b = m, p = Pid},
    Member2 = #lbm_pg_member{b = m, p = self()},

    ?assertEqual([], groups()),

    ?assert(member_insert(group, Member1)),
    ?assert(not member_insert(group, Member1)),
    ?assert(member_insert(group, Member2)),
    ?assert(not member_insert(group, Member2)),
    ?assertEqual([group], groups()),

    ?assertEqual(1, members_delete(group, [Member1])),
    ?assertEqual([Member2], members(group)),
    ?assertEqual(1, members_delete(group, Member2)),
    ?assertEqual([], members(group)),
    ?assertEqual([], groups()),

    ?assert(member_insert(group, Member1)),
    ?assert(not member_insert(group, Member1)),
    ?assert(member_insert(group, Member2)),
    ?assert(not member_insert(group, Member2)),
    ?assertEqual([group], groups()),

    ?assertEqual(1, members_delete('_', self())),
    ?assertEqual([group], groups()),
    ?assertEqual([Member1], members(group)),
    ?assertEqual(1, members_delete('_', node())),
    ?assertEqual([], groups()),
    ?assertEqual([], members(group)).

-endif.
