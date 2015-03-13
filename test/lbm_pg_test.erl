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
%%%=============================================================================

-module(lbm_pg_test).

-include_lib("eunit/include/eunit.hrl").

-define(GROUP, group).

-define(LOG(Fmt, Args), io:format(standard_error, Fmt, Args)).

-define(DOWN(Ref, Pid), receive {'DOWN', Ref, process, Pid, normal} -> ok end).
-define(DOWN_FUN, fun(_) -> ?DOWN(_, _) end).

-define(EXIT(Pid), receive {'EXIT', Pid, normal} -> ok end).
-define(EXIT_FUN, fun(_) -> ?EXIT(_) end).

-define(TIMEOUT, 90).

%%%=============================================================================
%%% TESTS
%%%=============================================================================

all_test_() ->
    {foreach, setup(), teardown(),
     [
      fun report_header/0,
      fun join_only/0,
      fun sync_send_no_members/0,
      fun sync_send_no_members_no_wait/0,
      fun sync_send_three_messages/0,
      fun sync_send_to_three_members/0,
      fun sync_send_member_joins_late/0,
      {spawn, fun() -> sync_send_one_group_one_member_few_senders([no_cache]) end},
      {spawn, fun() -> sync_send_one_group_one_member_few_senders([]) end},
      {spawn, fun sync_send_one_group_one_member_many_senders/0},
      {spawn, fun sync_send_one_group_many_members_many_senders/0},
      {timeout, ?TIMEOUT, {spawn, fun sync_send_many_groups_one_sender_per_group/0}},
      {timeout, ?TIMEOUT, {spawn, fun sync_send_many_groups_many_senders/0}},
      {timeout, ?TIMEOUT, {spawn, fun sync_send_one_group_few_senders_distributed_setup/0}},
      {timeout, ?TIMEOUT, {spawn, fun sync_send_one_group_many_senders_distributed_setup/0}},
      {timeout, ?TIMEOUT, {spawn, fun sync_send_many_groups_many_senders_distributed_setup/0}},
      fun send_three_messages/0
     ]}.

join_only() ->
    {S, SR} = lbm_pg_member:start(?GROUP, 0),
    receive {joined, S} -> ok end,
    ?assertEqual(1, length(lbm_pg:members(?GROUP))),
    ok = lbm_pg_member:stop(S),
    ?DOWN(SR, S).

sync_send_no_members() ->
    ?assertEqual(0, length(lbm_pg:members(?GROUP))),
    try lbm_pg:sync_send(?GROUP, msg, 100) of
        _ -> throw(test_failed)
    catch
        exit:{timeout, {lbm_pg, sync_send, [?GROUP, msg, 100, []]}} ->
            ok
    end.

sync_send_no_members_no_wait() ->
    ?assertEqual(0, length(lbm_pg:members(?GROUP))),
    try lbm_pg:sync_send(?GROUP, msg, 100, [no_wait]) of
        _ -> throw(test_failed)
    catch
        exit:{no_members, {lbm_pg, sync_send, [?GROUP, msg, 100, [no_wait]]}} ->
            ok
    end.

sync_send_three_messages() ->
    Messages = 3,
    Test = fun() ->
                   {S, SR} = lbm_pg_member:start(?GROUP, Messages),
                   receive {joined, S} -> ok end,
                   ?assertEqual(1, length(lbm_pg:members(?GROUP))),

                   ok = lbm_pg:info(),

                   {P, PR} = spawn_monitor(sender(?GROUP, Messages, [])),

                   ok = lbm_pg:info(),

                   ?DOWN(SR, S),
                   ?DOWN(PR, P)
           end,
    Time = element(1, timer:tc(Test)),
    report(Messages, 1, 1, 1, 1, Time),
    {'EXIT', {timeout, _}} = (catch lbm_pg:sync_send(?GROUP, msg, 100)).

sync_send_to_three_members() ->
    Test = fun() ->
                   {S1, S1R} = lbm_pg_member:start(?GROUP, 1),
                   receive {joined, S1} -> ok end,
                   ?assertEqual(1, length(lbm_pg:members(?GROUP))),

                   {S2, S2R} = lbm_pg_member:start(?GROUP, 1),
                   receive {joined, S2} -> ok end,
                   ?assertEqual(2, length(lbm_pg:members(?GROUP))),

                   {S3, S3R} = lbm_pg_member:start(?GROUP, 1),
                   receive {joined, S3} -> ok end,
                   ?assertEqual(3, length(lbm_pg:members(?GROUP))),

                   {P, PR} = spawn_monitor(sender(?GROUP, 3, [])),

                   ?DOWN(S1R, S1),
                   ?DOWN(S2R, S2),
                   ?DOWN(S3R, S3),
                   ?DOWN(PR, P)
           end,
    Time = element(1, timer:tc(Test)),
    report(3, 1, 3, 1, 1, Time),
    {'EXIT', {timeout, _}} = (catch lbm_pg:sync_send(?GROUP, msg, 100)).

sync_send_member_joins_late() ->
    Messages = 3,
    Test = fun() ->
                   {P, PR} = spawn_monitor(sender(?GROUP, Messages, [])),

                   timer:sleep(500),

                   {S, SR} = lbm_pg_member:start(?GROUP, Messages),
                   receive {joined, S} -> ok end,
                   ?assertEqual(1, length(lbm_pg:members(?GROUP))),

                   ?DOWN(PR, P),
                   ?DOWN(SR, S)
           end,
    Time = element(1, timer:tc(Test)),
    report(Messages, 1, 1, 1, 1, Time),
    {'EXIT', {timeout, _}} = (catch lbm_pg:sync_send(?GROUP, msg, 100)).

sync_send_one_group_one_member_few_senders(Options) ->
    Messages = 100000,
    Senders = 100,
    MessagesPerSender = Messages div Senders,
    Sender = fun(_) ->
                     spawn_monitor(sender(?GROUP, MessagesPerSender, Options))
             end,
    Test = fun() ->
                   lbm_pg_member:start(?GROUP, Messages),
                   for(Senders, Sender),
                   for(Senders + 1, ?DOWN_FUN)
           end,
    Time = element(1, timer:tc(Test)),
    report(Messages, 1, 1, Senders, 1, Time).

sync_send_one_group_one_member_many_senders() ->
    Messages = 100000,
    Sender = fun(_) ->
                     spawn_monitor(sender(?GROUP, 1, []))
             end,
    Test = fun() ->
                   lbm_pg_member:start(?GROUP, Messages),
                   for(Messages, Sender),
                   for(Messages + 1, ?DOWN_FUN)
           end,
    Time = element(1, timer:tc(Test)),
    report(Messages, 1, 1, Messages, 1, Time).

sync_send_one_group_many_members_many_senders() ->
    Members = 4,
    Messages = 100000,
    NumTermsPerMember = Messages div Members,

    Sender = fun(_) ->
                     spawn_monitor(sender(?GROUP, 1, []))
             end,
    Member = fun() ->
                     lbm_pg_member:start(?GROUP, NumTermsPerMember)
             end,
    Test = fun() ->
                   {S1, SR1} = Member(),
                   for(Messages, Sender),

                   ?DOWN(SR1, S1),
                   [Member() || _ <- lists:seq(1, Members - 1)],

                   for(Messages + (Members - 1), ?DOWN_FUN)
           end,
    Time = element(1, timer:tc(Test)),
    report(Messages, 1, Members, Messages, 1, Time).

sync_send_many_groups_one_sender_per_group() ->
    Groups = 5000,
    Messages = 100000,
    MessagesPerGroup = Messages div Groups,
    Sender = fun(Group) ->
                     spawn_monitor(sender(Group, MessagesPerGroup, []))
             end,
    Member = fun(Group) ->
                     lbm_pg_member:start(Group, MessagesPerGroup)
             end,
    Test = fun() ->
                   for(Groups, Member),
                   for(Groups, Sender),
                   for(Groups + Groups, ?DOWN_FUN)
           end,
    Time = element(1, timer:tc(Test)),
    report(Messages, Groups, Groups, Groups, 1, Time).

sync_send_many_groups_many_senders() ->
    Groups = 5000,
    Messages = 100000,
    MessagesPerGroup = Messages div Groups,
    Sender = fun(Group) ->
                     fun(_) ->
                             spawn_monitor(sender(Group, 1, []))
                     end
             end,
    Member = fun(Group) ->
                     lbm_pg_member:start(Group, MessagesPerGroup)
             end,
    Test = fun() ->
                   for(Groups, Member),
                   foreach(
                     fun(P) ->
                             for(MessagesPerGroup, P)
                     end, for(Groups, Sender)),
                   for(Groups + Messages, ?DOWN_FUN)
           end,
    Time = element(1, timer:tc(Test)),
    report(Messages, Groups, Groups, Messages, 1, Time).

sync_send_one_group_few_senders_distributed_setup() ->
    process_flag(trap_exit, true),

    {ok, Slave1} = slave_setup(slave1),
    {ok, Slave2} = slave_setup(slave2),
    {ok, Slave3} = slave_setup(slave3),

    Nodes = [Slave3, Slave2, Slave1, node()],
    NumNodes = length(Nodes),
    Messages = 100000,
    NumSendsPerNode = Messages div NumNodes,
    Senders = 5000,
    SendersPerNode = Senders div NumNodes,
    MessagesPerSender = Messages div Senders,

    Sender = fun(_, Node) ->
                     spawn_link(Node, sender(?GROUP, MessagesPerSender, []))
             end,
    Member = fun(Node) ->
                     lbm_pg_member:start_link(Node, ?GROUP, NumSendsPerNode)
             end,
    Test = fun() ->
                   S0 = Member(node()),
                   foreach(
                     fun(N) ->
                             for(SendersPerNode, Sender, [N])
                     end, Nodes),

                   ?EXIT(S0),
                   foreach(Member, Nodes -- [node()]),
                   for(Senders + (NumNodes - 1), ?EXIT_FUN)
           end,
    Time = element(1, timer:tc(Test)),
    report(Messages, 1, NumNodes, Senders, NumNodes, Time).

sync_send_one_group_many_senders_distributed_setup() ->
    process_flag(trap_exit, true),

    {ok, Slave1} = slave_setup(slave1),
    {ok, Slave2} = slave_setup(slave2),
    {ok, Slave3} = slave_setup(slave3),

    Nodes = [Slave3, Slave2, Slave1, node()],
    NumNodes = length(Nodes),
    Messages = 100000,
    NumSendsPerNode = Messages div NumNodes,

    Sender = fun(_, Node) ->
                     spawn_link(Node, sender(?GROUP, 1, []))
             end,
    Member = fun(Node) ->
                     lbm_pg_member:start_link(Node, ?GROUP, NumSendsPerNode)
             end,
    Test = fun() ->
                   S0 = Member(node()),
                   foreach(
                     fun(N) ->
                             for(NumSendsPerNode, Sender, [N])
                     end, Nodes),

                   ?EXIT(S0),
                   foreach(Member, Nodes -- [node()]),
                   for(Messages + (NumNodes - 1), ?EXIT_FUN)
           end,
    Time = element(1, timer:tc(Test)),
    report(Messages, 1, NumNodes, Messages, NumNodes, Time).

sync_send_many_groups_many_senders_distributed_setup() ->
    process_flag(trap_exit, true),

    {ok, Slave1} = slave_setup(slave1),
    {ok, Slave2} = slave_setup(slave2),
    {ok, Slave3} = slave_setup(slave3),

    Nodes = [Slave3, Slave2, Slave1, node()],
    NumNodes = length(Nodes),
    Groups = 5000,
    GroupsPerNode = Groups div NumNodes,

    Messages = 100000,
    MessagesPerGroup = Messages div Groups,
    MessagesPerGroupPerNode = MessagesPerGroup div NumNodes,

    Sender = fun(Group) ->
                     fun(_, Node) ->
                             spawn_link(Node, sender(Group, 1, []))
                     end
             end,
    Member = fun(Group) ->
                     fun(Node) ->
                             lbm_pg_member:start_link(Node, Group, MessagesPerGroup)
                     end
             end,
    Test = fun() ->
                   Ps = for(Groups, Sender),
                   {SsList , []} = lists:mapfoldr(
                                     fun(_, SsIn) ->
                                             lists:split(GroupsPerNode, SsIn)
                                     end, for(Groups, Member), Nodes),

                   foreach(
                     fun(N) ->
                             foreach(
                               fun(P) ->
                                       for(MessagesPerGroupPerNode, P, [N])
                               end, Ps)
                     end, Nodes),
                   foreach(
                     fun({N, Ss}) ->
                             foreach(fun(S) -> S(N) end, Ss)
                     end, lists:zip(Nodes, SsList)),
                   for(Messages + Groups, ?EXIT_FUN)
           end,
    Time = element(1, timer:tc(Test)),
    report(Messages, Groups, Groups, Messages, NumNodes, Time).

send_three_messages() ->
    Messages = 3,

    {S, SR} = lbm_pg_member:start(?GROUP, Messages),
    receive {joined, S} -> ok end,
    ?assertEqual(1, length(lbm_pg:members(?GROUP))),

    ok = lbm_pg:info(),

    send(?GROUP, send, Messages, []),

    ok = lbm_pg:info(),

    ?DOWN(SR, S).

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @private
%% sync send all terms in a tight loop
%%------------------------------------------------------------------------------
sender(Group, NumMessages, Options) ->
    fun() -> send(Group, sync_send, NumMessages, Options) end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
send(Group, Function, NumMessages, Options) ->
    for(NumMessages,
        fun(Term) ->
                ok = lbm_pg:Function(
                       Group,
                       lbm_pg_member:message(Term),
                       ?TIMEOUT * 1000,
                       Options)
        end).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
foreach(F, L) -> ok = lists:foreach(F, L).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
for(Num, Fun)           -> for(Num, Fun, []).
for(Num, Fun, Args)     -> for_loop(Num, Fun, Args, []).
for_loop(0, _,  _, Acc) -> lists:reverse(Acc);
for_loop(I, F, As, Acc) -> for_loop(I - 1, F, As, [apply(F, [I | As]) | Acc]).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
setup() ->
    fun() ->
            ok = distribute('master@localhost'),
            setup_apps()
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
-ifdef(DEBUG).
setup_apps() ->
    application:load(sasl),
    {ok, Apps} = application:ensure_all_started(lbm_pg, permanent),
    Apps.
-else.
setup_apps() ->
    application:load(sasl),
    error_logger:tty(false),
    ok = application:set_env(sasl, sasl_error_logger, false),
    {ok, Apps} = application:ensure_all_started(lbm_pg, permanent),
    Apps.
-endif.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
teardown() ->
    fun(Apps) ->
            [application:stop(App) || App <- Apps],
            error_logger:tty(true)
    end.

%%------------------------------------------------------------------------------
%% @private
%% Make this node a distributed node.
%%------------------------------------------------------------------------------
distribute(Name) ->
    os:cmd("epmd -daemon"),
    case net_kernel:start([Name, shortnames]) of
        {ok, _}                       -> ok;
        {error, {already_started, _}} -> ok;
        Error                         -> Error
    end.

%%------------------------------------------------------------------------------
%% @private
%% Start a slave node and setup its environment (code path, applications, ...).
%%------------------------------------------------------------------------------
slave_setup(Name) ->
    {ok, Node} = slave:start_link(localhost, Name),
    true = lists:member(Node, nodes()),
    slave_setup_env(Node),
    {ok, Node}.

%%------------------------------------------------------------------------------
%% @private
%% Setup the slave node environment (code path, applications, ...).
%%------------------------------------------------------------------------------
slave_setup_env(Node) ->
    Mod = lbm_pg_member,
    {Mod, Bin, FName} = code:get_object_code(Mod),
    ok = slave_execute(Node, fun() -> {module, _} = code:load_binary(Mod, FName, Bin) end),
    Paths = code:get_path(),
    ok = slave_execute(Node, fun() -> [code:add_patha(P)|| P <- Paths] end),
    ok = slave_execute(Node, fun() -> setup_apps() end).

%%------------------------------------------------------------------------------
%% @private
%% Execute `Fun' on the given node.
%%------------------------------------------------------------------------------
slave_execute(Node, Fun) ->
    Pid = spawn_link(Node, Fun),
    receive
        {'EXIT', Pid, normal} -> ok;
        {'EXIT', Pid, Reason} -> {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
report_header() ->
    ?LOG("~n", []),
    ?LOG("MESSAGES | GROUPS | MEMBERS | SENDERS | NODES | MILLISECONDS~n", []),
    ?LOG("---------+--------+---------+---------+-------+-------------~n", []).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
report(Messages, Groups, Members, Senders, Nodes, MicroSeconds) ->
    ?LOG("~8s | ~6s | ~7s | ~7s | ~5s | ~w~n",
         [io_lib:write(Messages),
          io_lib:write(Groups),
          io_lib:write(Members),
          io_lib:write(Senders),
          io_lib:write(Nodes),
          MicroSeconds / 1000]).
