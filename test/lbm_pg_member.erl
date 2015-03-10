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

-module(lbm_pg_member).

-behaviour(gen_server).

%% API
-export([start/2,
         start_link/3,
         stop/1,
         message/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(MESSAGE(X), {message, X}).

-ifdef(DEBUG).
-define(DBG(Fmt, Args), error_logger:info_msg(Fmt, Args)).
-else.
-define(DBG(Fmt, Args), Fmt = Fmt, Args = Args, ok).
-endif.

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Starts an anonymous, unlinked gen_server process.
%% @end
%%------------------------------------------------------------------------------
-spec start(term(), non_neg_integer()) -> {pid(), reference()}.
start(Group, NumTerms) ->
    {ok, Pid} = gen_server:start(?MODULE, [self(), Group, NumTerms], []),
    {Pid, erlang:monitor(process, Pid)}.

%%------------------------------------------------------------------------------
%% @doc
%% Starts an anonymous, unlinked gen_server process.
%% @end
%%------------------------------------------------------------------------------
-spec start_link(node(), term(), non_neg_integer()) -> pid().
start_link(Node, Group, NumTerms) ->
    Args = [self(), Group, NumTerms],
    spawn_link(
      Node,
      fun() ->
              process_flag(trap_exit, true),
              {ok, Pid} = gen_server:start_link(?MODULE, Args, []),
              ?DBG("Started member ~w on ~s~n", [Pid, Node]),
              receive {'EXIT', Pid, Reason} -> exit(Reason) end
      end).

%%------------------------------------------------------------------------------
%% @doc
%% Stop an member process gracefully.
%% @end
%%------------------------------------------------------------------------------
stop(Pid) -> gen_server:cast(Pid, stop).

%%------------------------------------------------------------------------------
%% @doc
%% Returns the expected message format for test messages.
%% @end
%%------------------------------------------------------------------------------
-spec message(term()) -> term().
message(Term) -> ?MESSAGE(Term).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

-record(state, {num_terms :: non_neg_integer()}).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([Parent, Group, NumTerms]) ->
    ok = lbm_pg:join_server(Group),
    Parent ! {joined, self()},
    erlang:monitor(process, Parent),
    {ok, #state{num_terms = NumTerms}}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_call(?MESSAGE(_), _From, State = #state{num_terms = 1}) ->
    {stop, normal, ok, State};
handle_call(?MESSAGE(_), _From, State = #state{num_terms = NumTerms}) ->
    {reply, ok, State#state{num_terms = NumTerms - 1}};
handle_call(Request, _From, State) ->
    {stop, {unexpected_call, Request}, undef, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_cast(stop, State) ->
    {stop, normal, State};
handle_cast(Request, State) ->
    {stop, {unexpected_cast, Request}, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_info({'DOWN', _, process, _, _}, State) ->
    {stop, normal, State};
handle_info(Info, State) ->
    {stop, {unexpected_info, Info}, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
terminate(_Reason, _State) -> ok.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) -> {ok, State}.
