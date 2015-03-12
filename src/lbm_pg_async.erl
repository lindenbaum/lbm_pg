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
%%% The implementation of the asynchronous send command. An asynchronous send is
%%% synchronous under the hood using a pool of processes to offload the actual
%%% work to.
%%% @end
%%%=============================================================================

-module(lbm_pg_async).

-behaviour(gen_server).

%% API
-export([spec/1,
         send/4,
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
%% Returns the child specification of the async worker pool.
%% @end
%%------------------------------------------------------------------------------
-spec spec(pos_integer()) -> supervisor:child_spec().
spec(NumWorkers) ->
    PoolOpts = [{workers, NumWorkers}, {worker, {?MODULE, []}}],
    {?MODULE, {wpool, start_pool, [?MODULE, PoolOpts]},
     permanent, infinity, supervisor, [wpool_pool]}.

%%------------------------------------------------------------------------------
%% @doc
%% See {@link lbm_pg:send/4} for detailed documentation.
%% @end
%%------------------------------------------------------------------------------
-spec send(lbm_pg:name(), term(), timeout(), [lbm_pg:send_option()]) -> ok.
send(Group, Message, Timeout, Options) ->
    Request = {send, os:timestamp(), Group, Message, Timeout, Options},
    wpool:cast(?MODULE, Request, best_worker).

%%------------------------------------------------------------------------------
%% @doc
%% Print the current asynchronous worker statistics to stdout.
%% @end
%%------------------------------------------------------------------------------
-spec info() -> ok.
info() ->
    Stats = wpool:stats(?MODULE),
    io:format("Async worker statistics:~n"),
    NumWorkers = proplists:get_value(workers, Stats),
    io:format(" * ~w workers~n", [NumWorkers]),
    TotalQueueLen = proplists:get_value(total_message_queue_len, Stats),
    io:format(" * ~w messges pending", [TotalQueueLen]).

%%%=============================================================================
%%% Internal API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Starts a worker of the async worker pool.
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() -> gen_server:start_link(?MODULE, [], []).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

-record(state, {}).

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
init([]) -> {ok, #state{}}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_call(_Request, _From, State) -> {reply, undef, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_cast({send, StartTimestamp, Group, Message, Timeout, Options}, State) ->
    NewTimeout = lbm_pg_sync:remaining_millis(Timeout, StartTimestamp),
    lbm_pg:sync_send(Group, Message, NewTimeout, Options),
    {noreply, State};
handle_cast(_Request, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
handle_info(_Info, State) -> {noreply, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%------------------------------------------------------------------------------
%% @private
%%------------------------------------------------------------------------------
terminate(_Reason, _State) -> ok.
