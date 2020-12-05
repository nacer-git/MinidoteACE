%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Georges Younes.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

-module(camus_middleware).
-author("Georges Younes <georges.r.younes@gmail.com").

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% client callbacks
-export([cbcast/3,
         setnotifyfun/1,
         setmembership/1,
         getstate/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("camus.hrl").

-record(state, {actor :: id(),
                vclock :: vclock(),
                depgraph :: depgraph(),
                nodebitmap :: orddict:orddict(),
                latency :: term(),
                bits :: non_neg_integer(),
                unacked_list :: unacked(),
                rcvd :: rcvd(),
                rcvdsdot :: [],
                notify_function :: fun()}).

-type state_t() :: #state{}.
-record(consistency, {
  consistency :: atom()
}).

%%%===================================================================
%%% client callbacks
%%%===================================================================

%% Broadcast message.
-spec cbcast(message(), term(), {dot(), context()}) -> term().
cbcast(Msg, Consistency, {Dot, Ctxt}) ->
    gen_server:call(?MODULE, {cbcast, Dot, Ctxt, Msg, Consistency}, infinity).

%% Register the delivery function.
-spec setnotifyfun(fun()) -> ok.
setnotifyfun(Fun) ->
    gen_server:call(?MODULE, {setnotifyfun, Fun}, infinity).

%% Set membership.
-spec setmembership([id()]) -> ok.
setmembership(NodeList) ->
    gen_server:call(?MODULE, {setmembership, NodeList}, infinity).

%% Get state.
-spec getstate() -> term().
getstate() ->
    gen_server:call(?MODULE, getstate, infinity).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Same as start_link([]).
-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%% gen_server callbacks
%%%===================================================================

%% @private
-spec init(list()) -> {ok, state_t()}.
init([]) ->

    F = fun(Msg) ->
        lager:info("Message unhandled: ~p", [Msg]),
        ok
    end,
    {ok, #state{actor=?UTIL:get_node(),
                vclock=vclock:new(),
                depgraph=depgraph:new(),
                nodebitmap=orddict:new(),
                bits=0,
                unacked_list=unacked:new(),
                rcvd=rcvd:new(),
                latency=camus_config:get(camus_latency, undefined),
				rcvdsdot=[],
                notify_function=F}}.

%% @private
-spec handle_call(term(), {pid(), term()}, state_t()) ->
    {reply, term(), state_t()}.

handle_call({setmembership, NodeList}, _From, #state{actor=Actor, rcvd=Rcvd0, latency=Latency0}=State) ->
    Sorted = lists:usort([Actor|NodeList]),
    Length = length(Sorted),
    Membership = lists:foldl(
        fun(X, Acc) ->
            orddict:store(lists:nth(X, Sorted), round(math:pow(2, Length-X)), Acc)
        end,
        orddict:new(),
        lists:seq(1, Length)),
    N = round(math:pow(2, Length) - 1),
    Rcvd = rcvd:init(NodeList, Rcvd0),
    Latency = ?UTIL:generate_latency(Latency0, Sorted),
   % ?LOG("Membership is ~p", [Membership]),
   % ?LOG("bits is ~p", [N]),
   % ?LOG("rcvd is ~p", [Rcvd]),
   % ?LOG("generate_latency gave the following matrixL ~p", [Latency]),
    {reply, ok, State#state{nodebitmap=Membership, bits=N, rcvd=Rcvd, latency=Latency}};

handle_call(getstate, _From,
    #state{actor=Actor,
                   vclock=VClock,
                   depgraph=Depgraph,
                   nodebitmap=Nodebitmap}=State) ->
    {reply, {Actor, VClock, Depgraph, Nodebitmap}, State};

handle_call({cbcast, Dot, Ctxt, Msg, Consistency}, _From,
            #state{actor=Actor,
                   vclock=VClock,
                   depgraph=Depgraph,
                   latency=Latency,
                   unacked_list=Unacked0,
                   rcvd=Rcvd,
                   rcvdsdot=Rcvdsdot,
                   bits=N,
                   notify_function=F,
                   nodebitmap=Nodebitmap}=State) ->

    ?LOG("handle_call cbcast: ~p",[Msg]),

    %% update VV
    VClock1 = vclock:update_dot(Dot, VClock),

    %% calculate bitstr
    Bi = orddict:fetch(Actor, Nodebitmap),
    B = N - Bi,
    %% add to unacked

    Unacked = unacked:add(Dot, Ctxt, Msg, B, Unacked0),
	Cons = Consistency#consistency.consistency,
    % ?LOG("bcast Consistency : ~p", [Cons]),
    Rcvdsdot1 = if Cons == strong -> [Dot|Rcvdsdot];
				   true -> Rcvdsdot
				end,
    %% bcast and record transmission
    M = {remotemsg, {Id, Ctr}=Dot, Ctxt, Msg, Consistency, Rcvd, Rcvdsdot1},
    Peers = orddict:fetch_keys(Nodebitmap),
    ?UTIL:bcast(M, ?MODULE, Peers, Latency),

    %% filter stable deps
    Filt = fun(K, V) -> not is_stable({K, V}, VClock1, Depgraph) end,
    Preds = maps:filter(Filt, Ctxt),
   % ?LOG("Local Dot ~p has original preds ~p", [Dot, Ctxt]),
   % ?LOG("Local Dot ~p has filtered preds ~p", [Dot, Preds]),

    %% add to to graph
    Depgraph1 = maps:fold(
        fun(K, V, Acc) ->
            Succ0 = depgraph:get({K, V}, succ, Acc),
            Succ1 = maps:put(Id, Ctr, Succ0),
            depgraph:update({K, V}, [{succ, Succ1}], Acc)
        end,
        Depgraph,
        Preds
    ),

    %% Update bitstr
    case Cons == causal of
    true ->  
	    List = [{stage, ?DLV}, {bitstr, B}, {pred, Preds}, {succ, maps:new()}, {consistency, Cons}, {pyld, Msg}];
    false ->
	    List = [{stage, ?RCV}, {bitstr, B}, {pred, Preds}, {succ, maps:new()}, {consistency, Cons}, {pyld, Msg}]
    end,
    
    Depgraph2 = depgraph:add_with(Dot, List, Depgraph1),

    %% update stability
    Depgraph3 = update_stability(B, Dot, Depgraph2, F),

  %  ?LOG("Delivered locally dot ~p, vclock is ~p, ctxt is ~p", [Dot, VClock1, Ctxt]),

    {reply, ok, State#state{vclock=VClock1, depgraph=Depgraph3, unacked_list=Unacked, rcvdsdot=Rcvdsdot1}};

handle_call({setnotifyfun, F}, _From, State) ->
    {reply, ok, State#state{notify_function=F}}.

%% @private
-spec handle_cast(term(), state_t()) -> {noreply, state_t()}.
handle_cast({remotemsg, {Id, Ctr}=Dot, P, Pyld, Consistency, RRcvd,Rrcvdsdot},
            #state{vclock=VClock0,
                   notify_function=F,
                   depgraph=Depgraph0,
                   latency=Latency,
                   actor=Actor,
                   unacked_list=Unacked0,
                   rcvd=LRcvd,
                   rcvdsdot=Lrcvdsdot0,
                   bits=N,
                   nodebitmap=Nodebitmap}=State) ->

    ?LOG("---Receiving Remote dot from ~p at ~p with preds:~p, vclock is ~p", [Dot, Actor, P, VClock0]),

     % Case dot not received/delivered before
    {VClockX, DepgraphX, UnackedX, RcvdX, LrcvdsdotX} = case rcvd:is_rcvd(Dot, LRcvd) of
        {error, E} ->
            ?LOG("is_rcvd returned error ~p", [E]),
            {VClock0, Depgraph0, Unacked0, LRcvd,Lrcvdsdot0};
        true ->
            ?LOG("ALREADY SEEN dot ~p", [Dot]),
            {VClock0, Depgraph0, Unacked0, LRcvd,Lrcvdsdot0};
        false ->
           % ?LOG("received: Rrcvdsdot: ~p", [Rrcvdsdot]),
		Cons = Consistency#consistency.consistency,
	    case Cons == strong of
	        true -> 
		       Lrcvdsdot1 = [Dot| Lrcvdsdot0];
            false ->
                Lrcvdsdot1 = Lrcvdsdot0
            end,
            B0 = orddict:fetch(Id, Nodebitmap),
	    {Depgraph00, Lrcvdsdot2} = check_strong_queries(Depgraph0, B0, Lrcvdsdot1, Rrcvdsdot, F, Latency, Nodebitmap),
          %  ?LOG("CAN try to deliver dot ~p", [Dot]),
           % ?LOG("In deliver: Rrcvdsdot: ~p", [Rrcvdsdot]),
           % ?LOG("In deliver: Lrcvdsdot0: ~p", [Lrcvdsdot0]),
           % ?LOG("In deliver: Lrcvdsdot1: ~p", [Lrcvdsdot1]),
           % ?LOG("In deliver: Lrcvdsdot2: ~p", [Lrcvdsdot2]),
          %  ?LOG("In deliver: Depgraph00: ~p", [Depgraph00]),

     		LRcvd1 = rcvd:add_dot(Dot, LRcvd),	
            %% TODO ack unacked
            {ResendList, Unacked1} = unacked:ack(RRcvd, N - B0, Unacked0),

            %% TODO prepend remotemsg, append Rcvd, resend
            prepare_resend(ResendList, LRcvd, Latency, Id),

            %% filter stable
            Filt = fun(K, V) -> not is_stable({K, V}, VClock0, Depgraph0) end,
            Preds = maps:filter(Filt, P),

            % For each predecessor, if not in graph add SLT
            % then add Dot to the succ of every pred
            % Bor/sum the provenenance/node bits of every undelivered pred
           % ?LOG("In deliver: middleware3: Dot is ~p", [Dot]),
           % ?LOG("In deliver: middleware3: P is ~p", [P]),
           % ?LOG("In deliver: middleware3: Preds are ~p", [Preds]),
            {Depgraph2, B} = maps:fold(
                fun(K, V, {AccG, AccB}) ->
                    ?LOG("In deliver: middleware3: a pred is {~p, ~p}", [K, V]),

                    Bx = orddict:fetch(K, Nodebitmap),
                    ?LOG("In deliver: middleware3: Bx is ~p", [Bx]),

                    {Depgraph1, B} = case depgraph:is_element({K, V}, AccG) of
                        true ->
                            {AccG, case depgraph:get({K, V}, stage, AccG) < 2  of
                                true ->
                                    ?LOG("In deliver: pred :~p,~p stage : ~p", [K, V, depgraph:get({K, V}, stage, AccG)]),
                                    ?LOG("In deliver: pred is element and not DLV with AccB is ~p", [AccB bor Bx]),
                                    AccB bor Bx;
                                false ->
                                    ?LOG("In deliver: pred is element and DLV with AccB is ~p", [AccB]),
                                    AccB
                            end};
                        false ->
                            {depgraph:add_with({K, V}, [{stage, ?SLT}, {succ, maps:new()}], AccG), AccB bor Bx}
                    end,
                    Succ0 = depgraph:get({K, V}, succ, Depgraph1),
                    Succ1 = maps:put(Id, Ctr, Succ0),
                    {depgraph:update({K, V}, [{succ, Succ1}], Depgraph1), B}
                end,
                {Depgraph00, 0},
                Preds
            ),
          %  ?LOG("In deliver: middleware3: B is ~p", [B]),
            % prepare values list
            List = [{stage, ?RCV}, {bitstr, B}, {pred, Preds}, {consistency, Cons}, {pyld, Pyld}],
            % if SLT update, else add with succ empty
            Depgraph3 = case depgraph:is_element(Dot, Depgraph2) of
                true ->
                    depgraph:update(Dot, List, Depgraph2);
                false ->
                    depgraph:add_with(Dot, [{succ, maps:new()}|List], Depgraph2)
            end,
            % if B=0 means all preds of Dot are delivered and so deliver Dot
             {VClock1, Depgraph4} = case B == 0 andalso Cons == causal of
                true ->
                    ?LOG("CAN deliver dot: ~p with Cons: ~p", [Dot, Cons]),
                   deliver(Dot, VClock0, Depgraph3, F, Nodebitmap, N);
                false ->
                    ?LOG("CANNOT deliver dot ~p with preds ~p, vclock is ~p", [Dot, P, VClock0]),
                    {VClock0, Depgraph3}
            end,
            {VClock1, Depgraph4, Unacked1, LRcvd1, Lrcvdsdot2}
    end,

    {noreply, State#state{vclock=VClockX, depgraph=DepgraphX, unacked_list=UnackedX, rcvd=RcvdX,rcvdsdot=LrcvdsdotX}};
%*******
handle_cast({deliver_strong, {Id, _Ctr}=Dot, Rrcvdsdot},
            #state{vclock=VClock0,
                   notify_function=F,
                   depgraph=Depgraph0,
                   latency=Latency,
                   %actor=Actor,
                   %unacked_list=Unacked0,
                   %rcvd=LRcvd,
                   rcvdsdot=Lrcvdsdot,
                   bits=N,
                   nodebitmap=Nodebitmap}=State) ->

% deliver strong query
    B0 = orddict:fetch(Id, Nodebitmap),
    {Depgraph1, Lrcvdsdot1} = check_strong_queries(Depgraph0,B0, Lrcvdsdot, Rrcvdsdot, F, Latency, Nodebitmap),
            %?LOG("********: Lrcvdsdot0: ~p", [Lrcvdsdot]),
            %?LOG("********: Lrcvdsdot1: ~p", [Lrcvdsdot1]),
    B2 = depgraph:get(Dot, bitstr, Depgraph0),
    {VClockX, DepgraphX, LrcvdsdotX} = case B2 == 0 of
    true ->
            ?LOG("deliver strong query: ~p", [Dot]),
        {VClock1, Depgraph2} =    deliver(Dot, VClock0, Depgraph1, F, Nodebitmap, N),
        Lrcvdsdot2 = lists:delete(Dot, Lrcvdsdot1),
	    {VClock1, Depgraph2, Lrcvdsdot2};
    false -> 
            ?LOG("strong query cant be delivered has dependencies: dot:~p, has dependencies:~p", [Dot,depgraph:get(Dot, pred, Depgraph0)]),
	    Depgraph2 = depgraph:update(Dot, [{consistency, causal}], Depgraph1),
        {VClock0, Depgraph2, Lrcvdsdot1}
    end,
%*******
{noreply, State#state{vclock=VClockX, depgraph=DepgraphX, rcvdsdot=LrcvdsdotX}};


handle_cast({stable, Dot},
            #state{depgraph=Depgraph0}=State) ->
    ?LOG("Stable Dot: ~p", [Dot]),

    %Depgraph1 = deletestable(Dot, Depgraph0),
    {noreply, State#state{depgraph=Depgraph0}};

handle_cast(Msg, State) ->
    lager:warning("Unhandled cast messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), state_t()) -> {noreply, state_t()}.
handle_info(Msg, State) ->
    lager:warning("Unhandled info messages: ~p", [Msg]),
    {noreply, State}.

%% @private
-spec terminate(term(), state_t()) -> term().
terminate(_Reason, _State) ->
    ok.

%% @private
-spec code_change(term() | {down, term()}, state_t(), term()) -> {ok, state_t()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% middleware internal private functions and procedures
%%%===================================================================

%% @private
-spec deliver(dot(), vclock(), depgraph(), fun(), orddict:orddict(), non_neg_integer()) -> {vclock(), depgraph()}.
deliver({Id, _}=Dot, VClock, Depgraph, F, Nodebitmap, N) ->
    P = depgraph:get(Dot, pred, Depgraph),
    Pyld = depgraph:get(Dot, pyld, Depgraph),
    F({camus, {deliver, Pyld, {Dot, P}}}),
  %  ?LOG("Delivered dot ~p with preds ~p, vclock is ~p", [Dot, P, VClock]),
    %% update vv
    VClock1 = vclock:update_dot(Dot, VClock),
    Cons = depgraph:get(Dot, consistency, Depgraph),
    %% Update stage and bitstr
    Bi = orddict:fetch(?UTIL:get_node(), Nodebitmap),
    Bj = orddict:fetch(Id, Nodebitmap),
    B1 = N - Bj,
    %% ts TF is the timestamp used sinnce something is delivered until stabilized
    case Cons == causal of
    true ->	
          Depgraph1 = depgraph:update(Dot, [{stage, ?DLV}, {bitstr, N - (Bi bor Bj)}], Depgraph),
          %% update stability
           % ?LOG("update stability of causal dot", []),
          Depgraph2 = update_stability(B1, Dot, Depgraph1, F);
    false ->
            %?LOG("stabilise strong dot", []),
           Depgraph2 = stabilize(Dot, Depgraph, F)
    end, 
    maps:fold(
        fun(K, V, {AccVC, AccDepgraph}) ->
            ?LOG("succ: {~p, ~p}", [K, V]),
            B2 = depgraph:get({K, V}, bitstr, AccDepgraph),
	    Cons1 = depgraph:get({K, V}, consistency, AccDepgraph), 	
          %  ?LOG("bitstring of succ is: ~p", [B2]),
          %  ?LOG("new update B1 of succ is: ~p", [B1]),
            NewB = B2 band B1,
          %  ?LOG("after update band B1 of succ is: ~p", [NewB]),
            Depgraph3 = depgraph:update({K, V}, [{bitstr, NewB}], AccDepgraph),
            case NewB == 0 andalso Cons1 == causal of
                true ->
                    ?LOG("dlvr succ "),
                    deliver({K, V}, AccVC, Depgraph3, F, Nodebitmap, N);
                false ->
                    ?LOG(" CANNOT dlvr succ"),
                    {AccVC, Depgraph3}
            end
        end,
        {VClock1, Depgraph2},
        depgraph:get(Dot, succ, Depgraph2)
    ).
%% @private
%spec check_strong_queries(depgraph(),non_neg_integer(), lists(), lists(), fun()) -> {depgraph(), lists()}.
check_strong_queries(Depgraph0,B0,Lrcvdsdot,Rrcvdsdot, F, Latency, Nodebitmap)->
	Mdots = maps:from_list([{Dot,0}||Dot<-Rrcvdsdot]),
			%?LOG("+++++map dots: ~p", [Mdots]),
    maps:fold(
        fun(Dot,_V,{AccDepgraph, Accdot})->
			%?LOG("+++++checking strong query: ~p,~p", [Dot,V]),
           case lists:member(Dot, Accdot) of
           true->
             B1 = depgraph:get(Dot, bitstr, AccDepgraph),
             B2 = B0 band B1,
			%?LOG("++++++bitstring B0= ~p, B1 = ~p, B2 = ~p", [B0,B1,B2]),
             case B2 == 0 of 
                 true ->
		    % 	?LOG("+++++++check strong query: true", []),
		            Pyld = depgraph:get(Dot, pyld, AccDepgraph),
					P = depgraph:get(Dot, pred, AccDepgraph),
		    	?LOG("+++++++Pyld: ~p", [Pyld]),
		            F({camus, {deliver, Pyld, {Dot, P}}}),
                    Depgraph1 = stabilize(Dot, AccDepgraph, F),
                    Accdot1 = lists:delete(Dot, Accdot),
		            M = {deliver_strong, Dot, Accdot},
		            Peers = orddict:fetch_keys(Nodebitmap),
		            ?UTIL:bcast1(M, ?MODULE, Peers, Latency),
				  %  timer:sleep(15),			
                    {Depgraph1, Accdot1};
                 false ->
                    Depgraph1 = depgraph:update(Dot, [{bitstr, B2}], AccDepgraph),
		     %	?LOG("++++++check strong query: false", []),
                    {Depgraph1, Accdot}
             end;
            false->
		    {AccDepgraph, Accdot}
            end
        end, 
        {Depgraph0, Lrcvdsdot},
        Mdots
).
%% @private
-spec update_stability(non_neg_integer(), dot(), depgraph(), fun()) -> depgraph().
update_stability(B, Dot, Depgraph0, F) ->
    maps:fold(
        fun(K, V, Acc) ->
            case depgraph:get({K, V}, stage, Acc) of
                ?STB ->
                    Acc;
                _ ->
                    B1 = depgraph:get({K, V}, bitstr, Acc),
                   % ?LOG("update_stability of Dot: ~p", [Dot]),
                   % ?LOG("B1 of Dot Pred: ~p", [B1]),
                   % ?LOG("incoming B of Dot: ~p", [B]),
                    B2 = B1 band B,
                    %?LOG("B band B1: ~p", [B2]),
                    case B1 =/= B2 of
                        true ->
                            Depgraph1 = depgraph:update({K, V}, [{bitstr, B2}], Acc),
                            case B2 == 0 of
                                true ->
                                    stabilize({K, V}, Depgraph1, F);
                                false ->
                                    update_stability(B, {K, V}, Depgraph1, F)
                            end;
                        false ->
                            Acc
                    end
            end
        end,
        Depgraph0,
        depgraph:get(Dot, pred, Depgraph0)
    ).

%% @private
-spec stabilize(dot(), depgraph(), fun()) -> depgraph().
stabilize(Dot, Depgraph0, F) ->
   % ?LOG("stabilize Dot: ~p", [Dot]),
    Preds = depgraph:get(Dot, pred, Depgraph0),
  %  ?LOG("stabilize Preds: ~p", [Preds]),
    Depgraph1 = maps:fold(
        fun(K, V, Acc) ->
            case depgraph:get({K, V}, stage, Acc) of
                ?STB ->
                    Acc;
                _ ->
                  %  ?LOG("stabilize Pred is not stable: ~p", [{K, V}]),
                    stabilize({K, V}, Acc, F)
            end
        end,
        Depgraph0,
        Preds
    ),
    Pyld = depgraph:get(Dot, pyld, Depgraph1),
    Depgraph2 = depgraph:update(Dot, [{stage, ?STB}], Depgraph1),
  %  ?LOG("Marked as Stable Dot: ~p", [Dot]),
    F({camus, {stable, Pyld, {Dot, Preds}}}),
    Depgraph2.

%% @private
%-spec deletestable(dot(), depgraph()) -> depgraph().
%deletestable({Id, _}=Dot, Depgraph0) ->
  %  ?LOG("in deletestable, Dot: ~p", [Dot]),
%    Succ = depgraph:get(Dot, succ, Depgraph0),
  %  ?LOG("in deletestable, Succ: ~p", [Succ]),
%    Depgraph1 = maps:fold(
%        fun(K, V, Acc) ->
%            P = depgraph:get({K, V}, pred, Acc),
   %         ?LOG("in deletestable, P: ~p", [P]),
%            Preds = maps:remove(Id, P),
    %        ?LOG("in deletestable, Preds: ~p", [Preds]),
%            depgraph:update({K, V}, [{pred, Preds}], Acc)
%        end,
%        Depgraph0,
%        Succ
%    ),
%    depgraph:delete(Dot, Depgraph1).

%% @private
-spec is_stable(dot(), vclock(), depgraph()) -> boolean().
%% checks if a dot D is stable.
%% D stable if D already delivered and:
%% - ready to be stable i.e. STB
%% - already stablize and not in G
is_stable(D, V, G) ->
    vclock:is_element(D, V)
    andalso case depgraph:is_element(D, G) of
        true ->
            depgraph:get(D, stage, G) == ?STB;
        false ->
            true
    end.

%% @private
prepare_resend(ResendList, Rcvd, Latency, SenderId) ->
    lists:foreach(
        fun({Dot, Ctxt, Msg}) ->
            M = {remotemsg, Dot, Ctxt, Msg, Rcvd},
            ?UTIL:send(M, ?MODULE, SenderId, Latency)
        end,
    ResendList).
