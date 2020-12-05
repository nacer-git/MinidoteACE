-module(minidote_server).
-behavior(gen_server).

-include("minidote.hrl").

-define(INTERVAL, 1000).

%% API
-export([init/1, handle_call/3, handle_cast/2, terminate/2, handle_info/2, code_change/3, read_objects/3, update_objects/4, stop/1, start_link/1]).

-export_type([key/0]).

-record(finish_update_objects, {from, new_crdt_state_map, new_vc :: vectorclock:clock(), answer:: boolean()}).

-type key() :: {Key :: binary(), Type :: antidote_crdt:typ(), Bucket :: binary()}.
-type from() :: {pid(), Tag :: term()}.

-record(state, {
  crdt_states = #{} :: maps:map(minidote:key(), antidote_crdt:crdt()),
  vc = vectorclock:new() :: vectorclock:clock(),
  self :: node(),
  dot = dot:new() :: dot(),
  ctxt = context:new() :: context(),
  waiting_requests = #{} :: maps:map(node(), priority_queue:pq({non_neg_integer(), from(), request()})),
  locks = #{} :: maps:map(key(), boolean()),
  locks_waiting = #{} :: maps:map(node(), queue:queue({from(), request()}))
}).

-record(downstream, {
  new_vc :: vectorclock:vectorclock(),
  new_effects :: [{minidote:key(), [antidote_crdt:effect()]}]
}).

-record(message, {
  new_vc :: vectorclock:vectorclock(),
  updates :: [{key(), Op :: atom(), Args :: any()}],
  clock :: vectorclock:vectorclock(),
  sender :: node(),
  frm :: from()
}).
-record(consistency, {
  consistency :: atom()
}).
-record(read_objects, {
  keys :: [key()],
  clock :: vectorclock:vectorclock()
}).

-record(update_lockobj, {
  updates :: [{key(), Op :: atom(), Args :: any()}],
  new_vc :: vectorclock:vectorclock(),
  clock :: vectorclock:vectorclock(),
  answer :: atom
}).
-record(update_objects, {
  updates :: [{key(), Op :: atom(), Args :: any()}],
  consistency :: atom(),
  clock :: vectorclock:vectorclock()
}).

-record(camus, {
  opq :: any()
}).

-record(update, {
  ds :: #downstream{},
  fob :: #finish_update_objects{}
}).

-record(bcastupd, {
  msg :: #message{},
  cons :: #consistency{}
%  fob :: #finish_update_objects{}
}).

-type request() :: #read_objects{} | #update_objects{}.
-type server() :: pid() | atom().


-spec start_link(atom()) -> {ok, server()} | ignore | {error, Error :: any()}.
start_link(ServerName) ->
  gen_server:start_link({local, ServerName}, ?MODULE, [], []).

stop(Pid) ->
  gen_server:stop(Pid).

-spec read_objects(server(), [key()], vectorclock:vectorclock()) ->
  {ok, [any()], vectorclock:vectorclock()}
  | {error, any()}.
read_objects(Server, Objects, Clock) ->
  gen_server:call(Server, #read_objects{keys = Objects, clock = Clock}).

-spec update_objects(server(), [{key(), Op :: atom(), Args :: any()}], vectorclock:vectorclock(), Consistency :: atom()) ->
  {ok, vectorclock:vectorclock()}
  | {error, any()}.
update_objects(Server, Updates, Clock, Consistency) ->
  case Clock of ignore -> ok; X when is_map(X) -> ok; _ -> throw({not_a_valid_clock, Clock}) end,
  gen_server:call(Server, #update_objects{updates = Updates, clock = Clock, consistency = Consistency}, 30000).

init(_Args) ->
  Members = os:getenv("MEMBERS", ""),
  List = string:split(Members, ",", all),
 % lager:info("List of members: ~p", [List]),
  Me = case os:getenv("NODE_NAME", "") of
    "" -> node();
    V -> list_to_atom(V)
  end,
  Others = connect(List),
  lager:info("Me ~p | Others ~p", [Me, Others]),

  CrdtStates = maps:new(),
  Self = self(),
  F=fun(M) -> 
    Self ! M
  end,
  camus:setnotifyfun(F),
  camus:setmembership(Others),

  Dot = dot:new_dot(Me),
  Ctxt = context:new(),
  {ok, #state{dot = Dot, ctxt = Ctxt, crdt_states = CrdtStates, self=Me}}.

handle_call(#update_objects{updates = _Upd,consistency = Consistency} = Req, From, State) ->
  NewState = handle_update_objects(Req, From, State),
  {noreply, NewState};
handle_call(#read_objects{} = Req, From, State) ->
  NewState = handle_read_objects(Req, From, State),
  {noreply, NewState};
handle_call(#update_lockobj{} = Req, From, State) ->
  NewState = apply_upd_objects(Req, From, State),
  {noreply, NewState};
handle_call(Req={camus, Opaque}, From, #state{dot=Dot, ctxt=Ctxt, self = ThisNode}=State1) ->
   {Type, {#message{new_vc = New_Vc, updates = Upds, clock = Clk, sender = Sender, frm = Frm}, _TS}, {NewDot, NewCtxt}} = camus:handle(Opaque, {Dot, Ctxt}),
%   Objects = [Obj || {Obj, _, _} <- Upds],
        %   lager:info("Sender: ~p, ThisNode:~p", [Sender, ThisNode]),
   NewState = case Type of
      deliver -> 
        case Sender == ThisNode of
        true ->  %deliver strong local queries after receiving acknowledgment
          % lager:info("apply strong local: ~p", [Upds]),
	       State2=apply_upd_objects(#update_lockobj{updates = Upds, clock = Clk, new_vc=New_Vc, answer=true}, Frm, State1),
		  % gen_server:reply(Frm, {ok, New_Vc}),
	       State2;
        false -> %deliver remote queries 
          % lager:info("apply remote: ~p", [Upds]),
           State = State1#state{dot=NewDot, ctxt=NewCtxt},
		  % timer:sleep(50),
           State2=apply_upd_objects(#update_lockobj{updates = Upds, clock = ignore,new_vc=New_Vc, answer=false}, Frm, State),
		  % timer:sleep(50),
		   State2
            end;
      stable ->
             State1
    end,
%  NewState1 = NewState#state{vc = vectorclock:merge(NewState#state.vc, New_Vc)},

      %% receiving downstream effects from other servers
      %% apply the effects to the current state
       %  handle_request_wih_dependency_clock(Req, Pyld#msg.frm, State, ignore, Objects, fun(State) ->
        %  deliver_update(State, Pyld#downstream.new_vc, Effects)

  check_waiting_requests(NewState),
  {noreply, NewState};
%% new receive camus updates
handle_call(#update{} = Req, _From, State) ->
  Ds = Req#update.ds,
  {NewDot, NewCtxt} = camus:cbcast(Ds, {State#state.dot, State#state.ctxt}),
  Fob = Req#update.fob,
  gen_server:cast(self(), Fob),
  {reply, ok, State#state{dot=NewDot, ctxt=NewCtxt}};
%% new handle_call bcastupd
handle_call(#bcastupd{} = Req, _From, State) ->
  Msg = Req#bcastupd.msg,
  Cons = Req#bcastupd.cons,
   %   lager:info("bcastupd &&&&: ~p", [Msg]),
  {NewDot, NewCtxt} = camus:cbcast(Msg, Cons,{State#state.dot, State#state.ctxt}),
  {reply, ok, State#state{dot=NewDot, ctxt=NewCtxt}};
handle_call(_Request, _From, _State) ->
  erlang:error(not_implemented).

handle_cast(#finish_update_objects{from = From, new_crdt_state_map = NewCrdtStatesMap, new_vc = NewVc, answer = Answer}, State) ->
  NewVc1 = vectorclock:merge(State#state.vc, NewVc),
  Keys = maps:keys(NewCrdtStatesMap),
  NewState = State#state{
    vc          = NewVc1,
    crdt_states = maps:merge(State#state.crdt_states, NewCrdtStatesMap),
    % release locks
    locks       = maps:without(Keys, State#state.locks)
  },
      lager:info("finish_update &&&&: ~p", [NewCrdtStatesMap]),
       ok = if Answer==true -> gen_server:reply(From, {ok, NewVc1});
			  true -> ok
		   end,
		 
  check_locks_waiting(NewState, Keys),
  check_waiting_requests(NewState),
  {noreply, NewState};
handle_cast(_Request, _State) ->
  erlang:error(not_implemented).

handle_info(Req=#camus{}, State) ->
  handle_call(Req, undef, State);
handle_info({handle_waiting, P}, State) ->
  Waiting = maps:get(P, State#state.waiting_requests, priority_queue:new()),
  case priority_queue:out(Waiting) of
    {{value, {_V, From, Req}}, NewQ} ->
      Waiting2 = case priority_queue:is_empty(NewQ) of
        true ->
          maps:remove(P, State#state.waiting_requests);
        false ->
          maps:put(P, NewQ, State#state.waiting_requests)
      end,
      State2 = State#state{waiting_requests = Waiting2},
	%	  case Req == #update_objects of
	%		  true -> apply_update_objects(Req, From, State2);
	%		  false -> handle_read_objects(Req, From, State2)
		  %end,
      handle_call(Req, From, State2);
    {empty, _} ->
      {noreply, State}
  end;
handle_info({handle_release_lock, Key}, State) ->
  case maps:get(Key, State#state.locks, false) of
    true ->
      % lock acquired by someone else
      {noreply, State};
    false ->
      Waiting = maps:get(Key, State#state.locks_waiting, priority_queue:new()),
      case queue:out(Waiting) of
        {{value, {From, Req}}, NewQ} ->
          Waiting2 = case queue:is_empty(NewQ) of
            true ->
              maps:remove(Key, State#state.locks_waiting);
            false ->
              maps:put(Key, NewQ, State#state.locks_waiting)
          end,
          State2 = State#state{locks_waiting = Waiting2},
		 % case Req == #update_objects{} of
		%	  true -> apply_update_objects(Req, From, State2);
		%	  false -> handle_read_objects(Req, From, State2)
		 % end,
          handle_call(Req, From, State2);
        {empty, _} ->
          {noreply, State}
      end
  end;
handle_info(log_recovery_done, State) ->
  {noreply, State};
handle_info(Request, _State) ->
  erlang:error({not_implemented, Request}).

deliver_downstream(State, Vc, Effects) ->
  NewVc = vectorclock:merge(State#state.vc, Vc),
  NewCrdtStatesMap = maps:from_list([apply_effects(State, Key, Effs) || {Key, Effs} <- Effects]),
  State#state{
    vc          = NewVc,
    crdt_states = maps:merge(State#state.crdt_states, NewCrdtStatesMap)
  }.

%% new deliver updates
deliver_update(State, Vc, Effects) ->
  NewVc = vectorclock:merge(State#state.vc, Vc),
  NewCrdtStatesMap = maps:from_list([apply_effects(State, Key, Effs) || {Key, Effs} <- Effects]),
  State#state{
    vc          = NewVc,
    crdt_states = maps:merge(State#state.crdt_states, NewCrdtStatesMap)
  }.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.

crdt_state(BoundObj, State) ->
  case maps:find(BoundObj, State#state.crdt_states) of
    {ok, S} ->
      S;
    error ->
      {_Key, CrdtType, _Bucket} = BoundObj,
      antidote_crdt:new(CrdtType)
  end.

apply_effects(State, BoundObj, Effects) ->
  {_Key, CrdtType, _Bucket} = BoundObj,
  CrdtState = crdt_state(BoundObj, State),
  NewCrdtState = lists:foldl(fun(Effect, S) ->
    {ok, S2} = antidote_crdt:update(CrdtType, Effect, S),
    S2
  end, CrdtState, Effects),
  {BoundObj, NewCrdtState}.

perform_updates(CrdtState, Updates) ->
  {S, Effs} = lists:foldl(fun({BoundObj, Op, Args}, {S, Effs}) ->
    {_Key, CrdtType, _Bucket} = BoundObj,
    true = antidote_crdt:is_type(CrdtType),
    Update = {Op, Args},
    {ok, Effect} = antidote_crdt:downstream(CrdtType, Update, S),
    {ok, NewCrdtState} = antidote_crdt:update(CrdtType, Effect, S),
    {NewCrdtState, [Effect | Effs]}
  end, {CrdtState, []}, Updates),
  {S, lists:reverse(Effs)}.

state_to_value({BoundObj, CrdtState}) ->
  {_Key, CrdtType, _Bucket} = BoundObj,
  Value = antidote_crdt:value(CrdtType, CrdtState),
  {BoundObj, Value}.

% checks if Clock <= State.vs
% if yes, calls handler
% if no, add the request to the waiting requests and calls it again later
handle_request_wih_dependency_clock(Req, From, State, Clock, KeysToLock, Handler) ->
  case Clock == ignore orelse vectorclock:leq_extra(Clock, State#state.vc) of
    true ->
      case lists:filter(fun(K) -> maps:get(K, State#state.locks, false) end, KeysToLock) of
        [Key | _] ->
          % add request to waiting requests of first key
          LocksWaiting = State#state.locks_waiting,
          KeyWaiting = maps:get(Key, LocksWaiting, queue:new()),
          KeyWaiting2 = queue:in({From, Req}, KeyWaiting),
          LocksWaiting2 = maps:put(Key, KeyWaiting2, LocksWaiting),
          State#state{locks_waiting = LocksWaiting2};
        [] ->
          % no current locks on used keys, can handle request directly
          Handler(State)
      end;
    {false, P, V} ->
      % add to pending
      WaitingRequests = State#state.waiting_requests,
      PWaiting = maps:get(P, WaitingRequests, priority_queue:new()),
      PWaiting2 = priority_queue:in({V, From, Req}, PWaiting),
      NewWaitingRequests = maps:put(P, PWaiting2, WaitingRequests),
      State#state{waiting_requests = NewWaitingRequests}
  end.

handle_read_objects(#read_objects{keys = Objects, clock = Clock} = Req, From, State1) ->
  handle_request_wih_dependency_clock(Req, From, State1, Clock, [], fun(State) ->
    CrdtStates = [{K, crdt_state(K, State)} || K <- Objects],
    Vc = State#state.vc,
    spawn_link(fun() ->
      Results = list_utils:pmap(fun state_to_value/1, CrdtStates),
      gen_server:reply(From, {ok, Results, Vc})
    end),
    State
  end).

%% handle_update
handle_update_objects(#update_objects{updates = Updates, clock = Clock, consistency = Consistency} = Req, From, State1) ->
      Self = State1#state.self,
	      ThisServer = self(),
      % might not be the latest new VC because of concurrent processes, but good estimate
      NewVc = vectorclock:increment(State1#state.vc, State1#state.self),
      spawn_link(fun() ->
   %   lager:info("handldate: ~p, clock:~p, cons: ~p", [Updates, Clock, Consistency]),
      gen_server:call(ThisServer, {bcastupd, #message{new_vc = NewVc, updates = Updates, clock = Clock, sender = Self, frm = From},
								    #consistency{consistency = Consistency}})
	   end),
	    State2 = case Consistency == causal of
		true ->
			% if causal consistency apply updates locally
			apply_upd_objects(#update_lockobj{updates = Updates, clock = Clock, new_vc=NewVc, answer = true}, From, State1);
  		false->
            State1
        end,
	  State2.

apply_upd_objects(#update_lockobj{updates = Updates, clock = Clock, new_vc=NewVc, answer = Answer} = Req, From, State1) ->
  Objects = [O || {O, _, _} <- Updates],
  handle_request_wih_dependency_clock(Req, From, State1, Clock, Objects, fun(State) ->
    ThisServer = self(),
   %   lager:info("apply_update &&&&: ~p", [Updates]),
    % Self = State#state.self,
    UpdatesByKey = list_utils:group_by(fun({K, _, _}) -> K end, Updates),
    Keys = maps:keys(UpdatesByKey),
    UpdatesByKeyWithState = [{K, crdt_state(K, State), Upds} || {K, Upds} <- maps:to_list(UpdatesByKey)],
    % use concurrency below so the server is not blocked for the whole update
    % We use locks on keys to make it still safe
    spawn_link(fun() ->
      % calculate new crdt states
      EffectsAndStates = list_utils:pmap(fun({Key, S, Upds}) ->
        {Key, perform_updates(S, Upds)} end, UpdatesByKeyWithState),
      NewCrdtStates = [{K, S} || {K, {S, _}} <- EffectsAndStates],
      NewEffects = [{K, Effs} || {K, {_, Effs}} <- EffectsAndStates],
      NewCrdtStatesMap = maps:from_list(NewCrdtStates),
      % might not be the latest new VC because of concurrent processes, but good estimate
     % NewVc = vectorclock:increment(State#state.vc, State#state.self),
     % gen_server:call(ThisServer, {update, #downstream{new_vc = NewVc, new_effects = NewEffects}, #finish_update_objects{from = From, new_crdt_state_map = NewCrdtStatesMap}})
        gen_server:cast(ThisServer, #finish_update_objects{from = From, new_crdt_state_map = NewCrdtStatesMap, new_vc=NewVc, answer = Answer})
    end),
    State#state{
      % acquire locks
      locks = maps:merge(State#state.locks, maps:from_list([{K, true} || K <- Keys]))
    }
  end).

% check processes waiting for a Clock
check_waiting_requests(State) ->
  maps:fold(fun(P, Queue, _) ->
    case priority_queue:peek(Queue) of
      {value, {V, _, _}} ->
        case vectorclock:get(State#state.vc, P) >= V of
          true ->
            self() ! {handle_waiting, P};
          false ->
            ok
        end;
      _ ->
        ok
    end
  end, ok, State#state.waiting_requests).

% check processes waiting for locks
check_locks_waiting(State, Keys) ->
  lists:foreach(fun(Key) ->
    case maps:is_key(Key, State#state.locks_waiting) of
      true ->
        self() ! {handle_release_lock, Key};
      false ->
        ok
    end
  end, Keys).

connect([]) ->
    [];
connect([Node|Rest]=All) ->
    {Id, _, _}=Spec = parse(Node),
    case camus_ps:join(Spec) of
        ok ->
            [Id | connect(Rest)];
        Error ->
            lager:info("Couldn't connect to ~p. Reason ~p. Will try again in ~p ms",
                       [Spec, Error, ?INTERVAL]),
            timer:sleep(?INTERVAL),
            connect(All)
    end.

parse(IdStr) ->
    [RestStr, IpStr] = string:split(IdStr, "@"),
    [_, PortStr] = string:split(RestStr, "-"),
    Id = list_to_atom(IdStr),
    Ip = case inet_parse:address(IpStr) of
        {ok, I} -> I;
        {error, Err} ->
          IpStr
      end,
    Port = list_to_integer(PortStr),
    {Id, Ip, Port}.
