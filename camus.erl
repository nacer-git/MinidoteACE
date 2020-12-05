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

-module(camus).
-author("Georges Younes <georges.r.younes@gmail.com>").

-export([start/0,
         stop/0]).

%% API
-export([cbcast/3,
         setnotifyfun/1,
         handle/2]).

%% Testing purposes
-export([setmembership/1,
         getstate/0]).

-include("camus.hrl").

%% @doc Start the application.
start() ->
    application:ensure_all_started(camus).

%% @doc Stop the application.
stop() ->
    application:stop(camus).

%%%===================================================================
%%% API
%%%===================================================================

%% Causal Broadcast message.
-spec cbcast(message(), term(), term()) -> term().
cbcast(Pyld, Consistency, State) ->
    {Dot0, Ctxt0}=State,
    Dot=dot:inc(Dot0),
	    ?LOG("camus cbcast: ~p",[Pyld]),
    ?MIDDLEWARE:cbcast(Pyld, Consistency, {Dot, Ctxt0}),
    Ctxt=context:set_val(Dot),
    {Dot, Ctxt}.

%% Handle.
-spec handle(term(), term()) -> {atom(), {message(), term()}, term()}.
handle({Type, Pyld, TS}, State) ->
    %% extract opaque
    case Type of
        deliver ->
            deliver(Pyld, TS, State);
        stable ->
            stable(Pyld, TS, State)
    end.

-spec deliver(message(), term(), term()) -> {atom(), {message(), term()}, term()}.
deliver(Pyld, TS, State) ->
    {Dot, Ctxt} = State,
    NewState = {Dot, context:add(TS, Ctxt)},
	    ?LOG("deliver: ~p",[Pyld]),
    {deliver, {Pyld, TS}, NewState}.

-spec stable(message(), term(), term()) -> {atom(), {message(), term()}, term()}.
stable(Pyld, TS, State) ->
    {D, _P} = TS,
    gen_server:cast(?MIDDLEWARE, {stable, D}),
    {stable, {Pyld, TS}, State}.

%% Configure the notification function.
-spec setnotifyfun(fun()) -> ok.
setnotifyfun(Fun) ->
    ?MIDDLEWARE:setnotifyfun(Fun).

%% Set membership.
-spec setmembership([id()]) -> ok.
setmembership(NodeList) ->
    ?MIDDLEWARE:setmembership(NodeList).

%% Get state.
-spec getstate() -> term().
getstate() ->
    ?MIDDLEWARE:getstate().
