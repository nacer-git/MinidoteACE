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

-module(camus_util).
-author("Georges Younes <georges.r.younes@gmail.com>").

%% camus_util callbacks
-export([send/4,
         bcast/4,
         bcast/3,bcast1/3,bcast1/4,
         get_timestamp/0,
         get_node/0,
         atom_to_binary/1,
         binary_to_atom/1,
         integer_to_atom/1,
         without_me/1,
         is_str_int/1,
         connection_name/1,
         connection_name/2,
         generate_latency/2]).

-include("camus.hrl").

%% debug
-export([qs/1,
         show/2]).

%%%===================================================================
%%% util common functions
%%%===================================================================

%% @private
send(M, Module, Peer, Latency) ->
    % ?PS:forward_message(Peer, Module, M),
    L = maps:get({node(), Peer}, Latency),
    ?PS:forward_message_with_latency(Peer, Module, M, L).

%% @private
bcast(M, Module, Peers) ->
    bcast(M, Module, Peers, 0).
bcast(M, Module, Peers, Latency) ->
    {A, B, C, D, F, Rcvd, Rcvdsdot} = M,
    lists:foreach(
        fun(Peer) ->
            E = maps:get(Peer, Rcvd),
			Rcvdsdot1 = lists:filter(fun({K, _V}) -> K == Peer end, Rcvdsdot),
            % ?LOG("bcasting to ~p", [Peer]),
           send({A, B, C, D, F, E, Rcvdsdot1}, Module, Peer, Latency)
        end,
    without_me(Peers)).
bcast1(M, Module, Peers) ->
    bcast1(M, Module, Peers, 0).
bcast1(M, Module, Peers, Latency) ->
    {A, B, Rcvdsdot} = M,
    lists:foreach(
        fun(Peer) ->
			Rcvdsdot1 = lists:filter(fun({K, _V}) -> K == Peer end, Rcvdsdot),
            % ?LOG("bcasting commit to ~p", [Peer]),
           send({A, B, Rcvdsdot1}, Module, Peer, Latency)
        end,
    without_me(Peers)).
%% @private get current time in milliseconds
-spec get_timestamp() -> integer().
get_timestamp() ->
  % {Mega, Sec, Micro} = os:timestamp(),
  % (Mega*1000000 + Sec)*1000 + round(Micro/1000).
  os:perf_counter(?UNIT).

%% @private
get_node() ->
    node().

%% @doc
-spec atom_to_binary(atom()) -> binary().
atom_to_binary(Atom) ->
    erlang:atom_to_binary(Atom, utf8).

%% @doc
-spec binary_to_atom(binary()) -> atom().
binary_to_atom(Binary) ->
    erlang:binary_to_atom(Binary, utf8).

%% @doc
-spec integer_to_atom(integer()) -> atom().
integer_to_atom(Integer) ->
    list_to_atom(integer_to_list(Integer)).

%% @doc
-spec connection_name(camus_id()) -> atom().
connection_name(Id) ->
    RandomIndex = rand:uniform(?CONNECTIONS),
    connection_name(Id, RandomIndex).

%% @doc
-spec connection_name(camus_id(), non_neg_integer()) -> atom().
connection_name(Id, Index) ->
    list_to_atom(atom_to_list(Id) ++ "_" ++ integer_to_list(Index)).

%% @doc Log Process queue length.
qs(ID) ->
    {message_queue_len, MessageQueueLen} = process_info(self(), message_queue_len),
    lager:info("MAILBOX ~p REMAINING: ~p", [ID, MessageQueueLen]).

%% @doc Pretty-print.
-spec show(id | dot | dots | vector | ops, term()) -> term().
show(id, Id) ->
    lists:nth(1, string:split(atom_to_list(Id), "@"));
show(dot, {Id, Seq}) ->
    {show(id, Id), Seq};
show(dots, Dots) ->
    lists:map(fun(Dot) -> show(dot, Dot) end, Dots);
show(vector, VV) ->
    %% only show seqs for VVs
    Dots = show(dots, maps:to_list(VV)),
    {_, Seqs} = lists:unzip(lists:sort(Dots)),
    erlang:list_to_tuple(Seqs);
show(ops, Ops) ->
    lists:map(
        fun({_, _, Dot, VV, From}) ->
            {show(dot, Dot), show(vector, VV), show(id, From)}
        end,
        Ops
    ).

%% @private
without_me(Members) ->
    Members -- [node()].

%% @private
generate_latency(Latency, Nodes) ->
    case Latency of
        undefined ->
            generate_default_latency(0, Nodes);
        L when is_integer(L) ->
            generate_default_latency(L, Nodes);
        List when is_list(List) ->
            generate_latency_matrix(List, Nodes);
        _ ->
            error
    end.

%% @private
generate_latency_matrix(L, Nodes) ->
    Sorted = lists:usort(Nodes),
    Len = length(Sorted),
    {A, _} = lists:foldl(
        fun(I, Acc) ->
            NodeI = lists:nth(I, Sorted),
            lists:foldl(
                fun(J, {Mx, LatList}) ->
                    NodeJ = lists:nth(J, Sorted),
                    case I == J of
                        true ->
                            {maps:put({NodeI, NodeJ}, 0, Mx), LatList};
                        false ->
                            Lat = lists:nth(1, LatList),
                            Mx1 = maps:put({NodeI, NodeJ}, Lat, Mx),
                            {maps:put({NodeJ, NodeI}, Lat, Mx1), lists:nthtail(1, LatList)}
                    end
                end,
                Acc,
            lists:seq(I, Len))
        end,
        {maps:new(), L},
    lists:seq(1, Len)),
    A.

%% @private
generate_default_latency(L, Nodes) ->
    Sorted = lists:usort(Nodes),
    Len = length(Sorted),
    lists:foldl(
        fun(I, Acc) ->
            NodeI = lists:nth(I, Sorted),
            lists:foldl(
                fun(J, Acc2) ->
                    NodeJ = lists:nth(J, Sorted),
                    case I == J of
                        true ->
                            maps:put({NodeI, NodeJ}, 0, Acc2);
                        false ->
                            Mx1 = maps:put({NodeI, NodeJ}, L, Acc2),
                            maps:put({NodeJ, NodeI}, L, Mx1)
                    end
                end,
                Acc,
            lists:seq(I, Len))
        end,
        maps:new(),
    lists:seq(1, Len)).

%% @private
is_str_int(S) ->
  try
    _ = list_to_integer(S),
    true
  catch error:badarg ->
    false
  end.
