-module(minidote_pb).
-include_lib("antidote_pb_codec/include/antidote_pb.hrl").
-export([process/1]).

% processes a request received from the protocol buffer interface
% and forwards it to the minidote_server
-spec process(antidote_pb_codec:request()) -> antidote_pb_codec:response().
process({static_update_objects, Clock, Properties, Updates}) ->
 {exclusive_locks, Property1} = hd(Properties),
     lager:info("consistency : ~p~n", [binary_to_atom(hd(Property1))]),
   case minidote:update_objects(Updates, decode_clock(Clock), binary_to_atom(hd(Property1))) of
    {ok, Clock2} ->
      {commit_transaction_response, {ok, encode_clock(Clock2)}};
    {error, {Code, Message}} when is_atom(Code) andalso is_binary(Message) ->
      {error_response, {Code, Message}};
    {error, Reason} ->
      {error_response, {unknown, lists:flatten(io_lib:format("~p", [Reason]))}}
  end;
process({static_read_objects, Clock, _Properties, BoundObjects}) ->
  case minidote:read_objects(BoundObjects, decode_clock(Clock)) of
    {ok, Results, Clock2} ->
      {static_read_objects_response, {Results, encode_clock(Clock2)}};
    {error, {Code, Message}} when is_atom(Code) andalso is_binary(Message) ->
      {error_response, {Code, Message}};
    {error, Reason} ->
      {error_response, {unknown, lists:flatten(io_lib:format("~p", [Reason]))}}
  end;
% The protocol buffer interface is taken from Antidote. Antidote supports
% transactions, but Minidote does not. To be kind of compatible, we
% implement the transaction interface below, but do not provide transactional
% semantics. This simply calls the same functions as the code above.
process({start_transaction, _, Property}) ->
 {exclusive_locks, Property1} = hd(Property),
	{start_transaction_response, {ok, hd(Property1)}};
%	{start_transaction_response, {ok, list_to_binary(Property)}};
process({update_objects, Updates, TxId}) ->
  case minidote:update_objects(Updates, ignore, erlang:binary_to_atom(TxId)) of
    {ok, _Clock} ->
      {operation_response, ok};
    {error, {Code, Message}} when is_atom(Code) andalso is_binary(Message) ->
      {error_response, {Code, Message}};
    {error, Reason} ->
      {error_response, {unknown, lists:flatten(io_lib:format("~p", [Reason]))}}
  end;
process({read_objects, BoundObjects, _TxId}) ->
  case minidote:read_objects(BoundObjects, ignore) of
    {ok, Results, _Clock} ->
      {read_objects_response, {ok, Results}};
    {error, {Code, Message}} when is_atom(Code) andalso is_binary(Message) ->
      {error_response, {Code, Message}};
    {error, Reason} ->
      {error_response, {unknown, lists:flatten(io_lib:format("~p", [Reason]))}}
  end;
process({commit_transaction, _TxID}) ->
  {commit_transaction_response, {ok, <<"ignore">>}};
process({abort_transaction, _TxId}) ->
  {operation_response, ok};
process(Message) ->
  lager:error("Received unhandled message ~p~n", [Message]),
  MessageStr = erlang:iolist_to_binary(io_lib:format("~p", [Message])),
  {error_response, {unknown, <<"Unhandled message ", MessageStr/binary>>}}.

decode_clock(undefined) ->
  ignore;
decode_clock(BinClock) ->
  binary_to_term(BinClock).

encode_clock(Clock) ->
  term_to_binary(Clock).
