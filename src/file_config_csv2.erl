%% @author     Dmitry Kolesnikov, <dmkolesnikov@gmail.com>
%% @copyright  (c) 2012 Dmitry Kolesnikov. All Rights Reserved
%%
%%    Licensed under the 3-clause BSD License (the "License");
%%    you may not use this file except in compliance with the License.
%%    You may obtain a copy of the License at
%%
%%         http://www.opensource.org/licenses/BSD-3-Clause
%%
%%    Unless required by applicable law or agreed to in writing, software
%%    distributed under the License is distributed on an "AS IS" BASIS,
%%    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%    See the License for the specific language governing permissions and
%%    limitations under the License
%%
%% @description
%%    The simple CSV-file parser based on event model. The parser generates an
%%    event/callback when the CSV line is parsed. The parser supports both
%%    sequential and parallel parsing.
%%
%%                           Acc
%%                       +--------+
%%                       |        |
%%                       V        |
%%                  +---------+   |
%%    ----Input---->| Parser  |--------> AccN
%%          +       +---------+
%%         Acc0          |
%%                       V
%%                   Event Line 
%%
%%    The parser takes as input binary stream, event handler function and
%%    initial state/accumulator. Event function is evaluated agains current 
%%    accumulator and parsed line of csv-file. Note: The accumaltor allows to
%%    carry-on application specific state throught event functions.
%%
-module(file_config_csv2).
-author("Dmitry Kolesnikov <dmkolesnikov@gmail.com>").
-export([parse/3, split/4, pparse/4]).

%%
%%
%-define(QUOTE,     $").  % Field quote character
-define(QUOTE,     0).  % Field quote character
-define(FIELD_BY,  $\t). % Field separator
-define(LINE_BY,   $\n). % Line separator

%%
%% parse(In, Fun, Acc0) -> Acc
%%   In  = binary(), input csv data to parse
%%   Fun = fun({line, Line}, Acc0) -> Acc, 
%%      Line  - list() list of parsed fields in reverse order
%%   Acc0 = term() application specific state/term carried throught
%%                 parser event hadlers
%%
%% @doc sequentially parses csv file
%%
-spec parse(In, Fun, Acc0) -> term() when
      In :: binary(), 
      FunArg :: {'line', list()} | {'shard', binary()} | 'eof',
      Fun :: fun((FunArg, term()) -> term()),
      Acc0 :: term(). 
parse(In, Fun, Acc0) ->
   parse(In, 0, 0, [], Fun, Acc0).

-spec parse(In, Pos, Len, Line, Fun, Acc0) -> term() when
      In :: binary(),
      Pos :: non_neg_integer(),
      Len :: non_neg_integer(),
      Line :: list(),
      FunArg :: {'line', list()} | {'shard', binary()} | 'eof',
      Fun :: fun((FunArg, term()) -> term()),
      Acc0 :: term().
parse(In, Pos, Len, Line, Fun, Acc0) when Pos + Len < size(In) ->
   case In of
      <<_:Pos/binary, _Tkn:Len/binary, ?QUOTE, _/binary>> ->
         % start field
         parse_quoted(In, Pos + Len + 1, 0, Line, Fun, Acc0);
      <<_:Pos/binary, Tkn:Len/binary, ?FIELD_BY,  _/binary>> ->
         % field match
         parse(In, Pos + Len + 1, 0, [Tkn | Line], Fun, Acc0);
      <<_:Pos/binary, Tkn:Len/binary, ?LINE_BY>> ->
         % last line match
         Fun(eof, Fun({line, [Tkn | Line]}, Acc0));
      <<_:Pos/binary, Tkn:Len/binary, ?LINE_BY, _/binary>>  ->
         % line match
         parse(In, Pos + Len + 1, 0, [], 
               Fun, Fun({line, [Tkn | Line]}, Acc0));
      _ ->
         % no match increase token
         parse(In, Pos, Len + 1, Line, Fun, Acc0)
   end;
parse(In, Pos, Len, Line, Fun, Acc0) ->
   <<_:Pos/binary, Tkn:Len/binary, _/binary>> = In,
   Fun(eof, Fun({line, [Tkn | Line]}, Acc0)).
  
-spec parse_quoted(In, Pos, Len, Line, Fun, Acc0) -> term() when
      In :: binary(),
      Pos :: non_neg_integer(),
      Len :: non_neg_integer(),
      Line :: list(),
      FunArg :: {'line', list()} | {'shard', binary()} | 'eof',
      Fun :: fun((FunArg, term()) -> term()),
      Acc0 :: term.
parse_quoted(In, Pos, Len, Line, Fun, Acc0) ->
   case In of
      <<_:Pos/binary, _Tkn:Len/binary, ?QUOTE, ?QUOTE, _/binary>> ->
         parse_quoted(In, Pos, Len + 2, Line, Fun, Acc0);
      <<_:Pos/binary, Tkn:Len/binary, ?QUOTE, ?FIELD_BY, _/binary>> ->
         % field match
         parse(In, Pos + Len + 2, 0, [unescape(Tkn) | Line], Fun, Acc0);
      <<_:Pos/binary, Tkn:Len/binary, ?QUOTE, ?LINE_BY, _/binary>> ->
         % field match
         parse(In, Pos + Len + 2, 0, [], Fun, 
               Fun({line, [unescape(Tkn) | Line]}, Acc0));   
      _ ->   
         parse_quoted(In, Pos, Len + 1, Line, Fun, Acc0)
   end.   
         
%%
%% @doc unescape
-spec unescape(In) -> binary() when
      In :: binary().
unescape(In) ->
   unescape(In, 0, 0, <<>>).

-spec unescape(In, I, Len, Acc) -> binary() when
      In :: binary(),
      I :: non_neg_integer(),
      Len :: non_neg_integer(),
      Acc :: binary().
unescape(In, I, Len, Acc) when I + Len < size(In) ->
   case In of
      <<_:I/binary, Tkn:Len/binary, ?QUOTE, ?QUOTE, _/binary>> ->
         unescape(In, I + Len + 2, 0, <<Acc/binary, Tkn/binary, ?QUOTE>>);
      _ ->
         unescape(In, I, Len + 1, Acc)
   end;
unescape(In, I, Len, Acc) ->
   <<_:I/binary, Tkn:Len/binary>> = In,
   <<Acc/binary, Tkn/binary>>.      
   
%%
%% split(In, Count, Fun, Acc0) -> Acc0
%%    In    = binary(), input csv data to split
%%    Count = integer(), number of shard to produce
%%    Fun = fun({shard, Shard}, Acc0) -> Acc, 
%%       Shard  - binary() chunk of csv data
%%    Acc0 = term() application specific state/term carried throught
%%                  parser event hadlers
%%
%% @doc split input csv data into chunks
%%
-spec split(In, Count, Fun, Acc0) -> term() when
      In :: binary(),                   % Input csv data to split
      Count :: non_neg_integer(),       % Number of shards to produce
      Fun :: fun(({shard, binary()}, term()) -> term()), % Event handler
      Acc0 :: term().                   % Application specific state/term carried through parser event handlers
split(In, Count, Fun, Acc0) ->
   Size = erlang:round(size(In) / Count), % approximate a shard size
   split(In, 0, Size, Size, Fun, Acc0).
 
-spec split(In, Pos, Size, Size0, Fun, Acc0) -> term() when
      In :: binary(),               % Input csv data
      Pos :: non_neg_integer(),     % Starting position in input
      Size :: non_neg_integer(),    % Starting offset for end of shard
      Size0 :: non_neg_integer(),   % Shard size
      Fun :: fun(({shard, binary()}, term()) -> term()),
      Acc0 :: term().
split(In, Pos, Size, Size0, Fun, Acc0) when Pos + Size < size(In) ->
   case In of
      <<_:Pos/binary, Shard:Size/binary, ?LINE_BY>> ->
         Fun({shard, Shard}, Acc0);
      <<_:Pos/binary, Shard:Size/binary, ?LINE_BY, _/binary>> ->
         split(In, Pos + Size + 1, Size0,    Size0, Fun, 
            Fun({shard, Shard}, Acc0)
         );
      _ ->
         split(In, Pos, Size + 1, Size0, Fun, Acc0)
   end;
split(In, Pos, _Size, _Size0, Fun, Acc0) ->
   <<_:Pos/binary, Shard/binary>> = In,
   Fun({shard, Shard}, Acc0).

%%
%% pparse(In, Count, Fun, App) -> NApp
%%   In    = binary(), input csv data to parse
%%   Count = integers(), number of worker processes
%%   Fun   = fun({line, Line}, Acc0) -> Acc, 
%%      Line  - list() list of parsed fields in reverse order
%%   Acc0 = term() application specific state/term carried throught
%%                 parser event hadlers
%%
%% @doc parallel parse csv file. 
%%
%% Shards the input csv data and parses each chunk in own process.
%%
-spec pparse(In, Count, Fun, Acc0) -> term() when
      In :: binary(),
      Count :: non_neg_integer(),
      FunArg :: {'line', list()} | {'shard', binary()} | 'eof',
      Fun :: fun((FunArg, term()) -> term()),
      Acc0 :: term().
pparse(In, Count, Fun, Acc0) ->   
   Wrk = fun({shard, Shard}, Id) ->
      Pid = self(),
      spawn(
         fun() ->
            R = parse(Shard, Fun, Fun({shard, Shard}, Acc0)),
            Pid ! {shard, Id, R}
         end
      ),
      Id + 1
   end,
   N = split(In, Count, Wrk, 1),
   join(lists:seq(1, N - 1), []).

   
-spec join(list(), term()) -> term().
join([H | T], Acc) ->
   receive 
      {shard, H, R} when is_list(R) -> join(T, Acc ++ R);
      {shard, H, R} -> join(T, [R|Acc])
   end;
join([], Acc) ->
   Acc.
