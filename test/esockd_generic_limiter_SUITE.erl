%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(esockd_generic_limiter_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> esockd_ct:all(?MODULE).

%%--------------------------------------------------------------------
%% Test cases for limiter
%%--------------------------------------------------------------------

t_create(_) ->
    {ok, _} = esockd_limiter:start_link(),
    Limiter = esockd_generic_limiter:create(#{module => esockd_limiter,
                                              name => bucket1,
                                              capacity => 10,
                                              interval => 1}),
    ?assertMatch(#{module := _, name := _}, Limiter),

    #{name     := bucket1,
      capacity := 10,
      interval := 1,
      tokens   := 10
     } = esockd_limiter:lookup(bucket1),
    ok = esockd_limiter:stop().

t_consume(_) ->
    {ok, _} = esockd_limiter:start_link(),
    Limiter = esockd_generic_limiter:create(#{module => esockd_limiter,
                                              name => bucket,
                                              capacity => 10,
                                              interval => 2}),
    #{name     := bucket,
      capacity := 10,
      interval := 2,
      tokens   := 10
     } = esockd_limiter:lookup(bucket),

    {ok, Limiter2} = esockd_generic_limiter:consume(1, Limiter),

    #{tokens := 9} = esockd_limiter:lookup(bucket),

    {ok, Limiter3} = esockd_generic_limiter:consume(4, Limiter2),
    #{tokens := 5} = esockd_limiter:lookup(bucket),

    {pause, P1, Limiter4} = esockd_limiter:consume(5, Limiter3),
    #{tokens := 0} = esockd_limiter:lookup(bucket),
    %% tokens exhausted, need pause to next interval
    ?assertEqual(1900 =< P1 andalso P1 =< 2000 , true),

    {pause, P2, Limiter5} = esockd_limiter:consume(6, Limiter4),
    #{tokens := -6} = esockd_limiter:lookup(bucket),
    %% borrowed tokens from next interval, but not exhausted next
    %% pause to next is enough
    ?assertEqual(1900 =< P2 andalso P2 =< 2000, true),

    {pause, P3, Limiter6} = esockd_limiter:consume(6, Limiter5),
    #{tokens := -12} = esockd_limiter:lookup(bucket),
    %% the tokens in next interval is exhausted,
    %% need pause to next next interval
    ?assertEqual(3900 =< P3 andalso P3 =< 4000, true),

    %% after 1 interval, generate 10 tokens but still not enough
    timer:sleep(2300),
    #{tokens := -2} = esockd_limiter:lookup(bucket),

    %% after 2 intervals, generate 20 tokens, now have 8 tokens
    timer:sleep(2300),
    {ok, _Limiter7} = esockd_limiter:consume(6, Limiter6),
    %% after consume, still have 2 tokens
    #{tokens := 2} = esockd_limiter:lookup(bucket),
    ok = esockd_limiter:stop().

t_undefined(_) ->
    {ok, undefined} = esockd_generic_limiter:consume(10, undefined),
    ok = esockd_generic_limiter:delete(undefined),
    ok.
