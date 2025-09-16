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

-module(esockd_rate_limit_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(MS, 1000).
-define(wrap(X), (X * 1000)).

all() -> esockd_ct:all(?MODULE).

t_info(_) ->
    Rl = esockd_rate_limit:new({1000, 10000}),
    Info = esockd_rate_limit:info(Rl),
    ?assertMatch(#{rate   := 1000,
                   burst  := 10000,
                   tokens := 10000
                  }, Info),
    ?assert(erlang:system_time(milli_seconds) >= maps:get(time, Info)).

%% for compatibility
%% Rates with a precision below 1000 may cause unexpected behavior
t_check_old(_) ->
    TokensPerSecond = 1,
    BurstTokens = 10,

    Rl = esockd_rate_limit:new({TokensPerSecond, BurstTokens}),
    #{tokens := BurstTokens} = esockd_rate_limit:info(Rl),

    %% less than burst, no pause, 5 tokens left
    {0, Rl1} = esockd_rate_limit:check(5, Rl),
    #{tokens := 5} = esockd_rate_limit:info(Rl1),

    %% consumed the rest 5 tokens,
    %% Caused Low-precision round problem, still need pause 1000ms
    %% P = 1/r = 1000ms
    {P0, Rl2} = esockd_rate_limit:check(5, Rl1),
    ?assertEqual(P0, 1000),
    #{tokens := 0} = esockd_rate_limit:info(Rl2),

    %% should paused 1000ms and do next check. but here we do not sleep
    %% So PauseTo will continue to delay for 1s
    %% LastPause = (Tokens-Limit)/r = 5000ms
    %% This PauseTo should be 6000ms
    {P1, Rl3} = esockd_rate_limit:check(5, Rl2),
    ?assertEqual(P1, 5000 + P0),
    #{tokens := 0} = esockd_rate_limit:info(Rl3),

    ok = timer:sleep(1000),
    %% next check after 1s sleep
    %% still 6000 - 1000 = 5000ms to wait for last pause
    %% This PauseTo should continue to increase by 2000ms
    {7000, _} = esockd_rate_limit:check(2, Rl3).

t_check(_) ->
    TokensPerSecond = ?wrap(1), %% 1 Token per millisecond
    BurstTokens = ?wrap(10),
    Rl = esockd_rate_limit:new({TokensPerSecond, BurstTokens}),

    %% ====================
    ?assertMatch(#{tokens := 10000}, esockd_rate_limit:info(Rl)),
    %% less than burst, no pause, 5000 tokens left
    {0, Rl1} = esockd_rate_limit:check(5000, Rl),
    ?assertMatch(#{tokens := 5000}, esockd_rate_limit:info(Rl1)),

    %% ====================
    ok = timer:sleep(1),
    {0, Rl2} = esockd_rate_limit:check(5000, Rl1),
    #{tokens := T0} = esockd_rate_limit:info(Rl2),
    %% Consumed the rest 5000 tokens, and few tokens generated during 1ms sleep
    %% So the remaining tokens should be a value greater than 0.
    %% Give an execution time deviation 2ms, we assert it between (0, 2].
    ?assert((T0 > 0) andalso (T0 =< 2)),

    %% ====================
    ok = timer:sleep(1),
    %% comuse the rest tokens (T0),
    %% and should pause for the missing token generation time
    %% P = (abs(T0 - ComusedTokens))/r
    {P1, Rl3} = esockd_rate_limit:check(5000, Rl2),
    #{tokens := T1} = esockd_rate_limit:info(Rl3),
    %% Give an execution time deviation 2ms, Real PauseTo = abs(T0 - 5000) - 2
    %% And no tokens left
    ?assert((5000 - T0 - 2 =< P1) andalso (P1 =< (5000 - T0))),
    ?assertEqual(0, T1),

    %% ====================
    Sleep = 1000,
    ok = timer:sleep(Sleep),
    %% P = (Tokens-Limit)/r = 1000ms
    {P2, _} = esockd_rate_limit:check(2000, Rl3),

    PauseTo = P1 - Sleep + 2000,
    %% allow +-2ms deviation
    ?assert(((PauseTo - 2) =< P2) andalso (P2 =< (PauseTo + 2))).

t_check_bignum(_) ->
    R1 = 30000000,
    R2 = 15000000,
    Rl = esockd_rate_limit:new({R1, R1}),
    #{tokens := R1} = esockd_rate_limit:info(Rl),
    {0, Rl1} = esockd_rate_limit:check(R2, Rl),
    #{tokens := R2} = esockd_rate_limit:info(Rl1),
    %% P = 1/r = 0.00003333 ~= 0ms
    {0, Rl2} = esockd_rate_limit:check(R2, Rl1),
    #{tokens := 0} = esockd_rate_limit:info(Rl2),

    timer:sleep(1000),
    %% P = (Tokens-Limit)/r = 1000ms
    {1000, Rl3} = esockd_rate_limit:check(R1*2, Rl2),
    #{tokens := 0} = esockd_rate_limit:info(Rl3),
    %% P = (Tokens-Limit)/r = 0.5ms ~= 1ms
    {1, Rl4} = esockd_rate_limit:check(R1*(1+0.0005), Rl2),
    #{tokens := 0} = esockd_rate_limit:info(Rl4).

