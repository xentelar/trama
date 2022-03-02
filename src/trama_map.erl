-module(trama_map).

-include_lib("kernel/include/logger.hrl").

-export([new/0]).
-export([update_register/3]).
-export([update_counter/3]).
-export([update_flag/3]).
-export([update_list/3]).

-export([map_to_rkmap/2]).
-export([rkmap_to_map/2]).

new() ->
    riakc_map:new().


update_register(K, V, RKMap) ->
   add_element(K, V, RKMap, {register}).

update_counter(K, V, RKMap) ->
   add_element(K, V, RKMap, {counter}).

update_flag(K, V, RKMap) ->
   add_element(K, V, RKMap, {flag}).

update_list(K, V, RKMap) ->
   add_element(K, V, RKMap, {}).

add_element(K, V, RKMap, {register}) when is_binary(K), is_binary(V) ->
   ?LOG_DEBUG(#{
      msg => <<"simple key, register value">>,
      key => K,
      value => V
   }),
   riakc_map:update(
                     {K, register},
                     fun(R) -> riakc_register:set(V, R) end,
                     RKMap
                  );

add_element(K, V, RKMap, {counter}) when is_binary(K), is_atom(V) ->
   ?LOG_DEBUG(#{
      msg => <<"simple key, counter value">>,
      key => K,
      value => V
   }),
   case V of
      increment ->
         Func = fun(C) -> riakc_counter:increment(1, C) end;
      decrement ->
         Func = fun(C) -> riakc_counter:decrement(1, C) end
   end,
   riakc_map:update( {K, counter}, Func, RKMap );

add_element(K, V, RKMap, {flag}) when is_binary(K), is_atom(V) ->
   ?LOG_DEBUG(#{
      msg => <<"simple key, flag value">>,
      key => K,
      value => V
   }),
   case V of
      true ->
         Func = fun(F) -> riakc_flag:enable(F) end;
      false ->
         Func = fun(F) -> riakc_flag:disable(F) end
   end,
   riakc_map:update( {K, flag}, Func, RKMap);

add_element(K, V, RKMap, _Opts) when is_binary(K), is_list(V) ->
   ?LOG_DEBUG(#{
      msg => <<"simple key, list value">>,
      key => K,
      value => V
   }),
   list_to_rk_map(K, V, RKMap);

add_element([H|T]=K, V, RKMap, Opts) when is_list(K), length(K)>1 ->
   ?LOG_DEBUG(#{
      msg => <<"full key">>,
      key => K,
      value => V
   }),
   riakc_map:update(
                     {H, map},
                     fun(R) -> add_element(T, V, R, Opts) end,
                     RKMap
                  );

add_element([H]=K, V, RKMap, Opts) when is_list(K), length(K)==1 ->
   ?LOG_DEBUG(#{
      msg => <<"simple key">>,
      key => H,
      value => V
   }),
   add_element(H, V, RKMap, Opts).


-spec rkmap_to_map(riakc_map:crdt_map(), map()) -> map().
rkmap_to_map(RKMap, Opts) ->
   M = to_map(riakc_map:value(RKMap), Opts),
   {{map, M}, {rkmap, RKMap}}.

to_map(Tuplas, Opts) ->
   Func = fun 
            ({{K, map}, V}, Acc) ->
               Def = maps:get(K, Opts, undefined),
               Type = get_type(Def, Opts),
               NewV = to_map(V, Type),
               maps:put(K, NewV, Acc);
            ({{K, register}, V}, Acc) ->
               convert(K, V, Acc, Opts);
            ({{K, counter}, V}, Acc) ->
               convert(K, V, Acc, Opts);
            ({{K, flag}, V}, Acc) ->
               convert(K, V, Acc, Opts);
            ({{K, set}, V}, Acc) ->
               convert(K, V, Acc, Opts)
         end,
   lists:foldl(Func, #{}, Tuplas).

get_type(undefined, Opts) ->
   Opts;
get_type(Def, _) ->
   maps:get(type, Def).
   
%%-spec map_to_rkmap(map()) -> riakc_map:crdt_map().
map_to_rkmap(Map, RKMap) ->
   maps:fold(fun update_rk_map/3, RKMap, Map).

update_rk_map(K, V, RKMap) when is_map(V) ->
   riakc_map:update(
                     {K, map},
                     fun(M) -> update_rk_map(K, V, M) end,
                     RKMap
                  );

update_rk_map(K, V, RKMap) when is_list(V) ->
   list_to_rk_map(K, V, RKMap);

update_rk_map(K, V, RKMap) ->
   riakc_map:update(
                     {K, register},
                     fun(R) -> riakc_register:set(V, R) end,
                     RKMap
                  ).


list_to_rk_map(K, [H|T], RKMap0) ->
   ?LOG_DEBUG(#{
      msg => <<"simple key, list value">>,
      key => K,
      value => H
   }),
   RKMap1 = riakc_map:update(
                           {K, set},
                           fun(S) -> riakc_set:add_element(H, S) end, 
                           RKMap0
                        ),
   list_to_rk_map(K, T, RKMap1);

list_to_rk_map(K, [], RKMap) ->
   ?LOG_DEBUG(#{
      msg => <<"simple key, end list">>,
      key => K
   }),
   RKMap.


convert(K, V, Map, Opts) ->
   Def = maps:get(K, Opts),
   Type = maps:get(type, Def),
   ?LOG_DEBUG(#{
      msg => <<"convert value to">>,
      type => Type,
      key => K,
      value => V
   }),
   case Type of
      int ->
         maps:put(K, binary_to_integer(V), Map);
      float ->
         maps:put(K, binary_to_float(V), Map);
      bool ->
         maps:put(K, V, Map);
      binary ->
         maps:put(K, V, Map);
      counter ->
         maps:put(K, V, Map);
      {list, int} ->
         maps:put(K, to_int_list(V, []), Map);
      {list, float} ->
         maps:put(K, to_float_list(V, []), Map);
      {list, bool} ->
         maps:put(K, V, Map);
      {list, binary} ->
         maps:put(K, V, Map)
   end.

to_int_list([H|T], Acc) ->
   A = binary_to_integer(H),
   to_int_list(T, [A|Acc]);
to_int_list([], Acc) ->
   Acc.

to_float_list([H|T], Acc) ->
   A = binary_to_float(H),
   to_float_list(T, [A|Acc]);
to_float_list([], Acc) ->
   Acc.