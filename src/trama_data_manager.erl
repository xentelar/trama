-module(trama_data_manager).

-include_lib("kernel/include/logger.hrl").

-export([store/4]).
-export([delete/3]).
-export([find/3]).

-spec find({binary(), map()}, binary(), map()) -> {{map, map()}, {rkmap, map()}} | {error, list()}.
find(Resource, RKKey, Opts) ->

   Res = trama_pool_manager:execute(
                        fun(Conn) ->
                           riakc_pb_socket:fetch_type(Conn, Resource, RKKey)
                        end),
   case Res of
      {error, {notfound, map}} -> 
         ?LOG_ERROR(#{
            msg => <<"record not found">>,
            error => {notfound}
         }),
         {error, {map, notfound}};
      {error, Error} -> 
         ?LOG_ERROR(#{
            msg => <<"general error, check the status of riak kv">>,
            error => Error
         }),
         {error, {riakkv, general}};
      {ok, RKMap} ->
         ?LOG_DEBUG(#{
            msg => <<"record found">>,
            map => RKMap
         }),
         trama_map:rkmap_to_map(RKMap, Opts)
   end.

-spec store({binary(), map()}, binary(), map(), map()) -> ok | {error, list()}.
store(Resource, RKKey, RKMap, _Opts) when is_binary(RKKey) ->
   
   Operation = riakc_map:to_op(RKMap),
   
   Res = trama_pool_manager:execute(
                        fun(Conn) -> 
                           riakc_pb_socket:update_type(Conn, Resource, RKKey, Operation)
                        end),
   
   case Res of
      {error, Error} ->
         ?LOG_ERROR(#{
            msg => <<"general error, check the status of riak kv">>,
            error => Error
         }),
         {error, {riakkv, general}};
      ok ->
         ok;
      {ok, _Key_or_Data_Type} ->
         ok;
      {ok, _Key, _Data_Type} ->
         ok
   end.

-spec delete({binary(), map()}, binary(), map()) -> ok | {error, list()}.
delete(Resource, RKKey, _Opts) ->

   Res = trama_pool_manager:execute(fun(Conn) ->
                                       riakc_pb_socket:delete(Conn, Resource, RKKey)
                                   end),
   case Res of
      ok -> 
         ok;
      _ -> 
         {error, {riakkv, general}}
   end.
