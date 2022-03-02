-module(trama_pool_manager).

-include_lib("kernel/include/logger.hrl").

-export([start/5]).
-export([stop/0]).
-export([checkout/0]).
-export([checkin/2]).

-export([execute/1]).

-export([check_connections/0]).

-spec start(binary(), integer(), integer(), integer(), integer()) -> pid() | {error, list()}.
start(Address, Port, Size, Max, QMax) ->
    pooler:start(),
    PoolConfig = [
                    {name, riak_pool},
                    {group, riak},
                    {max_count, Max},
                    {init_count, Size},
                    {queue_max, QMax},
                    {start_mfa,
                        {riakc_pb_socket, start_link, [Address, Port]}}
                ],
    pooler:new_pool(PoolConfig).

-spec stop() -> ok.
stop() ->
    pooler:rm_pool(riak_pool).

-spec checkout() -> pid() | error_no_members.
checkout() ->
    pooler:take_member(riak_pool, 100).

-spec checkin(pid(), ok | fail) -> ok.
checkin(Conn, Status) ->
    pooler:return_member(riak_pool, Conn, Status).

-spec execute(function()) -> list() | no_return().
execute(Fun) ->
    Conn = checkout(),
    case Conn of
        Conn when is_pid(Conn) ->
            try
                Res = Fun(Conn),
                checkin(Conn, ok),
                Res
            catch
                C:R:S ->
                    checkin(Conn, fail),
                    erlang:raise(C, R, S)
            end;
        error_no_members ->
            {pool_exausted, <<"there no more connections">>}
    end.

-spec check_connections() -> Res when Res :: [{ok, list()} | {error, list()}].
check_connections() ->
    pooler:call_free_members(riak_pool,
        fun(C) ->
            riakc_pb_socket:ping(C, 1000),
            ok
        end).
