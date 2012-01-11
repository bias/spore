-module(sky).

% database loop structure
-include("db.hrl").

% client
-export([start_db/0, req/5, start_ind/1, view/1, root/2]).
% backend
-export([loop/1, req_loop/6, ind_loop/1]).

% TODO add distribution of data
% TODO broadcast sync, caching, distributed routing
% TODO data movement
% TODO file index on network, pass around blocks not files

%
% DB LOOP

% spawn index request process
start_db() ->
	spawn_link(?MODULE, loop, []).

loop(S) ->
	receive 

		% Reply to block request for data
		{request, R} ->
			Ttl = S#s.ttl,
			case length(R#r.path) of
				Ttl -> loop(S);
				_A -> broadcast_handle_block(S,R)
			end;

		% Received data
		{data, (R=#r{path=[H|T]})} -> 
			H ! {data, R#r{path=T}},
			loop(S);

		% Data hand out
		{dist, D={Key, _Data}} ->
			% kick to random neighbor
			case lists:keyfind(Key, 2, S#s.db) of
				false -> 
					loop(S#s{db=[D|S#s.db]});
				_ -> 
					lists:nth(random:uniform(length(S#s.nbh)), S#s.nbh) ! {dist, D},
					loop(S)
			end;

		% DB rpc modification
		{nbh, rpc, PidL} -> 
			loop(S#s{ nbh = lists:umerge(lists:delete(self(),PidL), S#s.nbh) });

		{log, rpc, nbh, Pid} ->
			Pid ! {log, nbh, self(), S#s.nbh},
			loop(S);

		{log, rpc, rc, Pid} ->
			Pid ! {log, rc, self(), S#s.rc},
			loop(S);

		stop -> ok

	end.

%
% DB Service API 

broadcast_handle_block(S, (R=#r{key=Key, time=Time, id=Id})) ->

	case L1=lists:filter(fun(E) -> case E of #r{key=Key} -> true; _ -> false end end, S#s.qu) of
		% new request, fully process
		[] -> 
			dispatch(S, R),
			loop(S#s{qu=[R|S#s.qu], rc=S#s.rc+1});
		_ -> ok
	end,

	case L2=lists:filter(fun(E) -> case E of #r{time=Time} -> true; _ -> false end end, L1) of
		% repeat request, fully process
		[] -> 
			dispatch(S, R),
			loop(S#s{qu=[R|S#s.qu], rc=S#s.rc+1});
		_ -> ok
	end,

	case lists:filter(fun(E) -> case E of #r{id=Id} -> true; _ -> false end end, L2) of
		% incomplete request (new id), forward
		[] -> 
			broadcast(S, R),
			loop(S#s{qu=[R|S#s.qu], rc=S#s.rc+1});
		% already received, ignore
		[_] ->
			loop(S#s{rc=S#s.rc+1})
	end.

dispatch(S, (R=#r{key=Key, path=[H|T]})) ->
	case lists:keyfind(Key, 1, S#s.db) of
		{Key, Data} -> 
			H ! {data, R#r{path=T, data=Data}};
		false -> 
			broadcast(S, R)
	end.

broadcast(S, R) ->
	[ request(N, R) || N <- S#s.nbh ].

request(Pid, R) ->
	Pid ! {request, R#r{path=[self()|R#r.path]}}.


%
% INDEX LOOP 

start_ind(DB) ->
	spawn_link(?MODULE, ind_loop, [#i{db=DB}]).

% FIXME starting with flat index, single root
ind_loop( (I=#i{root=Root}) ) -> 
	receive

		{root, NewRoot} -> 
			req(NewRoot, 1, 5, 100, I#i.db),
			ind_loop(I#i{root=NewRoot});

		{data, #r{key=Root, data=Data}} -> 
			ind_loop(I#i{i={Root,Data}});

		{view, Pid} ->
			Pid ! {index, I#i.i},
			ind_loop(I)

	end.

root(Ind, Root) -> 
	Ind ! {root, Root}.

view(Ind) -> 
	Ind ! {view, self()}.

%
% Client API - uses rpc messages

% spawn block request process
% FIXME stubbing erasure coding 
req(Key, N, T, Freq, DB) ->
	spawn_link(?MODULE, req_loop, [N, T, Freq, self(), DB, #r{key=Key, time=now(), id=0, path=[]}]).

req_loop(0, _, _, Req, _, _) -> 
	Req ! {data, complete};
req_loop(_, 0, _, Req, _, _) -> 
	Req ! {data, fail};
req_loop(N, T, Freq, Req, DB, R) ->
	receive
		{data, _NR} -> 
			% FIXME should assemble data rather than passing it
			%Req ! {data, NR},
			req_loop(N-1, T, Freq, Req, DB, R)
		after Freq ->
			request(DB, R),
			req_loop(N, T-1, Freq, Req, DB, R#r{id=R#r.id+1})
	end.
