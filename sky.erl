-module(sky).

% database loop structure
-include("db.hrl").

% client
-export([req/3]).
% backend
-export([loop/1, req_loop/4]).
% testing
-export([test_y/0]).

% TODO add distribution of data
% TODO broadcast sync, caching, distributed routing
% TODO data movement

%
% Main DB loop
loop(S) ->
	receive 

		% Reply to request for data
		{request, R} ->
			handle(S,R);

		% Received data
		{data, R} -> 
			% XXX strange record pattern matching rules
			[H|T] = R#r.path,
			H ! {data, R#r{path=T}},
			loop(S);

		% DB rpc modification
		{nbh, rpc, Pid} -> 
			loop(S#s{nbh=[Pid|S#s.nbh]});

		stop -> ok

	end.

handle(S, R) ->
	% XXX strange record pattern matching rules
	File=R#r.file, Time=R#r.time, Id=R#r.id,

	% check for previous request 
	% XXX using tuple ordering of record 
	case lists:keyfind(File, 2, S#s.qu) of

		% already received, ignore
		#r{file=File, time=Time, id=Id} ->
			io:format("~w duplicate, ignore~n", [self()]),
			loop(S);

		% incomplete request (new id), forward
		#r{file=File, time=Time} ->
			io:format("~w renewed,   forward~n", [self()]),
			broadcast(S, R),
			loop(S#s{qu=[R|S#s.qu]});

		% repeat request, fully process
		#r{file=File} -> 
			io:format("~w repeat,    process~n", [self()]),
			dispatch(S, R),
			loop(S#s{qu=[R|S#s.qu]});

		% new request, fully process
		false -> 
			io:format("~w new,       process~n", [self()]),
			dispatch(S, R),
			loop(S#s{qu=[R|S#s.qu]})
	end.

dispatch(S, R) ->
	% XXX strange record pattern matching rules
	File = R#r.file,
	case lists:keyfind(File, 1, S#s.db) of
		{File, Data} -> 
			[H|T] = R#r.path,
			H ! {data, R#r{path=T, data=Data}};
		false -> 
			broadcast(S, R)
	end.


%
% Service API 
broadcast(S, R) ->
	[ request(N, R) || N <- S#s.nbh ].

request(Pid, R) ->
	Pid ! {request, R#r{path=[self()|R#r.path]}}.


%
% Client API - uses rpc messages

% spawn request process

% FIXME kill request after a while?
req(File, N, DB) ->
	spawn_link(?MODULE, req_loop, [N, self(), DB, #r{file=File, time=time(), id=0, path=[]}]).

req_loop(0, _, _, _) -> ok;
req_loop(N, Rec, DB, R) ->
	% XXX strange record pattern matching rules
	File=R#r.file,
	receive
		{data, NR} -> 
			Rec ! {data, NR},
			req_loop(N-1, Rec, DB, R)
		after 100 ->
			request(DB, R),
			req_loop(N, Rec, DB, R#r{id=R#r.id+1})
	end.


%
%  Testing
test_y() ->
	DB = spawn_link(?MODULE, loop, [#s{user=self(), db=[{crud,0},{junk,0}]}]), 
	register(mydb, DB),
	N1 = spawn_link(?MODULE, loop, [#s{nbh=[DB], db=[{crud,1}]}]),
	N11 = spawn_link(?MODULE, loop, [#s{nbh=[N1], db=[{crud,2},{junk,1}]}]),
	N12 = spawn_link(?MODULE, loop, [#s{nbh=[N1], db=[{crud,3}]}]),
	N2 = spawn_link(?MODULE, loop, [#s{nbh=[DB], db=[{crud,4}]}]),
	N21 = spawn_link(?MODULE, loop, [#s{nbh=[N2], db=[{crud,5},{junk,2}]}]),
	N22 = spawn_link(?MODULE, loop, [#s{nbh=[N2], db=[{crud,6}]}]),
	DB ! {nbh, rpc, N1},
	DB ! {nbh, rpc, N2},
	N1 ! {nbh, rpc, N11},
	N1 ! {nbh, rpc, N12},
	N2 ! {nbh, rpc, N21},
	N2 ! {nbh, rpc, N22},
	DB.
