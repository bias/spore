-module(sky).

% database loop structure
-include("db.hrl").

% client
-export([req/3]).
% backend
-export([loop/1, req_loop/6]).
% testing
-export([test_y/0]).

%
% Main DB loop

loop(S) ->

	receive 

		% Reply to request for data
		%{request, Path=[H|T], File, Time, Id} -> 
		{request, #r{path=Path, }}
			% check for previous request
			case lists:keyfind(File, 1, S#s.qu) of
				% already received, ignore
				{File, Time, Id} -> 
					loop(S);
				% incomplete request, forward
				{File, Time, _OldId} -> 
					broadcast(S, Path, File, Time, Id),
					loop(S#s{qu=[{File,Time,Id}|S#s.qu]});
				% repeat request, fully process
				{File, _OldTime, _} -> 
					case lists:keyfind(File, 1, S#s.db) of
						{File, Data} -> 
							H ! {data, T, File, Data, Time};
						false -> 
							broadcast(S, Path, File, Time, Id)
					end,
					loop(S#s{qu=[{File,Time,Id}|S#s.qu]});
				% new request, fully process
				false -> 
					case lists:keyfind(File, 1, S#s.db) of
						{File, Data} -> 
							H ! {data, T, File, Data, Time};
						false -> 
							broadcast(S, Path, File, Time, Id)
					end,
					loop(S#s{qu=[{File,Time,Id}|S#s.qu]})
			end;

		% Received data
		{data, [H|T]=_Path, File, Data, Time} -> 
			% sending storage pid as unique id for file piece
			H ! {data, T, File, Data, Time},
			loop(S);

		% DB rpc modification
		{nbh, rpc, Pid} -> 
			loop(S#s{nbh=[Pid|S#s.nbh]});

		stop -> ok
			
	end.

%
% Client API - uses rpc messages

% spawn request process

req(File, N, DB) ->
	spawn_link(?MODULE, req_loop, [File, N, time(), self(), DB, 0]).

% XXX kill request after a while
req_loop(_, 0, _, _, _, _) -> ok;
req_loop(File, N, Time, Rec, DB, Id) ->
	receive
		{data, _, File, Data, Time} -> 
			Rec ! {File, Data},
			req_loop(File, N-1, Time, Rec, DB, Id)
		after 100 ->
			request(DB, [], File, Time, Id),
			req_loop(File, N, Time, Rec, DB, Id+1)
	end.


%
% Service API 
broadcast(S, Path, File, Time, Id) ->
	[ request(N, Path, File, Time, Id) || N <- S#s.nbh ].

request(Pid, Path, File, Time, Id) ->
	Pid ! {request, [self()|Path], File, Time, Id}.


%
%  Test networks
test_y() ->
	DB = spawn_link(?MODULE, loop, [#s{user=self(), db=[{crud,0}]}]), 
	register(mydb, DB),
	N1 = spawn_link(?MODULE, loop, [#s{nbh=[DB], db=[{crud,1}]}]),
	N11 = spawn_link(?MODULE, loop, [#s{nbh=[N1], db=[{crud,2}]}]),
	N12 = spawn_link(?MODULE, loop, [#s{nbh=[N1], db=[{crud,3}]}]),
	N2 = spawn_link(?MODULE, loop, [#s{nbh=[DB], db=[{crud,4}]}]),
	N21 = spawn_link(?MODULE, loop, [#s{nbh=[N2], db=[{crud,5}]}]),
	N22 = spawn_link(?MODULE, loop, [#s{nbh=[N2], db=[{crud,6}]}]),
	DB ! {nbh, rpc, N1},
	DB ! {nbh, rpc, N2},
	N1 ! {nbh, rpc, N11},
	N1 ! {nbh, rpc, N12},
	N2 ! {nbh, rpc, N21},
	N2 ! {nbh, rpc, N22},
	DB.
