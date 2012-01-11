-module(sim).

% database loop structure
-include("db.hrl").

% testing
-export([test_u/0, test_u/1, user/7, logger/1, log/0]).

%
% programatic databases

% XXX first create uniform connection and distribution
test_u() ->
	test_u([3, 1, 3, 1, 1, 300, 1]).

test_u([N,S,F,K,Kp,Ms,T]) -> 
	uniform_net(N,S,F,K,Kp,Ms,T).

% network size N, neighbor size upto S, data size F, symbol size up to K
uniform_net(N,S,F,K,Kp,Ms,T) ->
	% Time based seed
	{A1, A2, A3} = now(),
	random:seed(A1, A2, A3),
	% create N many vanilla databases
	PidL = [ spawn_link(sky, loop, [#s{ttl=N}]) || _Num <- lists:seq(1,N) ],
	% XXX neighbor backbone to ensure connected graph
	[ connect(lists:nth(Nth,PidL), lists:nth(Nth+1,PidL)) || Nth <- lists:seq(1,N-1) ],
	% give each db neighbors
	[ uniform_nbh(N, S, Pid, PidL) || Pid <- PidL ],
	% hand out data
	[ uniform_data(N, Num, K, PidL) || Num <- lists:seq(1,F) ],
	% give each db a user
	UserL = [ spawn_link(sim, user, [Pid, F, Kp, Ms, T, 0, 0]) || Pid <- PidL ], 	
	{PidL,UserL}.

uniform_nbh(N, S, Pid, PidL) -> 
	[ connect(Pid, lists:nth(random:uniform(N), PidL)) || _Num <- lists:seq(1,random:uniform(S)) ],
	ok.

uniform_data(N, Num, K, PidL) ->
	[ lists:nth(random:uniform(N), PidL) ! {dist, {Num,SymbolNum}} || SymbolNum <- lists:seq(1, K) ],
	ok.

connect(Pid1, Pid2) ->
	Pid1 ! {nbh, rpc, [Pid2]},
	Pid2 ! {nbh, rpc, [Pid1]},
	ok.

user(_Pid, _F, _Kp, _Ms, 0, Rc, Dc) ->
	io:format("~w done with Rc ~w Dc ~w ~n", [self(), Rc, Dc]);
user(Pid, F, Kp, Ms, T, Rc, Dc) ->
	receive
		{data, complete} ->
			user(Pid, F, Kp, Ms, T-1, Rc, Dc+1);
		{data, fail} ->
			io:format("~w failed to get file~n", [self()]),
			user(Pid, F, Kp, Ms, T-1, Rc, Dc);
		{q, Pid} ->
			Pid ! {Rc, Dc}
		after Ms ->
			{A1, A2, A3} = now(),
			random:seed(A1, A2, A3),
			File = random:uniform(F),
			io:format("~w asking for ~w~n", [self(), File]),
			sky:req(File, Kp, 5, 100, Pid),
			user(Pid, F, Kp, Ms, T, Rc+1, Dc)
	end.

logger(DBL) ->
	Pid = spawn_link(?MODULE, log, []),
	%log(DBL, nbh, Pid),
	log(DBL, rc, Pid).

log(DBL, nbh, Pid) ->
	[ DB ! {log, rpc, nbh, Pid}  || DB <- DBL ];
log(DBL, rc, Pid) ->
	[ DB ! {log, rpc, rc, Pid}  || DB <- DBL ].

log() ->
	receive
		{log, nbh, Pid, Nbh} ->
			[ io:format("\"~w\" -> \"~w\",~n", [Pid, N]) || N <- Nbh ],
			log();
		{log, rc, Pid, Rc} ->
			io:format("\"~w\", ~w~n", [Pid, Rc]),
			log()
	end.
