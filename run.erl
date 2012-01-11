#!/usr/bin/env escript
%%! -smp enable debug verbose

-import(sky).
-import(sim).

main(_) ->
	{DBL, UL} = sim:test_u(),
	timer:sleep(1000),
	{ok, File} = file:open("data/test", [write]),
	sim:logger(DBL, File).

trial(N,K,Y0) ->
	Hs = integer_to_list(H),
	Bs = integer_to_list(B),
	PID = integrate:start("hw3.data/b"++Bs++"_"++Hs++".csv"),
	integrate:bash(H*10, 0, 1/H, Y0, fun(_X,Y) -> -Y*Y end, PID)

	{DBL, UL} = sim:test_u(),
	timer:sleep(1000),
	{ok, File} = file:open("data/test", [write]),
	sim:logger(DBL, File).
