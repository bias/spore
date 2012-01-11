% db loop state
-record(s, {db = [], nbh = [], qu = [], rc=0, ttl}).

% index loop state
-record(i, {i = [], db, root=null}).

% request state
-record(r, {key, time, id, path, data}).
