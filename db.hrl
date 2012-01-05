% loop state
-record(s, {db, user, nbh = [], qu = [], rc}).

% request state
-record(r, {file, time, id, path, data}).

% queue state
-record(q, {file, time, id}).
