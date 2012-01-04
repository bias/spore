% loop state
-record(s, 
	{
		db = [],
		user = null,
		nbh = [],
		qu = []
	}).

% request state
-record(r,
	{
		file = null,
		path = [],
		time = null,
		id = 0
	}).

% data state
-record(d,
	{
		file = null,
		data = null,
		path = [],
		time = null
	}).
