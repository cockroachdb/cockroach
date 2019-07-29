UPDATE abc SET b = old.b + 1, c = old.c + 1 FROM abc AS old WHERE abc.a=2 and abc.b=abc.c ORDER BY abc.v DESC LIMIT 1 RETURNING abc.b, c, 4 AS d
