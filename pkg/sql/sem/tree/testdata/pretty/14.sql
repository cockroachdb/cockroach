UPDATE abc SET (b, c) = (8, 9) WHERE a=2 and b=c ORDER BY v DESC LIMIT 1 RETURNING abc.b, c, 4 AS d
