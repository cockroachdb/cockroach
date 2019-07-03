select
	min(a,b),
	min(a order by b),
	min(a order by b, c),
	min(a order by b, c, d),
	min(distinct a,b),
	min(),
	min() over (),
	min() over (order by x),
	min() filter (where x >3 and y < 4),
	min() filter (where x >3 and y < 4) over (order by x),
	min() over (range between unbounded preceding and unbounded following),
	min() over (rows between 1 following and 1 following),
	min() over (w partition by a,b order by x,y rows between 1 following and 1 following)
