select * from
  (a cross join b),
  (a cross join (c cross join d)),
  d,
  (table x),
  (table e order by f)
