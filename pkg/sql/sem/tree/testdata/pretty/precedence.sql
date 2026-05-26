select
  (a + b) - c, (a - b) + c,  -- verify associativity at top
  a + (b - c), a - (b + c),  -- ditto
  ((a + b) - c) + d, ((a - b) + c) + d, -- veriffy associativity in sub-expr
  (a + (b - c)) + d, (a - (b + c)) + d, -- ditto
  ((a + b) - c) - d, ((a - b) + c) - d, -- ditto
  (a + (b - c)) - d, (a - (b + c)) - d, -- ditto
  d + ((a + b) - c), d + ((a - b) + c), -- ditto
  d + (a + (b - c)), d + (a - (b + c)), -- ditto
  d - ((a + b) - c), d - ((a - b) + c), -- ditto
  d - (a + (b - c)), d - (a - (b + c)), -- ditto
  ((a1/a2)/(b/c))/((d/e)/f),            -- ditto
  (a+b)*(c*d),                 -- mixed precedence
  a+(b*c), (a+b)*c             -- ditto
