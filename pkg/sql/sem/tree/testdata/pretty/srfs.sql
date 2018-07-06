select * from
   generate_series(a), -- a simple scalar expression
   rows from(generate_series(b)), -- a rows from clause that can be elided
   rows from(generate_series(a), generate_series(b)) -- this one not
