CREATE TABLE public.a (
	a INT8 NOT NULL,
	b INT8 NULL,
	CONSTRAINT "primary" PRIMARY KEY (a ASC),
	FAMILY "primary" (a, b)
);
