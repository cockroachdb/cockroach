CREATE TABLE t (
	i INT NOT NULL,
	t STRING NULL,
	CONSTRAINT "primary" PRIMARY KEY (i ASC),
	INDEX t_t_idx (t ASC),
	FAMILY "primary" (i, t)
);

CREATE TABLE a (
	i INT NOT NULL,
	CONSTRAINT "primary" PRIMARY KEY (i ASC),
	FAMILY "primary" (i)
);

INSERT INTO t (i, t) VALUES
	(1, 'test'),
	(2, 'other');

INSERT INTO a (i) VALUES
	(2);

ALTER TABLE a ADD CONSTRAINT fk_i_ref_t FOREIGN KEY (i) REFERENCES t (i);

-- Validate foreign key constraints. These can fail if there was unvalidated data during the dump.
ALTER TABLE a VALIDATE CONSTRAINT fk_i_ref_t;
