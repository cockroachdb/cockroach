This directory contains the sources for CockroachDB's SQL middle-end.

As per the [tech note on this topic](../../docs/tech-notes/sql.md),
this layer contains multiple components interacting together.

In an ideal world, we would make these components live in separate Go
packages. However, because Go does not allow circular dependencies, this
separation is technically difficult.

Therefore, we resort to a separation of concerns based on separate
file name prefixes.

The following components are part of the high-level architecture of
the SQL layer:

| SQL middle-end component              | File name prefix |
|---------------------------------------|------------------|
| Session, Executor                     | `x_`             |
| Prep phase (initial logical planning) | `p_`             |
| Logical plan optimization             | `o_`             |
| Schema access, including lease mgt.   | `sa_`            |
| Schema changes                        | `sc_`            |
| Run-time aspects of query execution   | `r_`             |

The following components exist in this package currently, but
will like be moved / merged into other places over time.

| Component                                   | File name prefix |
|---------------------------------------------|------------------|
| Low-level SQL/KV code                       | `sqlkv_`         |
| Logical plan constructors + local execution | `n_`             |

The latter set of files currently prefixed with `n_` will, over time,
be split up between code pertaining to logical planning (planNode
hierarchy and constructors), which will be prefixed by `p_`, and and
code pertaining to local (i.e. non-distributed) execution, which will
be prefixed by `r_`.
