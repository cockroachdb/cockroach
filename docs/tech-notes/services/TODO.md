# High-level to-do list

Marketing items:

- [ ] write a one-pager explanation.
- [ ] prepare a compelling demo.

Technical items:

- Infrastructure:
  - [ ] SQL front-end statements: CREATE SERVICE etc
  - [ ] extract the definition of injected functions away from the HTTP handler.
  - [ ] implement `current_database`.
  - [ ] create "validate" function to check the validity of a config without applying it.
  - [ ] support for `redirect_success` / `redirect_error` for the login/logout predefined services.
  - [ ] proper authorization for DROP/ALTER based on owner (not just `admin` role).
  - [ ] execution method "sql" (query spelled out in full by configuration)
  - [ ] auto-generate Swagger spec and demonstrate service exploration using Swagger UI
  - [ ] more unit tests

- Demonstrators:
  - [X] make the HTTP listen address/port configurable via `--services-addr` on the CLI.
  - [ ] serve in `cockroach demo`
  - [ ] sketch a couple of demos in `cockroach gen example-data services` (at least one demo should be
        implemented in two variants: one with server-side rendering and
        one using a js front-end):
    - [ ] blog app
    - [ ] url shortener
    - [ ] message box
    - [ ] SQL table editor (maybe initially only for simple data types)

Stretch:

- [ ] performance benchmarking (incl mem usage)
- [ ] routing based on virtual host name (the `virtual_host` option)
- [ ] auto-provisioning of TLS certificates
- [ ] execution method "table" (service defined via just a SQL table
      name and perhaps columns, so that GET runs a SELECT, and PUT
      runs an UPSERT)
- [ ] execution method BLOB using streaming aggregation across multiple SQL rows.
- [ ] execution method WASM using wazero  https://github.com/tetratelabs/wazero (talk to Andrew W)
- [ ] execution method GraphQL
