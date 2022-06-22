# Cockroach Web Functions

**Summary: CWF runs web back-ends from the database directly.**

CWF intends to cut through layers of complexity faced by app
developers, by pushing a db-centric version of web application
development and deployment.

It cross-pollinates superbly with the Cockroach Cloud Serverless product.

In CWF, developers define back-end web functions inside the
database. CockroachDB runs the web back-end in-process with database
servers. DB access latency is eliminated. Web instances auto-scale
with the database. Multi-region web code and its SQL get routed
automatically to the nearest DB region. Code and data are backed up
together. API schemas can be auto-generated from code. User
authn/authz are unified between the web code and the database queries.

In a nutshell, CWF's deep integration with CockroachDB eliminates many
manual tasks and "config hell" needed to produce a state-of-the-art,
Cloud-ready, multi-region web app.

**Cockroach Web Functions are best used for:**
- Auto-provisioning back-end CRUD APIs for inventory management, retail, logistics, metadata, digital assets, ledgers, user profiles, project management, etc.
- Fast prototyping of (multi-region) web apps.
- Defining custom HTTP data ingestion endpoints from 3rd party data producers.
- Creating new database observability features for DBAs without upgrading CockroachDB.
- Implementing custom REST APIs on top of an app-specific data store (e.g. SCIM).
- Defining app-specific end-user data control interfaces for GDPR compliance.

**Key features:**
- Web functions can be defined directly as SQL: CockroachDB generates API glue automatically (JSON schema and results, e.g. via Swagger).
- Web functions can be uploaded as WASM, compiled from JS, Rust, C++ etc.
- Multi-region routing: zero-conf lowest data latency to the nearest SQL region.
- Common versioning and checkpointing between web code and data, including SQL schema. Atomic app upgrades.
- Direct integration with Cloud identity providers for authentication and authorization.
- Database servers auto-scale with web application load. No SQL conn throttling.
- Integrated distributed tracing between web app and database for load analysis and troubleshooting.
- Optional auto-provisioning of TLS certificates for web services.
- Optional server-side rendering with a simple templating engine for demos.

**How does it work?**

Developers upload web functions & config into db using SQL, WebDAV or
a CLI. CockroachDB takes care of receiving HTTP requests and routing
them to the appropriate web function. Each request runs in a sandbox,
but the web function can read HTTP parameters, access the database
using SQL and produce a HTTP response. Next to a general-purpose WASM
sandbox, CockroachDB also pre-defines multiple execution methods for
common cases & to ease prototyping.
