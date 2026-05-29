// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
Package pgwire implements the server side of the PostgreSQL wire
protocol (version 3.0, supported by Postgres 7.4 and later) for
CockroachDB. It is the entry point for every SQL client connection: it
accepts TCP/Unix sockets, negotiates TLS, performs authentication,
relays parsed commands to the SQL execution engine, and streams
results back to the client in the format the protocol prescribes.

The on-the-wire message catalogue, framing rules, and parameter
encoding are defined upstream in the PostgreSQL documentation:

  - Protocol overview:
    https://www.postgresql.org/docs/current/protocol.html
  - Message formats:
    https://www.postgresql.org/docs/current/protocol-message-formats.html
  - Message flow:
    https://www.postgresql.org/docs/current/protocol-flow.html

This file documents how that protocol is implemented inside
CockroachDB; the rest of this comment assumes a working knowledge of
the message catalogue above.

# Layered Architecture

A SQL client connection traverses the following layers, top down:

  - Server (server.go) — one instance per SQL tenant. Owns the
    accept loop, connection metrics, the HBA / identity-map config,
    the per-tenant memory monitor, and tracks open connections so
    they can be canceled at drain time.

  - PreServeConnHandler (pre_serve.go) — tenant-independent. It
    inspects the first message on the wire, handles the SSLRequest
    and GSSENCRequest sentinels, performs the TLS handshake, decodes
    the StartupMessage, and decides which tenant Server should own
    the connection. Memory used during this phase is accounted to a
    tenant-independent monitor; ownership is transferred to the
    tenant-specific monitor once the connection is routed.

  - conn (conn.go) — represents a single authenticated connection.
    A dedicated goroutine reads protocol messages, decodes them, and
    pushes commands onto a sql.StmtBuf. The same goroutine also
    handles the write side: it formats results from a buffer fed by
    sql.ConnExecutor and flushes them to the wire.

  - authenticator (authenticator.go, auth.go, auth_methods.go,
    auth_behaviors.go) — runs in a separate goroutine that
    cooperates with conn over an AuthConn channel. It evaluates the
    HBA rules, performs the authentication challenge/response
    (cleartext password, SCRAM-SHA-256, certificate, GSS, JWT,
    LDAP), optionally maps an external identity to a database
    username through pg_ident, and signals success or failure.

  - commandResult (command_result.go) — implements
    sql.ClientComm. ConnExecutor calls into it to add rows, set the
    command tag, advertise notices, etc. commandResult buffers those
    operations and ultimately writes the corresponding pgwire
    messages (RowDescription, DataRow, CommandComplete,
    ReadyForQuery, ErrorResponse, NoticeResponse, ParameterStatus)
    onto the conn's write buffer.

The split between these layers is intentional: pre_serve runs before
we know which tenant the connection belongs to, conn owns wire I/O,
the authenticator owns the authentication state machine, and
commandResult adapts the SQL executor's row-oriented API to the
protocol's message-oriented API. Because the read goroutine in conn
must never block on SQL execution (it would stall the client's
message stream), the StmtBuf indirection between conn and
ConnExecutor is mandatory rather than incidental.

# Connection Lifecycle

A successful connection follows these phases:

 1. Accept. The server's listener delivers a net.Conn. The
    PreServeConnHandler reads the first message: an SSLRequest or
    GSSENCRequest produces a single-byte 'S' / 'N' response and, if
    affirmative, upgrades the socket; then the StartupMessage
    follows, carrying user, database, and other status parameters.
    A versionCancel first message is dispatched immediately to the
    cancel path described below and the socket is closed.

 2. Route to tenant. The handler resolves the tenant from the
    StartupMessage and hands the connection (with reserved memory)
    to that tenant's Server.ServeConn.

 3. Authenticate. conn.processCommands starts the authenticator
    goroutine and blocks until it succeeds or fails. Authentication
    can require multiple round trips (e.g. SCRAM is a three-leg
    handshake); intermediate AuthenticationRequest messages are
    written by conn on behalf of the authenticator. Failure produces
    an ErrorResponse and closes the connection.

 4. Send initial connection data. After an AuthenticationOk,
    conn writes ParameterStatus messages for the well-known status
    parameters (server_version, client_encoding, DateStyle, ...),
    then a BackendKeyData containing the value used for query
    cancellation (see "Cancellation" below), and finally a
    ReadyForQuery indicating an idle transaction block.

 5. Process commands. conn enters its read loop. For each protocol
    message it pushes one or more commands onto the StmtBuf. The
    ConnExecutor (in pkg/sql) consumes the StmtBuf in another
    goroutine, executes them, and feeds results back through
    commandResult. conn's write side flushes those results when the
    executor completes a command or when an explicit Flush /
    ReadyForQuery is required by the protocol.

 6. Termination. A Terminate message, network error, server
    shutdown, or context cancellation ends the loop. conn closes
    the StmtBuf (which causes the executor goroutine to exit),
    releases reserved memory, and the connection's entry is removed
    from the server's connCancelMap.

# Simple vs. Extended Query Protocol

The protocol exposes two execution modes; both are supported:

  - Simple Query: a single Query message contains a SQL string,
    which may contain multiple statements. handleSimpleQuery parses
    the string and pushes one ExecStmt per statement to the
    StmtBuf. COPY FROM is special-cased: it temporarily yields the
    connection's read goroutine to a copyMachine that consumes
    CopyData messages directly. COPY in the simple protocol is the
    only supported form (extended-mode COPY is rejected explicitly
    in handleParse).

  - Extended Query: Parse, Bind, Describe, Execute, Close, Sync,
    and Flush messages each become individual commands
    (PrepareStmt, BindStmt, DescribeStmt, ExecPortal,
    DeletePreparedStmt, etc.) on the StmtBuf. The protocol allows
    pipelining — the client may send several commands before the
    first Sync — and the implementation honors that by deferring
    completion messages until either a Sync arrives or an
    error forces the executor to skip ahead to the next Sync.

Result encoding is governed by the pgwirebase.FormatCode value
selected during Bind (or text by default). types.go contains the
text/binary encoders for every Postgres type CockroachDB supports;
gaps are surfaced as protocol errors at this layer.

# Cancellation

Postgres performs query cancellation out of band: a separate TCP
connection sends a CancelRequest containing a 64-bit BackendKeyData
that the server issued at session start. CockroachDB cannot use the
Postgres scheme verbatim because the cancel may land on a node that
does not own the session, so the BackendKeyData encodes the
SQLInstanceID of the originating node alongside random session bits.
PreServeConnHandler routes a versionCancel message either to the
local server (if it owns the SQLInstanceID) or via DistSQL to the
node that does. See pkg/sql/pgwire/pgwirecancel and the design
document at docs/RFCS/20220202_pgwire_compatible_cancel.md.

# Authentication and Identity Mapping

Authentication selection is HBA-driven. The hba subpackage parses
pg_hba.conf-style configuration (settable per cluster via the
server.host_based_authentication.configuration cluster setting). For
each new connection the server evaluates rules in order against the
peer address, connection type (local, hostssl, hostnossl, ...),
database, and user, and selects the first matching method.

Each method is registered in auth_methods.go and produces an
AuthBehaviors value (auth_behaviors.go) that bundles four
operations:

  - ReplacementIdentity: optionally translates the identity that
    the protocol-level user string refers to into a different
    "system identity" (e.g. a Kerberos principal or X.509 CN).

  - MapRole: consults pg_ident-style mappings (the identmap
    subpackage) to translate that system identity into the database
    role the connection will assume.

  - Authenticate: performs the actual challenge/response with the
    client and verifies credentials.

  - MaybeAuthorize: optionally performs post-auth authorization
    (used by LDAP for group membership lookup).

The resulting role determines session defaults, replication
privilege, and the connection's identity for the rest of its
lifetime. Auth events are logged to the SESSIONS log channel; set
server.auth_log.sql_sessions.enabled or server.auth_log.sql_connections.enabled
to control verbosity.

# Compatibility Notes

CockroachDB targets PostgreSQL wire compatibility but is not
identical. Notable points to keep in mind:

  - pgcode values are not always identical to PostgreSQL's. The
    pgcode subpackage is canonical for the codes CockroachDB
    returns; treat the comment in pgcode/doc.go as authoritative.

  - COPY is supported only in the simple protocol; extended-mode
    COPY FROM and asynchronous COPY pipelining are not supported.

  - The replication subprotocol (the START_REPLICATION /
    IDENTIFY_SYSTEM commands used by physical replication tools)
    has limited support, gated on the replication session
    parameter. See pkg/sql/pgrepl.

  - Backend message types beyond the Postgres-defined set must
    not be invented; clients in the wild assume the message-type
    byte enumerates a known set, and unknown bytes typically
    cause client disconnects.

When a Postgres client misbehaves against CockroachDB, the most
common causes are: (a) a type whose binary encoding diverges from
Postgres in a corner case (look in types.go and write a pgtest
case under pkg/testutils/pgtest), or (b) a session parameter or
ParameterStatus value the client expected but that CockroachDB
does not advertise (statusReportParams in conn.go controls what we
send).

# Subpackages

  - pgwirebase — protocol primitives shared with pkg/sql.
    Contains the message-type enums, ReadBuffer, FormatCode, and
    the Conn interface used by the COPY machine. The split exists
    to break an import cycle: pkg/sql cannot import pgwire because
    pgwire imports pkg/sql, but the COPY implementation in pkg/sql
    needs to read raw protocol bytes.

  - pgcode — the catalogue of 5-character SQLSTATE error codes
    used throughout the source tree.

  - pgerror — the canonical error type for SQL execution.
    Exposes constructors that attach a pgcode, support for hints
    and details (rendered as the corresponding ErrorResponse
    fields), and serialization for distributed execution.

  - pgnotice — analog of pgerror for non-fatal NoticeResponse
    messages (warnings, notices, info messages emitted by
    statements like SET).

  - pgwirecancel — the BackendKeyData type and helpers used by
    the cancel routing path described above.

  - hba — pg_hba.conf parser and matcher.

  - identmap — pg_ident.conf parser; maps external system
    identities to database roles.

# Debugging

Useful entry points when investigating a wire-protocol bug:

  - Reproduce against the in-tree byte-level tester. The
    pkg/testutils/pgtest harness drives a real connection by
    sending typed messages and asserting on the server's reply,
    one byte at a time. pgtest_test.go in this package wires it up
    and supports running the same script against a vanilla
    Postgres server (-addr) to confirm whether divergent behavior
    is a bug or a known incompatibility.

  - Capture a wire trace with Wireshark's pgsql dissector, or set
    PGOPTIONS="-c log_statement=all" plus a libpq trace
    (PQtrace) to dump the message stream from the client side.

  - Inspect the SESSIONS log channel; it contains structured
    events for authentication outcomes, drain-induced
    disconnects, and unexpected protocol errors. The EXEC log
    channel records each statement received via either protocol
    mode.

  - Query crdb_internal.cluster_sessions (or use SHOW SESSIONS)
    to observe in-flight sessions, including the user, client
    address, active query, and elapsed time. Combine with
    crdb_internal.cluster_queries to correlate cancel attempts
    with the queries they targeted.

# Related RFCs

  - docs/RFCS/20220202_pgwire_compatible_cancel.md — the
    BackendKeyData scheme described above.

  - docs/RFCS/20220721_sso_authentication.md — the design of
    JWT- and SSO-based authentication methods plugged into the
    AuthBehaviors framework.

  - docs/RFCS/20160425_drain_modes.md — the multi-phase drain
    protocol that this package implements via Drain /
    WaitForSQLConnsToClose / drainImpl.
*/
package pgwire
