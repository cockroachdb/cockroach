// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cliflags

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/kr/text"
)

// FlagInfo contains the static information for a CLI flag and helper
// to format the description.
type FlagInfo struct {
	// Name of the flag as used on the command line.
	Name string

	// Shorthand is the short form of the flag (optional).
	Shorthand string

	// EnvVar is the name of the environment variable through which the flag value
	// can be controlled (optional).
	EnvVar string

	// Description of the flag.
	//
	// The text will be automatically re-wrapped. The wrapping can be stopped by
	// embedding the tag "<PRE>": this tag is removed from the text and
	// signals that everything that follows should not be re-wrapped. To start
	// wrapping again, use "</PRE>".
	Description string
}

const usageIndentation = 1
const wrapWidth = 79 - usageIndentation

// wrapDescription wraps the text in a FlagInfo.Description.
func wrapDescription(s string) string {
	var result bytes.Buffer

	// split returns the parts of the string before and after the first occurrence
	// of the tag.
	split := func(str, tag string) (before, after string) {
		pieces := strings.SplitN(str, tag, 2)
		switch len(pieces) {
		case 0:
			return "", ""
		case 1:
			return pieces[0], ""
		default:
			return pieces[0], pieces[1]
		}
	}

	for len(s) > 0 {
		var toWrap, dontWrap string
		// Wrap everything up to the next stop wrap tag.
		toWrap, s = split(s, "<PRE>")
		result.WriteString(text.Wrap(toWrap, wrapWidth))
		// Copy everything up to the next start wrap tag.
		dontWrap, s = split(s, "</PRE>")
		result.WriteString(dontWrap)
	}
	return result.String()
}

// Usage returns a formatted usage string for the flag, including:
// * line wrapping
// * indentation
// * env variable name (if set)
func (f FlagInfo) Usage() string {
	s := "\n" + wrapDescription(f.Description)
	if f.EnvVar != "" {
		// Check that the environment variable name matches the flag name. Note: we
		// don't want to automatically generate the name so that grepping for a flag
		// name in the code yields the flag definition.
		correctName := "COCKROACH_" + strings.ToUpper(strings.Replace(f.Name, "-", "_", -1))
		if f.EnvVar != correctName {
			panic(fmt.Sprintf("incorrect EnvVar %s for flag %s (should be %s)",
				f.EnvVar, f.Name, correctName))
		}
		s = s + "\nEnvironment variable: " + f.EnvVar
	}
	// github.com/spf13/pflag appends the default value after the usage text. Add
	// an additional indentation so the default is well-aligned with the
	// rest of the text. This is admittedly fragile.
	return text.Indent(s, strings.Repeat(" ", usageIndentation)) + "\n"
}

// Attrs and others store the static information for CLI flags.
var (
	Attrs = FlagInfo{
		Name: "attrs",
		Description: `
An ordered, colon-separated list of node attributes. Attributes are arbitrary
strings specifying machine capabilities. Machine capabilities might include
specialized hardware or number of cores (e.g. "gpu", "x16c"). For example:
<PRE>

  --attrs=x16c:gpu</PRE>`,
	}

	Locality = FlagInfo{
		Name: "locality",
		Description: `
An ordered, comma-separated list of key-value pairs that describe the topography
of the machine. Topography often includes cloud provider regions and availability
zones, but can also refer to on-prem concepts like datacenter or rack. Data is
automatically replicated to maximize diversities of each tier. The order of tiers
is used to determine the priority of the diversity, so the more inclusive localities
like region should come before less inclusive localities like availability zone. The
tiers and order must be the same on all nodes. Including more tiers is better than
including fewer. For example:
<PRE>

  --locality=cloud=gce,region=us-west1,zone=us-west-1b
  --locality=cloud=aws,region=us-east,zone=us-east-2

</PRE>
This flag is incompatible with --locality-file.`,
	}

	LocalityFile = FlagInfo{
		Name: "locality-file",
		Description: `
File name to read locality data from. Using this flag has the same effect as
providing the file's contents directly via the --locality flag. Any leading or
trailing whitespace characters, as defined by Unicode, will be automatically
trimmed.
<PRE>

</PRE>
This flag is incompatible with --locality.`,
	}

	Background = FlagInfo{
		Name: "background",
		Description: `
Start the server in the background. This is similar to appending "&"
to the command line, but when the server is started with --background,
control is not returned to the shell until the server is ready to
accept requests.`,
	}

	DisableInMemoryTenant = FlagInfo{
		Name:        "disable-in-memory-tenant",
		Description: `Do not start a secondary tenant in-memory.`,
	}

	// TODO(knz): Remove this once https://github.com/cockroachdb/cockroach/issues/84604
	// is addressed.
	ApplicationInternalRPCPortRange = FlagInfo{
		Name: "internal-rpc-port-range",
		Description: `
TCP port range to use for the internal RPC service for application-level servers.
This service is used for node-to-node RPC traffic and to serve data for 'debug zip'.
`,
	}

	SQLMem = FlagInfo{
		Name: "max-sql-memory",
		Description: `
Maximum memory capacity available to store temporary data for SQL clients,
including prepared queries and intermediate data rows during query execution.
Accepts numbers interpreted as bytes, size suffixes (e.g. 1GB and 1GiB) or a
percentage of physical memory (e.g. .25). If left unspecified, defaults to 25% of
physical memory.`,
	}

	GoMemLimit = FlagInfo{
		Name: "max-go-memory",
		Description: `
Soft memory limit set on the Go runtime (which is also configurable via the
GOMEMLIMIT environment variable, but --max-go-memory has higher precedence if
both are set). Notably, the pebble cache (as configured by --cache) is not under
control of the Go runtime and should not be considered when determining this
soft memory limit. Accepts numbers interpreted as bytes, size suffixes (e.g. 1GB
and 1GiB) or a percentage of physical memory (e.g. .25). If left unspecified,
defaults to 2.25x of --max-sql-memory (subject to max-go-memory + 1.15x --cache
not exceeding 90% of available RAM). Set to 0 to disable the soft memory limit
(not recommended).`,
	}

	TSDBMem = FlagInfo{
		Name: "max-tsdb-memory",
		Description: `
Maximum memory capacity available to store temporary data for use by the
time-series database to display metrics in the DB Console. Accepts numbers
interpreted as bytes, size suffixes (e.g. 1GB and 1GiB) or a
percentage of physical memory (e.g. 0.01). If left unspecified, defaults to
1% of physical memory or 64MiB whichever is greater. It maybe necessary to
manually increase this value on a cluster with hundreds of nodes where
individual nodes have very limited memory available. This can constrain
the ability of the DB Console to process time-series queries used to render
metrics for the entire cluster. This capacity constraint does not affect
SQL query execution.`,
	}

	GoGCPercent = FlagInfo{
		Name: "go-gc-percent",
		Description: `
Garbage collection target percentage set on the Go runtime (which is also
configurable via the GOGC environment variable, but --go-gc-percent has higher
precedence if both are set). A garbage collection is triggered when the ratio of
freshly allocated data to live data remaining after the previous collection
reaches this percentage. If left unspecified, defaults to 300%. If set to a
negative value, disables the target percentage garbage collection heuristic,
leaving only the soft memory limit heuristic to trigger garbage collection.`,
	}

	SQLTempStorage = FlagInfo{
		Name: "max-disk-temp-storage",
		Description: `
Maximum storage capacity available to store temporary disk-based data for SQL
queries that exceed the memory budget (e.g. join, sorts, etc are sometimes able
to spill intermediate results to disk). Accepts numbers interpreted as bytes,
size suffixes (e.g. 32GB and 32GiB) or a percentage of disk size (e.g. 10%). If
left unspecified, defaults to 32GiB.
<PRE>

</PRE>
The location of the temporary files is within the first store dir (see --store).
If expressed as a percentage, --max-disk-temp-storage is interpreted relative to
the size of the storage device on which the first store is placed. The temp
space usage is never counted towards any store usage (although it does share the
device with the first store) so, when configuring this, make sure that the size
of this temp storage plus the size of the first store don't exceed the capacity
of the storage device.
<PRE>

</PRE>
If the first store is an in-memory one (i.e. type=mem), then this temporary
"disk" data is also kept in-memory. A percentage value is interpreted as a
percentage of the available internal memory. If not specified, the default
shifts to 100MiB when the first store is in-memory.
`,
	}

	AuthTokenValidityPeriod = FlagInfo{
		Name: "expire-after",
		Description: `
Duration after which the newly created session token expires.`,
	}

	OnlyCookie = FlagInfo{
		Name: "only-cookie",
		Description: `
Display only the newly created cookie on the standard output
without additional details and decoration.`,
	}

	Cache = FlagInfo{
		Name: "cache",
		Description: `
Total size in bytes for caches, shared evenly if there are multiple
storage devices. Size suffixes are supported (e.g. 1GB and 1GiB).
If left unspecified, defaults to 128MiB. A percentage of physical memory
can also be specified (e.g. .25).`,
	}

	ClientHost = FlagInfo{
		Name:   "host",
		EnvVar: "COCKROACH_HOST",
		Description: `
CockroachDB node to connect to.
This can be specified either as an address/hostname, or
together with a port number as in -s myhost:26257.
If the port number is left unspecified, it defaults to 26257.
An IPv6 address can also be specified with the notation [...], for
example [::1]:26257 or [fe80::f6f2:::]:26257.`,
	}

	ClientPort = FlagInfo{
		Name:        "port",
		Shorthand:   "p",
		EnvVar:      "COCKROACH_PORT",
		Description: `Deprecated. Use --host=<host>:<port>.`,
	}

	Database = FlagInfo{
		Name:        "database",
		Shorthand:   "d",
		EnvVar:      "COCKROACH_DATABASE",
		Description: `The name of the database to connect to.`,
	}

	DumpMode = FlagInfo{
		Name: "dump-mode",
		Description: `
What to dump. "schema" dumps the schema only. "data" dumps the data only.
"both" (default) dumps the schema then the data.`,
	}

	ReadTime = FlagInfo{
		Name: "as-of",
		Description: `
Reads the data as of the specified timestamp. Formats supported are the same
as the timestamp type.`,
	}

	DumpAll = FlagInfo{
		Name: "dump-all",
		Description: `
Dumps all databases, for each non-system database provides dump of all available tables.`,
	}

	Execute = FlagInfo{
		Name:      "execute",
		Shorthand: "e",
		Description: `
Execute the SQL statement(s) on the command line, then exit. This flag may be
specified multiple times and each value may contain multiple semicolon
separated statements. If an error occurs in any statement, the command exits
with a non-zero status code and further statements are not executed. The
results of each SQL statement are printed on the standard output.

This flag is incompatible with --file / -f.`,
	}

	File = FlagInfo{
		Name:      "file",
		Shorthand: "f",
		Description: `
Read and execute the SQL statement(s) from the specified file.
The file is processed as if it has been redirected on the standard
input of the shell.

This flag is incompatible with --execute / -e.`,
	}

	Watch = FlagInfo{
		Name: "watch",
		Description: `
Repeat the SQL statement(s) specified with --execute
with the specified period. The client will stop watching
if an execution of the SQL statement(s) fail.`,
	}

	NoLineEditor = FlagInfo{
		Name: "no-line-editor",
		Description: `
Force disable the interactive line editor. Can help during testing.`,
	}

	EchoSQL = FlagInfo{
		Name: "echo-sql",
		Description: `
Reveal the SQL statements sent implicitly by the command-line utility.`,
	}

	CliDebugMode = FlagInfo{
		Name: "debug-sql-cli",
		Description: `
Simplify the SQL CLI to ease troubleshooting of CockroachDB
issues. This echoes sent SQL, removes the database name and txn status
from the prompt, and forces behavior to become independent on current
transaction state. Equivalent to --echo-sql, \unset check_syntax and
\set prompt1 %n@%M>.`,
	}

	EmbeddedMode = FlagInfo{
		Name: "embedded",
		Description: `
Simplify and reduce the SQL CLI output to make it appropriate for
embedding in a 'playground'-type environment.

This causes the shell to omit informational message about
aspects that can only be changed with command-line flags
or environment variables: in an embedded environment, the user
has no control over these and the messages would thus be
confusing.

It also causes the shell to omit informational messages about
networking details (e.g. server address), as it is assumed
that the embedding environment will report those instead.`,
	}

	SafeUpdates = FlagInfo{
		Name: "safe-updates",
		Description: `
Disable SQL statements that may have undesired side effects. For
example a DELETE or UPDATE without a WHERE clause. By default, this
setting is enabled (true) and such statements are rejected to prevent
accidents. This can also be overridden in a session with SET
sql_safe_updates = FALSE.`,
	}

	ReadOnly = FlagInfo{
		Name: "read-only",
		Description: `
Set the session variable default_transaction_read_only to on.`,
	}

	Set = FlagInfo{
		Name: "set",
		Description: `
Set a client-side configuration parameter before running the SQL
shell. This flag may be specified multiple times.`,
	}

	TableDisplayFormat = FlagInfo{
		Name: "format",
		Description: `
Selects how to display table rows in results. Possible values: tsv, csv,
table, records, ndjson, json, sql, html, raw. If left unspecified, defaults to
tsv for non-interactive sessions and table for interactive sessions.`,
	}

	ClusterName = FlagInfo{
		Name: "cluster-name",
		Description: `
Sets a name to verify the identity of a remote node or cluster. The value must
match between this node and the remote node(s) specified via --join.
<PRE>

</PRE>
This can be used as an additional verification when either the node or cluster,
or both, have not yet been initialized and do not yet know their cluster ID.
<PRE>

</PRE>
To introduce a cluster name into an already-initialized cluster, pair this flag
with --disable-cluster-name-verification.
`,
	}

	DisableClusterNameVerification = FlagInfo{
		Name: "disable-cluster-name-verification",
		Description: `
Tell the server to ignore cluster name mismatches. This is meant for use when
opting an existing cluster into starting to use cluster name verification, or
when changing the cluster name.
<PRE>

</PRE>
The cluster should be restarted once with --cluster-name and
--disable-cluster-name-verification combined, and once all nodes have
been updated to know the new cluster name, the cluster can be
restarted again with this flag removed.`,
	}

	Join = FlagInfo{
		Name:      "join",
		Shorthand: "j",
		Description: `
The addresses for connecting a node to a cluster.
<PRE>

</PRE>
When starting a multi-node cluster for the first time, set this flag
to the addresses of 3-5 of the initial nodes. Then run the cockroach
init command against one of the nodes to complete cluster startup.
<PRE>

</PRE>
When starting a singe-node cluster, leave this flag out. This will
cause the node to initialize a new single-node cluster without
needing to run the cockroach init command.
<PRE>

</PRE>
When adding a node to an existing cluster, set this flag to 3-5
of the nodes already in the cluster; it's easiest to use the same
list of addresses that was used to start the initial nodes.
<PRE>

</PRE>
This flag can be specified separately for each address:
<PRE>

  --join=localhost:1234 --join=localhost:2345

</PRE>
Or can be specified as a comma separated list in single flag,
or both forms can be used together, for example:
<PRE>

  --join=localhost:1234,localhost:2345 --join=localhost:3456</PRE>`,
	}

	JoinPreferSRVRecords = FlagInfo{
		Name: "experimental-dns-srv",
		Description: `
When enabled, the node will first attempt to fetch SRV records
from DNS for every name specified with --join. If a valid
SRV record is found, that information is used instead
of regular DNS A/AAAA lookups.
This feature is experimental and may be removed or modified
in a later version.`,
	}

	ListenAddr = FlagInfo{
		Name: "listen-addr",
		Description: `
The address/hostname and port to listen on for intra-cluster
communication, for example --listen-addr=myhost:26257 or
--listen-addr=:26257 (listen on all interfaces).
<PRE>

</PRE>
If the address part is left unspecified, it defaults to
the "all interfaces" address (0.0.0.0 IPv4 / [::] IPv6).
If the port part is left unspecified, it defaults to 26257.
<PRE>

</PRE>
An IPv6 address can also be specified with the notation [...], for
example [::1]:26257 or [fe80::f6f2:::]:26257.
<PRE>

</PRE>
If --advertise-addr is left unspecified, the node will also announce
this address for use by other nodes. It is strongly recommended to use
--advertise-addr in cloud and container deployments or any setup where
NAT is present between cluster nodes.
<PRE>

</PRE>
Unless --sql-addr is also specified, this address is also
used to accept SQL client connections. Using --listen-addr
to specify the SQL address without --sql-addr is a deprecated
feature.
`,
	}

	ServerHost = FlagInfo{
		Name:        "host",
		Description: `Alias for --listen-addr. Deprecated.`,
	}

	ServerPort = FlagInfo{
		Name:        "port",
		Description: `Alias for --listen-port. Deprecated.`,
	}

	AdvertiseAddr = FlagInfo{
		Name: "advertise-addr",
		Description: `
The address/hostname and port to advertise to other CockroachDB nodes
for intra-cluster communication. It must resolve and be routable from
other nodes in the cluster.
<PRE>

</PRE>
If left unspecified, it defaults to the setting of --listen-addr.
If the flag is provided but either the address part or the port part
is left unspecified, that particular part defaults to the
same part in --listen-addr.
<PRE>

</PRE>
An IPv6 address can also be specified with the notation [...], for
example [::1]:26257 or [fe80::f6f2:::]:26257.
<PRE>

</PRE>
The port number should be the same as in --listen-addr unless port
forwarding is set up on an intermediate firewall/router.`,
	}

	AdvertiseHost = FlagInfo{
		Name:        "advertise-host",
		Description: `Alias for --advertise-addr. Deprecated.`,
	}

	AdvertisePort = FlagInfo{
		Name:        "advertise-port",
		Description: `Deprecated. Use --advertise-addr=<host>:<port>.`,
	}

	ListenSQLAddr = FlagInfo{
		Name: "sql-addr",
		Description: `
The hostname or IP address to bind to for SQL clients, for example
--sql-addr=myhost:26257 or --sql-addr=:26257 (listen on all interfaces).
<PRE>

</PRE>
If specified but the address part is omitted, the address part
defaults to the address part of --listen-addr.
If specified but the port number is omitted, the port number
defaults to 26257.
<PRE>

</PRE>
To actually use separate bindings, it is recommended to specify
both flags and use a different port number via --listen-addr, for
example --sql-addr=:26257 --listen-addr=:26258. Ensure that
--join is set accordingly on other nodes. It is also possible
to use the same port number but separate host addresses.
<PRE>

</PRE>
An IPv6 address can also be specified with the notation [...], for
example [::1]:26257 or [fe80::f6f2:::]:26257.
<PRE>

</PRE>
If --sql-addr is left unspecified, the address specified by
--listen-addr will be used for both RPC and SQL connections.
This default behavior is deprecated; we recommend always
setting --sql-addr.`,
	}

	SQLAdvertiseAddr = FlagInfo{
		Name: "advertise-sql-addr",
		Description: `
The SQL address/hostname and port to advertise to CLI admin utilities
and via SQL introspection for the purpose of SQL address discovery.
It must resolve and be routable from clients.
<PRE>

</PRE>
If left unspecified, it defaults to the setting of --sql-addr.
If the flag is provided but either the address part or the port part
is left unspecified, that particular part defaults to the
same part in --sql-addr.
<PRE>

</PRE>
An IPv6 address can also be specified with the notation [...], for
example [::1]:26257 or [fe80::f6f2:::]:26257.
<PRE>

</PRE>
The port number should be the same as in --sql-addr unless port
forwarding is set up on an intermediate firewall/router.`,
	}

	ListenHTTPAddr = FlagInfo{
		Name: "http-addr",
		Description: `
The hostname or IP address to bind to for HTTP requests.
If left unspecified, the address part defaults to the setting of
--listen-addr. The port number defaults to 8080.
An IPv6 address can also be specified with the notation [...], for
example [::1]:8080 or [fe80::f6f2:::]:8080.`,
	}

	HTTPAdvertiseAddr = FlagInfo{
		Name: "advertise-http-addr",
		Description: `
The HTTP address/hostname and port to advertise to nodes in the cluster
for reporting the DB Console address and proxying of HTTP connections.
It must resolve and be routable from other nodes in the cluster for
proxying to work in DB Console.
<PRE>

</PRE>
If left unspecified, it defaults to the host setting of --advertise-addr
and the port of --http-addr, which is 8080 by default. If advertise-addr
is left unspecified, it defaults to the setting of http-addr. If the
flag is unspecified as well as fallbacks, it defaults to the hostname as
reported by the OS.
<PRE>

</PRE>
An IPv6 address can also be specified with the notation [...], for
example [::1]:26257 or [fe80::f6f2:::]:26257.
<PRE>

</PRE>
The port number should be the same as in --http-addr unless port
forwarding is set up on an intermediate firewall/router.`,
	}

	UnencryptedLocalhostHTTP = FlagInfo{
		Name: "unencrypted-localhost-http",
		Description: `
When specified, restricts HTTP connections to localhost-only and disables
TLS for the HTTP interface. The hostname part of --http-addr, if specified,
is then ignored. This flag is intended for use to facilitate
local testing without requiring certificate setups in web browsers.`,
	}

	AcceptSQLWithoutTLS = FlagInfo{
		Name: "accept-sql-without-tls",
		Description: `
When specified, this node will accept SQL client connections that do not wish
to negotiate a TLS handshake. Authentication is still otherwise required
as per the HBA configuration and all other security mechanisms continue to
apply. This flag is experimental.
`,
	}

	AcceptProxyProtocolHeaders = FlagInfo{
		Name: "accept-proxy-protocol-headers",
		Description: `
Allows CockroachDB to parse proxy protocol headers. Proxy protocol is used by
some proxies to retain the original client IP information after the proxy has
rewritten the source IP address of forwarded packets.
<PRE>

</PRE>
When using this flag, ensure all traffic to CockroachDB flows through a proxy
which adds proxy protocol headers, to prevent spoofing of client IP address
information.
`,
	}

	LocalityAdvertiseAddr = FlagInfo{
		Name: "locality-advertise-addr",
		Description: `
List of ports to advertise to other CockroachDB nodes for intra-cluster
communication for some locality. This should be specified as a comma
separated list of locality@address. Addresses can also include ports.
For example:
<PRE>

  "region=us-west@127.0.0.1,zone=us-west-1b@127.0.0.1"
  "region=us-west@127.0.0.1:26257,zone=us-west-1b@127.0.0.1:26258"</PRE>`,
	}

	ListenHTTPAddrAlias = FlagInfo{
		Name:        "http-host",
		Description: `Alias for --http-addr. Deprecated.`,
	}

	ListenHTTPPort = FlagInfo{
		Name:        "http-port",
		Description: `Deprecated. Use --http-addr=<host>:<port>.`,
	}

	ListeningURLFile = FlagInfo{
		Name: "listening-url-file",
		Description: `
After the CockroachDB node has started up successfully, it will
write its connection URL to the specified file.`,
	}

	PIDFile = FlagInfo{
		Name: "pid-file",
		Description: `
After the CockroachDB node has started up successfully, it will
write its process ID to the specified file.`,
	}

	Socket = FlagInfo{
		Name:        "socket",
		EnvVar:      "COCKROACH_SOCKET",
		Description: `Deprecated in favor of --socket-dir.`,
	}

	SocketDir = FlagInfo{
		Name:   "socket-dir",
		EnvVar: "COCKROACH_SOCKET_DIR",
		Description: `
Accept client connections using a Unix domain socket created
in the specified directory.

Note: for compatibility with PostgreSQL clients and drivers,
the generated socket name has the form "/path/to/.s.PGSQL.NNNN",
where NNNN is the port number configured via --listen-addr.

PostgreSQL clients only take a port number and directory as input and construct
the socket name programmatically. To use, for example:
<PRE>

	psql -h /path/to -p NNNN ...
</PRE>`,
	}

	ClientInsecure = FlagInfo{
		Name:   "insecure",
		EnvVar: "COCKROACH_INSECURE",
		Description: `
Connect to a cluster without using TLS nor authentication.
This makes the client-server connection vulnerable to MITM attacks. Use with care.`,
	}

	ServerInsecure = FlagInfo{
		Name: "insecure",
		Description: `
Start a node with all security controls disabled.
There is no encryption, no authentication and internal security
checks are also disabled. This makes any client able to take
over the entire cluster.
<PRE>

</PRE>
This flag is only intended for non-production testing.
<PRE>

</PRE>
Beware that using this flag on a public network without --listen-addr
is likely to cause the entire host server to become compromised.
<PRE>

</PRE>
To simply accept non-TLS connections for SQL clients while keeping
the cluster secure, consider using --accept-sql-without-tls instead.
Also see: ` + build.MakeIssueURL(53404) + `
`,
	}

	ExternalIODisableHTTP = FlagInfo{
		Name:        "external-io-disable-http",
		Description: `Disable use of HTTP when accessing external data.`,
	}

	ExternalIODisableImplicitCredentials = FlagInfo{
		Name: "external-io-disable-implicit-credentials",
		Description: `
Disable use of implicit credentials when accessing external data.
Instead, require the user to always specify access keys.`,
	}
	ExternalIODisabled = FlagInfo{
		Name: "external-io-disabled",
		Description: `
Disable use of "external" IO, such as to S3, GCS, or the file system (nodelocal), or anything other than userfile.`,
	}
	ExternalIOEnableNonAdminImplicitAndArbitraryOutbound = FlagInfo{
		Name: "external-io-enable-non-admin-implicit-access",
		Description: `
Allow non-admin users to specify arbitrary network addressses (e.g. https:// URIs or custom endpoints in s3:// URIs) and
implicit credentials (machine account/role providers) when running operations like IMPORT/EXPORT/BACKUP/etc.
Note: that --external-io-disable-http or --external-io-disable-implicit-credentials still apply, this only removes the admin-user requirement.`,
	}

	// KeySize, CertificateLifetime, AllowKeyReuse, and OverwriteFiles are used for
	// certificate generation functions.
	KeySize = FlagInfo{
		Name:        "key-size",
		Description: `Key size in bits for CA/Node/Client certificates.`,
	}

	CertificateLifetime = FlagInfo{
		Name:        "lifetime",
		Description: `Certificate lifetime.`,
	}

	AllowCAKeyReuse = FlagInfo{
		Name:        "allow-ca-key-reuse",
		Description: `Use the CA key if it exists.`,
	}

	OverwriteFiles = FlagInfo{
		Name:        "overwrite",
		Description: `Certificate and key files are overwritten if they exist.`,
	}

	TenantScope = FlagInfo{
		Name: "tenant-scope",
		Description: `Assign a tenant scope to the certificate.
This will restrict the certificate to only be valid for the specified tenants.
This flag is optional. When omitted, the certificate is not scoped; i.e.
it can be used with all tenants.`,
	}

	TenantScopeByNames = FlagInfo{
		Name: "tenant-name-scope",
		Description: `Assign a tenant scope using tenant names to the certificate.
This will restrict the certificate to only be valid for the specified tenants.
This flag is optional. When omitted, the certificate is not scoped; i.e.
it can be used with all tenants.`,
	}

	GeneratePKCS8Key = FlagInfo{
		Name:        "also-generate-pkcs8-key",
		Description: `Also write the key in pkcs8 format to <certs-dir>/client.<username>.key.pk8.`,
	}

	DisableUsernameValidation = FlagInfo{
		Name: "disable-username-validation",
		Description: `Do not validate that the provided identity has a valid structure for
a SQL identifier. If passed, and the identity is not a valid SQL identifier, the generated
certificate can only be used if an identity map has been configured server-side.`,
	}

	Password = FlagInfo{
		Name:        "password",
		Description: `Prompt for the new user's password.`,
	}

	CertsDir = FlagInfo{
		Name:        "certs-dir",
		EnvVar:      "COCKROACH_CERTS_DIR",
		Description: `Path to the directory containing SSL certificates and keys.`,
	}

	// Server version of the certs directory flag, cannot be set through environment.
	ServerCertsDir = FlagInfo{
		Name:        "certs-dir",
		Description: CertsDir.Description,
	}

	CertPrincipalMap = FlagInfo{
		Name: "cert-principal-map",
		Description: `
A comma separated list of <cert-principal>:<db-principal> mappings. This allows
mapping the principal in a cert to a DB principal such as "node" or "root" or
any SQL user. This is intended for use in situations where the certificate
management system places restrictions on the Subject.CommonName or
SubjectAlternateName fields in the certificate (e.g. disallowing a CommonName
such as "node" or "root"). If multiple mappings are provided for the same
<cert-principal>, the last one specified in the list takes precedence. A
principal not specified in the map is passed through as-is via the identity
function. A cert is allowed to authenticate a DB principal if the DB principal
name is contained in the mapped CommonName or DNS-type SubjectAlternateName
fields. It is permissible for the <cert-principal> string to contain colons.
`,
	}

	RootCertDistinguishedName = FlagInfo{
		Name: "root-cert-distinguished-name",
		Description: `
A string of comma separated list of distinguished-name
<attribute-type>=<attribute-value> mappings in accordance with RFC4514 for the root
user. This strictly needs to match the DN subject in the client certificate
provided for root user if this flag is set.
`,
	}

	NodeCertDistinguishedName = FlagInfo{
		Name: "node-cert-distinguished-name",
		Description: `
A string of comma separated list of distinguished-name
<attribute-type>=<attribute-value> mappings in accordance with RFC4514 for the node
user. This strictly needs to match the DN subject in the client certificate
provided for node user if this flag is set.
`,
	}

	CAKey = FlagInfo{
		Name:        "ca-key",
		EnvVar:      "COCKROACH_CA_KEY",
		Description: `Path to the CA key.`,
	}

	ClockDevice = FlagInfo{
		Name: "clock-device",
		Description: `
Override HLC to use PTP hardware clock user space API when querying for current
time. The value corresponds to the clock device to be used. This is currently
only tested and supported on Linux.
<PRE>

  --clock-device=/dev/ptp0</PRE>`,
	}

	MaxOffset = FlagInfo{
		Name: "max-offset",
		Description: `
Maximum clock offset for the cluster. If real clock skew exceeds this value,
consistency guarantees can no longer be upheld, possibly resulting in stale
reads and other anomalies. This value affects the frequency of uncertainty-based
read restarts and write latencies for global tables.
<PRE>

</PRE>
If a node detects that its clock offset from other nodes is too large, it will
self-terminate to protect consistency guarantees. This check can be disabled
via --disable-max-offset-check.
<PRE>

</PRE>
This value should be the same on all nodes in the cluster. It is allowed to
differ such that the max-offset value can be changed via a rolling restart of
the cluster, in which case the real clock skew between nodes must be below the
smallest max-offset value of any node.
`,
	}

	DisableMaxOffsetCheck = FlagInfo{
		Name: "disable-max-offset-check",
		Description: `
Normally, a node will self-terminate if it finds that its clock offset with the
rest of the cluster exceeds --max-offset. This flag disables this check. The
operator is responsible for ensuring that real clock skew never exceeds
max-offset, to avoid read inconsistencies and other correctness anomalies.
`,
	}

	Store = FlagInfo{
		Name:      "store",
		Shorthand: "s",
		Description: `
The file path to a storage device. This flag must be specified separately for
each storage device, for example:
<PRE>

  --store=/mnt/ssd01 --store=/mnt/ssd02 --store=/mnt/hda1

</PRE>
For each store, the "attrs" and "size" fields can be used to specify device
attributes and a maximum store size (see below). When one or both of these
fields are set, the "path" field label must be used for the path to the storage
device, for example:
<PRE>

  --store=path=/mnt/ssd01,attrs=ssd,size=20GiB

</PRE>
In most cases, node-level attributes are preferable to store-level attributes.
However, the "attrs" field can be used to match capabilities for storage of
individual databases or tables. For example, an OLTP database would probably
want to allocate space for its tables only on solid state devices, whereas
append-only time series might prefer cheaper spinning drives. Typical
attributes include whether the store is flash (ssd), spinny disk (hdd), or
in-memory (mem), as well as speeds and other specs. Attributes can be arbitrary
strings separated by colons, for example:
<PRE>

  --store=path=/mnt/hda1,attrs=hdd:7200rpm

</PRE>
The store size in the "size" field is not a guaranteed maximum but is used when
calculating free space for rebalancing purposes. The size can be specified
either in a bytes-based unit or as a percentage of hard drive space,
for example:
<PRE>

  --store=path=/mnt/ssd01,size=10000000000     -> 10000000000 bytes
  --store=path=/mnt/ssd01,size=20GB            -> 20000000000 bytes
  --store=path=/mnt/ssd01,size=20GiB           -> 21474836480 bytes
  --store=path=/mnt/ssd01,size=0.02TiB         -> 21474836480 bytes
  --store=path=/mnt/ssd01,size=20%             -> 20% of available space
  --store=path=/mnt/ssd01,size=0.2             -> 20% of available space
  --store=path=/mnt/ssd01,size=.2              -> 20% of available space

</PRE>
For an in-memory store, the "type" and "size" fields are required, and the
"path" field is forbidden. The "type" field must be set to "mem", and the
"size" field must be set to the true maximum bytes or percentage of available
memory that the store may consume, for example:
<PRE>

  --store=type=mem,size=20GiB
  --store=type=mem,size=90%

</PRE>
Optionally, to configure admission control enforcement to prevent disk
bandwidth saturation, the "provisioned-rate" field can be specified with
the "disk-name" and an optional "bandwidth". The bandwidth is used to override
the value of the cluster setting, kvadmission.store.provisioned_bandwidth.
For example:
<PRE>

  --store=provisioned-rate=disk-name=nvme1n1
  --store=provisioned-rate=disk-name=sdb:bandwidth=250MiB/s

</PRE>
Commas are forbidden in all values, since they are used to separate fields.
Also, if you use equal signs in the file path to a store, you must use the
"path" field label.

(default is 'cockroach-data' in current directory except for mt commands
which use 'cockroach-data-tenant-X' for tenant 'X')
`,
	}

	WALFailover = FlagInfo{
		Name:   "wal-failover",
		EnvVar: "COCKROACH_WAL_FAILOVER",
		Description: `
Configures the use and behavior of WAL failover. WAL failover enables
automatic failover to another directory if a WAL write does not complete
within the configured threshold. Defaults to "disabled". Possible values
depend on the number of stores a node is configured to use.

If a node has multiple stores, the value "among-stores" enables automatic
failover to another store's data directory. CockroachDB will automatically
assign each store a secondary to serve as its WAL failover destination.
For example:
<PRE>

  --wal-failover=among-stores

</PRE>

If a node has a single store, the value "path=<path>" enables automatic
failover to the provided path. After this setting is used, changing the
configuration to a new path or disabling requires providing the previous
path as ",prev_path=<path>". For example:

<PRE>

    --wal-failover=path=/mnt/data2
    --wal-failover=path=/mnt/data3,prev_path=/mnt/data2
    --wal-failover=disabled,prev_path=/mnt/data3

</PRE>

See the storage.wal_failover.unhealthy_op_threshold cluster setting.
`,
	}

	StorageEngine = FlagInfo{
		Name:        "storage-engine",
		Description: "Deprecated: only present for backward compatibility.",
	}

	SecondaryCache = FlagInfo{
		Name: "experimental-secondary-cache",
		Description: `
Enables the use of a secondary cache to store objects from shared storage (see
--experimental-shared-storage) inside local paths for each store. A size must
be specified with this flag, which will be the maximum size for the secondary
cache on each store on this node:
<PRE>

  --experimental-secondary-cache=20GiB

</PRE>
The size can be given in various ways:
<PRE>

  --experimental-secondary-cache=10000000000     -> 10000000000 bytes
  --experimental-secondary-cache=20GB            -> 20000000000 bytes
  --experimental-secondary-cache=20GiB           -> 21474836480 bytes
  --experimental-secondary-cache=20%             -> 20% of available space
  --experimental-secondary-cache=0.2             -> 20% of available space
  --experimental-secondary-cache=.2              -> 20% of available space</PRE>`,
	}

	SharedStorage = FlagInfo{
		Name: "experimental-shared-storage",
		Description: fmt.Sprintf(`
Shared storage URL (with a cloud scheme, eg. s3://, gcs://) to use for all stores
on this cockroach node. Cockroach can take advantage of this storage for faster
replication from node to node, as well as to grow beyond locally-available disk
space. The format of this URL is the same as that specified for bulk operations,
for more on that see:

<PRE>
%s
</PRE>

This is an experimental option, and must be specified on every start of this
node starting from the very first call to start. Passing this flag on an existing
initialized node is not supported.
`, docs.URL("use-cloud-storage-for-bulk-operations")),
	}

	Size = FlagInfo{
		Name:      "size",
		Shorthand: "z",
		Description: `
The Size to fill Store upto(using a ballast file):
Negative value means denotes amount of space that should be left after filling the disk.
If the Size is left unspecified, it defaults to 1GB.
<PRE>

  --size=20GiB

</PRE>
The size can be given in various ways:
<PRE>

  --size=10000000000     -> 10000000000 bytes
  --size=20GB            -> 20000000000 bytes
  --size=20GiB           -> 21474836480 bytes
  --size=0.02TiB         -> 21474836480 bytes
  --size=20%             -> 20% of available space
  --size=0.2             -> 20% of available space
  --size=.2              -> 20% of available space</PRE>`,
	}

	Verbose = FlagInfo{
		Name: "verbose",
		Description: `
Verbose output.`,
	}

	TempDir = FlagInfo{
		Name: "temp-dir",
		Description: `
The parent directory path where a temporary subdirectory will be created to be used for temporary files.
This path must exist or the node will not start.
The temporary subdirectory is used primarily as working memory for distributed computations
and CSV importing.
For example, the following will generate an arbitrary, temporary subdirectory
"/mnt/ssd01/temp/cockroach-temp<NUMBER>":
<PRE>

  --temp-dir=/mnt/ssd01/temp

</PRE>
If this flag is unspecified, the temporary subdirectory will be located under
the root of the first store.`,
	}

	ExternalIODir = FlagInfo{
		Name: "external-io-dir",
		Description: `
The local file path under which remotely-initiated operations that can specify
node-local I/O paths, such as BACKUP, RESTORE or IMPORT, can access files.
Following symlinks _is_ allowed, meaning that other paths can be added by
symlinking to them from within this path.
<PRE>

</PRE>
Note: operations in a distributed cluster can run across many nodes, so reading
or writing to any given node's local file system in a distributed cluster is not
usually useful unless that filesystem is actually backed by something like NFS.
<PRE>

</PRE>
If left empty, defaults to the "extern" subdirectory of the first store
directory.
<PRE>

</PRE>
The value "disabled" will disable all local file I/O.
`,
	}

	URL = FlagInfo{
		Name:   "url",
		EnvVar: "COCKROACH_URL",
		Description: `
Connection URL, of the form:
<PRE>
   postgresql://[user[:passwd]@]host[:port]/[db][?parameters...]
</PRE>
For example, postgresql://myuser@localhost:26257/mydb.
<PRE>

</PRE>
If left empty, the discrete connection flags are used: host, port,
user, database, insecure, certs-dir.`,
	}

	User = FlagInfo{
		Name:        "user",
		Shorthand:   "u",
		EnvVar:      "COCKROACH_USER",
		Description: `Database user name.`,
	}

	From = FlagInfo{
		Name: "from",
		Description: `
Start key and format as [<format>:]<key>. Supported formats: raw, hex, human,
rangeID. The raw format supports escaped text. For example, "raw:\x01k" is the
prefix for range local keys. The hex format takes an encoded MVCCKey.`,
	}

	To = FlagInfo{
		Name: "to",
		Description: `
Exclusive end key and format as [<format>:]<key>. Supported formats: raw, hex,
human, rangeID. The raw format supports escaped text. For example, "raw:\x01k"
is the prefix for range local keys. The hex format takes an encoded MVCCKey.`}

	Limit = FlagInfo{
		Name:        "limit",
		Description: `Maximum number of keys to return.`,
	}

	Values = FlagInfo{
		Name:        "values",
		Description: `Print values along with their associated key.`,
	}

	Sizes = FlagInfo{
		Name:        "sizes",
		Description: `Print key and value sizes along with their associated key.`,
	}

	Replicated = FlagInfo{
		Name:        "replicated",
		Description: "Restrict scan to replicated data.",
	}

	GossipInputFile = FlagInfo{
		Name:      "file",
		Shorthand: "f",
		Description: `
File containing the JSON output from a node's /_status/gossip/ endpoint.
If specified, takes priority over host/port flags.`,
	}

	PrintSystemConfig = FlagInfo{
		Name: "print-system-config",
		Description: `
If specified, print the system config contents. Beware that the output will be
long and not particularly human-readable.`,
	}

	DecodeAsTable = FlagInfo{
		Name: "decode-as-table",
		Description: `
Base64-encoded Descriptor to use as the table when decoding KVs.`,
	}

	FilterKeys = FlagInfo{
		Name: "type",
		Description: `
Only show certain types of keys: values, intents, txns, or rangekeys. If
omitted, all key types are shown. txns will also implicitly limit the key range
to local keys, unless specified`,
	}

	DrainWait = FlagInfo{
		Name: "drain-wait",
		Description: `
When non-zero, wait for at most the specified amount of time for the node to
drain all active client connections and migrate away range leases.
If zero, the command waits until the last client has disconnected and
all range leases have been migrated away.`,
	}

	Wait = FlagInfo{
		Name: "wait",
		Description: `
Specifies when to return during the decommissioning process.
Takes any of the following values:
<PRE>

  - all   waits until all target nodes' replica counts have dropped to zero and
          marks the nodes as fully decommissioned. This is the default.
  - none  marks the targets as decommissioning, but does not wait for the
          replica counts to drop to zero before returning. If the replica counts
          are found to be zero, nodes are marked as fully decommissioned. Use
          when polling manually from an external system.
</PRE>`,
	}

	Timeout = FlagInfo{
		Name: "timeout",
		Description: `
If nonzero, return with an error if the operation does not conclude within the
specified timeout. The timeout is specified with a suffix of 's' for seconds,
'm' for minutes, and 'h' for hours.
`,
	}

	NodeRanges = FlagInfo{
		Name:        "ranges",
		Description: `Show node details for ranges and replicas.`,
	}

	NodeStats = FlagInfo{
		Name:        "stats",
		Description: `Show node disk usage details.`,
	}

	NodeAll = FlagInfo{
		Name: "all", Description: `Show all node details.
When no node ID is specified, also lists all nodes that have been decommissioned
in the history of the cluster.`,
	}

	NodeDecommission = FlagInfo{
		Name: "decommission", Description: `Show node decommissioning details.
When no node ID is specified, also lists all nodes that have been decommissioned
in the history of the cluster.`,
	}

	NodeDecommissionSelf = FlagInfo{
		Name: "self",
		Description: `Use the node ID of the node connected to via --host
as target of the decommissioning or recommissioning command.`,
	}

	NodeDecommissionChecks = FlagInfo{
		Name: "checks",
		Description: `
Specifies how to evaluate readiness checks prior to node decommission.
Takes any of the following values:
<PRE>

  - enabled  evaluate readiness prior to starting node decommission.
  - strict   use strict readiness evaluation mode prior to node decommission.
  - skip     skip readiness checks and immediately request node decommission.
             Use when rerunning node decommission.
</PRE>`,
	}

	NodeDecommissionDryRun = FlagInfo{
		Name: "dry-run",
		Description: `Only evaluate decommission readiness and check decommission
status, without actually decommissioning the node.`,
	}

	NodeDrainSelf = FlagInfo{
		Name: "self",
		Description: `Use the node ID of the node connected to via --host
as target of the drain command.`,
	}

	NodeDrainShutdown = FlagInfo{
		Name:        "shutdown",
		Description: `Shutdown the target node after it is drained.`,
	}

	SQLFmtLen = FlagInfo{
		Name: "print-width",
		Description: `
The line length where sqlfmt will try to wrap.`,
	}

	SQLFmtSpaces = FlagInfo{
		Name:        "use-spaces",
		Description: `Indent with spaces instead of tabs.`,
	}

	SQLFmtTabWidth = FlagInfo{
		Name:        "tab-width",
		Description: `Number of spaces per indentation level.`,
	}

	SQLFmtNoSimplify = FlagInfo{
		Name:        "no-simplify",
		Description: `Don't simplify output.`,
	}

	SQLFmtAlign = FlagInfo{
		Name:        "align",
		Description: `Align the output.`,
	}

	DemoSQLPort = FlagInfo{
		Name: "sql-port",
		Description: `First port number for SQL servers.
There should be as many TCP ports available as the value of --nodes
starting at the specified value; for multitenant demo clusters, the
number of required ports is twice the value of --nodes.`,
	}

	DemoHTTPPort = FlagInfo{
		Name: "http-port",
		Description: `First port number for HTTP servers.
There should be as many TCP ports available as the value of --nodes
starting at the specified value; for multitenant demo clusters, the
number of required ports is twice the value of --nodes.`,
	}

	DemoNodes = FlagInfo{
		Name:        "nodes",
		Description: `How many in-memory nodes to create for the demo.`,
	}

	DemoNodeSQLMemSize = FlagInfo{
		Name: "max-sql-memory",
		Description: `
Maximum memory capacity available for each node to store temporary data for SQL
clients, including prepared queries and intermediate data rows during query
execution. Accepts numbers interpreted as bytes, size suffixes (e.g. 1GB and
1GiB) or a percentage of physical memory (e.g. .25). If left unspecified,
defaults to 256MiB.
`,
	}
	DemoNodeCacheSize = FlagInfo{
		Name: "cache",
		Description: `
Total size in bytes for caches per node, shared evenly if there are multiple
storage devices. Size suffixes are supported (e.g. 1GB and 1GiB).
If left unspecified, defaults to 64MiB. A percentage of physical memory
can also be specified (e.g. .25).`,
	}

	RunDemoWorkload = FlagInfo{
		Name:        "with-load",
		Description: `Run a demo workload against the pre-loaded database.`,
	}

	ExpandDemoSchema = FlagInfo{
		Name:        "expand-schema",
		Description: `Expand the workload schema up to the specified size.`,
	}

	DemoNameGenOpts = FlagInfo{
		Name:        "name-gen-options",
		Description: `Use the specified options for the name generation during schema expansion (JSON syntax).`,
	}

	DemoWorkloadMaxQPS = FlagInfo{
		Name:        "workload-max-qps",
		Description: "The maximum QPS when a workload is running.",
	}

	DemoNodeLocality = FlagInfo{
		Name: "demo-locality",
		Description: `
Locality information for each demo node. The input is a colon separated
list of localities for each node. The i'th locality in the colon separated
list sets the locality for the i'th demo cockroach node. For example:
<PRE>

--demo-locality=region=us-east1,az=1:region=us-east1,az=2:region=us-east1,az=3

</PRE>
Assigns node1's region to us-east1 and availability zone to 1, node 2's region
to us-east1 and availability zone to 2, and node 3's region to us-east1 and
availability zone to 3.
`,
	}

	DemoGeoPartitionedReplicas = FlagInfo{
		Name: "geo-partitioned-replicas",
		Description: fmt.Sprintf(`
When used with the Movr dataset, create a 9 node cluster and automatically apply
the geo-partitioned replicas topology across 3 virtual regions named us-east1,
us-west1, and europe-west1. This command will fail with an error if an
enterprise license could not be acquired, or if the Movr dataset is not used.
More information about the geo-partitioned replicas topology can be found at:
<PRE>

%s
</PRE>
		`, docs.URL("topology-geo-partitioned-replicas.html")),
	}

	DemoMultitenant = FlagInfo{
		Name: "multitenant",
		Description: `
If set, cockroach demo will start separate in-memory KV and SQL servers in multi-tenancy mode.
The SQL shell will be connected to the first tenant, and can be switched between tenants
and the system tenant using the \connect command.`,
	}

	DemoDisableServerController = FlagInfo{
		Name: "disable-server-controller",
		Description: `
If set, the server controller will not be used to start secondary
tenant servers.`,
	}

	DemoNoLicense = FlagInfo{
		Name: "disable-demo-license",
		Description: `
If set, disable enterprise features.`,
	}

	DemoEnableRangefeeds = FlagInfo{
		Name: "auto-enable-rangefeeds",
		Description: `
If set to false, overrides the default demo behavior of enabling rangefeeds.`,
	}

	UseEmptyDatabase = FlagInfo{
		Name:        "empty",
		Description: `Deprecated in favor of --no-example-database`,
	}

	NoExampleDatabase = FlagInfo{
		Name:   "no-example-database",
		EnvVar: "COCKROACH_NO_EXAMPLE_DATABASE",
		Description: `
Disable the creation of a default dataset in the demo shell.
This makes 'cockroach demo' faster to start.`,
	}

	GeoLibsDir = FlagInfo{
		Name: "spatial-libs",
		Description: `
The location where all libraries for spatial operations is located.`,
	}

	Global = FlagInfo{
		Name: "global",
		Description: `
Simulate a global cluster. This adds artificial latencies to nodes in different
regions. This flag only works with the default node localities. This setting is experimental.`,
	}

	WriteSize = FlagInfo{
		Name: "write-size",
		Description: `
Size of blocks to write to storage device.`,
	}

	SyncInterval = FlagInfo{
		Name: "sync-interval",
		Description: `
Number of bytes to write before running fsync.`,
	}

	BenchConcurrency = FlagInfo{
		Name: "concurrency",
		Description: `
Number of workers for benchmarking.`,
	}

	BenchDuration = FlagInfo{
		Name: "duration",
		Description: `
Amount of time to run workers.`,
	}

	BenchServer = FlagInfo{
		Name: "server",
		Description: `
Run as server for network benchmark.`,
	}

	BenchPort = FlagInfo{
		Name: "port",
		Description: `
Port for network benchmark.`,
	}

	BenchAddresses = FlagInfo{
		Name: "addresses",
		Description: `
Addresses for network benchmark.`,
	}
	BenchLatency = FlagInfo{
		Name: "latency",
		Description: `
Latency or throughput mode.`,
	}

	ZipNodes = FlagInfo{
		Name: "nodes",
		Description: `
List of nodes to include. Can be specified as a comma-delimited
list of node IDs or ranges of node IDs, for example: 5,10-20,23.
The default is to include all nodes.`,
	}

	ZipExcludeNodes = FlagInfo{
		Name: "exclude-nodes",
		Description: `
List of nodes to exclude. Can be specified as a comma-delimited
list of node IDs or ranges of node IDs, for example: 5,10-20,23.
The default is to not exclude any node.`,
	}

	ZipIncludedFiles = FlagInfo{
		Name: "include-files",
		Description: `
List of glob patterns that determine files that can be included
in the output. The list can be specified as a comma-delimited
list of patterns, or by using the flag multiple times.
The patterns apply to the base name of the file, without
a path prefix.
The default is to include all files.
<PRE>

</PRE>
This flag is applied before --exclude-files; for example,
including '*.log' and then excluding '*foo*.log' will
exclude 'barfoos.log'.
<PRE>

</PRE>
You can use the 'debug list-files' command to explore how
this flag is applied.`,
	}

	ZipExcludedFiles = FlagInfo{
		Name: "exclude-files",
		Description: `
List of glob patterns that determine files that are to
be excluded from the output. The list can be specified
as a comma-delimited list of patterns, or by using the
flag multiple times.
The patterns apply to the base name of the file, without
a path prefix.
<PRE>

</PRE>
This flag is applied after --include-files; for example,
including '*.log' and then excluding '*foo*.log' will
exclude 'barfoos.log'.
<PRE>

</PRE>
You can use the 'debug list-files' command to explore how
this flag is applied.`,
	}

	ZipFilesFrom = FlagInfo{
		Name: "files-from",
		Description: `
Limit file collection to those files modified after the
specified timestamp, inclusive.
The timestamp can be expressed as YYYY-MM-DD,
YYYY-MM-DD HH:MM or YYYY-MM-DD HH:MM:SS and is interpreted
in the UTC time zone.
The default value for this flag is 48 hours before now.
<PRE>

</PRE>
When customizing this flag to capture a narrow range
of time, consider adding extra seconds/minutes
to the range to accommodate clock drift and uncertainties.
<PRE>

</PRE>
You can use the 'debug list-files' command to explore how
this flag is applied.`,
	}

	ZipFilesUntil = FlagInfo{
		Name: "files-until",
		Description: `
Limit file collection to those files created before the
specified timestamp, inclusive.
The timestamp can be expressed as YYYY-MM-DD,
YYYY-MM-DD HH:MM or YYYY-MM-DD HH:MM:SS and is interpreted
in the UTC time zone.
The default value for this flag is some time beyond
the current time, to ensure files created during
the collection are also included.
<PRE>

</PRE>
When customizing this flag to capture a narrow range
of time, consider adding extra seconds/minutes
to the range to accommodate clock drift and uncertainties.
<PRE>

</PRE>
You can use the 'debug list-files' command to explore how
this flag is applied.`,
	}

	ZipRedactLogs = FlagInfo{
		Name: "redact-logs",
		Description: `
DEPRECATED: Redact text that may contain confidential data or PII from
retrieved log entries.
<PRE>

</PRE>
Note that this flag is being deprecated in favor of the --redact flag.
Setting this flag will be interpreted in the same way as setting the
--redact flag.
`,
	}

	ZipRedact = FlagInfo{
		Name: "redact",
		Description: `
Redact anything that may contain confidential data or PII from retrieved
debug data. An exception is made for range key data, as this data is
necessary to support CockroachDB.
`,
	}

	ZipIncludeRangeInfo = FlagInfo{
		Name: "include-range-info",
		Description: `
Include one file per node with information about the KV ranges stored on that node,
in nodes/{node ID}/ranges.json. Additionally, include problem ranges information.
This information can be vital when debugging issues that involve the KV storage layer,
such as data placement, load balancing, performance or other behaviors. In certain situations,
on large clusters with large numbers of ranges, these files can be omitted if and only if the
issue being investigated is already known to be in another layer of the system (for example,
an error message about an unsupported feature or incompatible value in a SQL schema change or
statement). Note however many higher-level issues are ultimately related to the underlying KV
storage layer described by these files so only set this to false if directed to do so by Cockroach
Labs support.
`,
	}

	ZipIncludeGoroutineStacks = FlagInfo{
		Name: "include-goroutine-stacks",
		Description: `
Fetch stack traces for all goroutines running on each targeted node in nodes/*/stacks.txt
and nodes/*/stacks_with_labels.txt files. Note that fetching stack traces for all goroutines is
a "stop-the-world" operation, which can momentarily have negative impacts on SQL service
latency. Note that any periodic goroutine dumps previously taken on the node will still be
included in nodes/*/goroutines/*.txt.gz, as these would have already been generated and don't
require any additional stop-the-world operations to be collected.
`,
	}

	ZipIncludeRunningJobTraces = FlagInfo{
		Name: "include-running-job-traces",
		Description: `
Include information about each running, traceable job in jobs/*/*/trace.zip
files. This involves collecting cluster-wide traces for each running job in the
cluster.
`,
	}

	ZipCPUProfileDuration = FlagInfo{
		Name: "cpu-profile-duration",
		Description: `
Fetch CPU profiles from the cluster with the specified sample duration.
The zip command will block for the duration specified. Zero disables this feature.
`,
	}

	ZipConcurrency = FlagInfo{
		Name: "concurrency",
		Description: `
The maximum number of nodes to request data from simultaneously.
Can be set to 1 to ensure only one node is polled for data at a time.
`,
	}

	StmtDiagDeleteAll = FlagInfo{
		Name:        "all",
		Description: `Delete all bundles.`,
	}

	StmtDiagCancelAll = FlagInfo{
		Name:        "all",
		Description: `Cancel all outstanding requests.`,
	}

	ImportSkipForeignKeys = FlagInfo{
		Name: "skip-foreign-keys",
		Description: `
Speed up data import by ignoring foreign key constraints in the dump file's DDL.
Also enables importing individual tables that would otherwise fail due to
dependencies on other tables.
`,
	}

	ImportMaxRowSize = FlagInfo{
		Name: "max-row-size",
		Description: `
Override limits on line size when importing Postgres dump files. This setting
may need to be tweaked if the Postgres dump file has extremely long lines.
`,
	}

	ImportIgnoreUnsupportedStatements = FlagInfo{
		Name: "ignore-unsupported-statements",
		Description: `
Ignore statements that are unsupported during an import from a PGDUMP file.
`,
	}

	ImportLogIgnoredStatements = FlagInfo{
		Name: "log-ignored-statements",
		Description: `
Log unsupported statements that are ignored during an import from a PGDUMP file to the specified
destination. This flag should be used in conjunction with the ignore-unsupported-statements flag
that ignores the unsupported statements during an import.
`,
	}

	ImportRowLimit = FlagInfo{
		Name: "row-limit",
		Description: `
Specify the number of rows that will be imported for each table during a PGDUMP or MYSQLDUMP import.
This can be used to check schema and data correctness without running the entire import.
`,
	}

	Log = FlagInfo{
		Name: "log",
		Description: `Logging configuration, expressed using YAML syntax.
For example, you can change the default logging directory with:
--log='file-defaults: {dir: ...}'.
See the documentation for more options and details.

To preview how the log configuration is applied, or preview the
default configuration, you can use the 'cockroach debug check-log-config' sub-command.
`,
	}

	LogConfigFile = FlagInfo{
		Name: "log-config-file",
		Description: `File name to read the logging configuration from.
This has the same effect as passing the content of the file via
the --log flag.`,
	}

	LogConfigVars = FlagInfo{
		Name: "log-config-vars",
		Description: `Environment variables that will be expanded if
present in the body of the logging configuration.`,
	}

	StderrThresholdOverride = FlagInfo{
		Name: "logtostderr",
		Description: `--logtostderr=XXX is an alias for --log='sinks: {stderr: {filter: XXX}}'.
If no value is specified, the default value for the command is inferred: INFO for server
commands, WARNING for client commands.`,
	}

	FileThresholdOverride = FlagInfo{
		Name:        "log-file-verbosity",
		Description: `--log-file-verbosity=XXX is an alias for --log='file-defaults: {filter: XXX}}'.`,
	}

	StderrNoColorOverride = FlagInfo{
		Name:        "no-color",
		Description: `--no-color=XXX is an alias for --log='sinks: {stderr: {no-color: XXX}}'.`,
	}

	RedactableLogsOverride = FlagInfo{
		Name:        "redactable-logs",
		Description: `--redactable-logs=XXX is an alias for --log='file-defaults: {redactable: XXX}}'.`,
	}

	LogFileMaxSizeOverride = FlagInfo{
		Name:        "log-file-max-size",
		Description: "--log-file-max-size=XXX is an alias for --log='file-defaults: {max-file-size: XXX}'.",
	}

	LogGroupMaxSizeOverride = FlagInfo{
		Name:        "log-group-max-size",
		Description: `--log-group-max-size=XXX is an alias for --log='file-defaults: {max-group-size: XXX}'.`,
	}

	LogDirOverride = FlagInfo{
		Name:        "log-dir",
		Description: `--log-dir=XXX is an alias for --log='file-defaults: {dir: XXX}'.`,
	}

	SQLAuditLogDirOverride = FlagInfo{
		Name:        "sql-audit-dir",
		Description: `--sql-audit-dir=XXX is an alias for --log='sinks: {file-groups: {sql-audit: {channels: SENSITIVE_ACCESS, dir: ...}}}'.`,
	}

	BuildTag = FlagInfo{
		Name: "build-tag",
		Description: `
When set, the command prints only the build tag for the executable,
without any other details.
`,
	}

	ExportTableTarget = FlagInfo{
		Name:        "table",
		Description: `Select the table to export data from.`,
	}

	ExportDestination = FlagInfo{
		Name: "destination",
		Description: `
The destination to export data.
If the export format is readable and this flag left unspecified,
defaults to display the exported data in the terminal output.
`,
	}

	ExportTableFormat = FlagInfo{
		Name: "format",
		Description: `
Selects the format to export table rows from backups.
Only csv is supported at the moment.
`,
	}

	ExportCSVNullas = FlagInfo{
		Name:        "nullas",
		Description: `The string that should be used to represent NULL values.`,
	}

	StartKey = FlagInfo{
		Name: "start-key",
		Description: `
Start key and format as [<format>:]<key>. Supported formats: raw, hex, bytekey.
The raw format supports escaped text. For example, "raw:\x01k" is
the prefix for range local keys.
The bytekey format does not require table-key prefix.`,
	}

	MaxRows = FlagInfo{
		Name:        "max-rows",
		Description: `Maximum number of rows to return (Default 0 is unlimited).`,
	}

	ExportRevisions = FlagInfo{
		Name:        "with-revisions",
		Description: `Export revisions of data from a backup table since the last schema change.`,
	}

	ExportRevisionsUpTo = FlagInfo{
		Name:        "up-to",
		Description: `Export revisions of data from a backup table up to a specific timestamp.`,
	}

	Recursive = FlagInfo{
		Name:      "recursive",
		Shorthand: "r",
		Description: `
When set, the entire subtree rooted at the source directory will be uploaded to
the destination. Every file in the subtree will be uploaded to the corresponding
path under the destination; i.e. the relative path will be maintained. À la
rsync, a trailing slash in the source will avoid creating an additional
directory level under the destination. The destination can be expressed one of
four ways: empty (not specified), a relative path, a well-formed URI with no
host, or a full well-formed URI.
<PRE>

</PRE>
If a destination is not specified, the default URI scheme and host will be used,
and the basename from the source will be used as the destination directory.
For example: 'userfile://defaultdb.public.userfiles_root/yourdirectory'
<PRE>

</PRE>
If the destination is a relative path such as 'path/to/dir', the default
userfile URI schema and host will be used
('userfile://defaultdb.public.userfiles_$user/'), and the relative path will be
appended to it.
For example: 'userfile://defaultdb.public.userfiles_root/path/to/dir'
<PRE>

</PRE>
If the destination is a well-formed URI with no host, such as
'userfile:///path/to/dir/', the default userfile URI schema and host will be
used ('userfile://defaultdb.public.userfiles_$user/').
For example: 'userfile://defaultdb.public.userfiles_root/path/to/dir'
<PRE>

</PRE>
If the destination is a full well-formed URI, such as
'userfile://db.schema.tablename_prefix/path/to/dir', then it will be used
verbatim.
For example: 'userfile://foo.bar.baz_root/path/to/dir'
`,
	}

	RecoverStore = FlagInfo{
		Name:      "store",
		Shorthand: "s",
		Description: `
The file path to a storage device. This flag must be specified separately for
each storage device.
<PRE>

  --store=/mnt/ssd01 --store=/mnt/ssd02 --store=/mnt/hda1

</PRE>
Flag is syntactically identical to --store flag of start command, but only path
part is used. This is done to make flags interoperable between start and recover
commands.

See start --help for more flag details and examples.
`,
	}

	ConfirmActions = FlagInfo{
		Name: "confirm",
		Description: `
Confirm action:
<PRE>
y - assume yes to all prompts
n - assume no/abort to all prompts
p - prompt interactively for a confirmation
</PRE>
`,
	}

	RecoverIgnoreInternalVersion = FlagInfo{
		Name: "ignore-internal-version",
		Description: `
When set, staging and local store plan application commands will ignore internal
cluster version. This option must only be used to bypass version check if
cluster is stuck in the middle of upgrade and locally stored versions differ
from node to node and previous application or staging attempt failed.
`,
	}

	PrintKeyLength = FlagInfo{
		Name: "print-key-max-length",
		Description: `
Maximum number of characters in printed keys and spans. If key representation
exceeds this value, it is truncated. Set to 0 to disable truncation.`,
	}
	// Attrs and others store the static information for CLI flags.

	EnterpriseEncryption = FlagInfo{
		Name: "enterprise-encryption",
		Description: `
<PRE>Specify encryption options for one of the stores on a node. If multiple
stores exist, the flag must be specified for each store.

A valid enterprise license is required to use this functionality.

Key files should be generated by "cockroach gen encryption-key".

Valid fields:

* path    (required): must match the path of one of the stores, or the special
                      value "*" to match all stores
* key     (required): path to the current key file, or "plain"
* old-key (required): path to the previous key file, or "plain"
* rotation-period   : amount of time after which data keys should be rotated

</PRE>
example:
<PRE>
  --enterprise-encryption=path=cockroach-data,key=/keys/aes-128.key,old-key=plain</PRE>
`,
	}
)
