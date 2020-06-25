// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cliflags

import (
	"bytes"
	"fmt"
	"strings"

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

const usageIndentation = 8
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
	s := "\n" + wrapDescription(f.Description) + "\n"
	if f.EnvVar != "" {
		// Check that the environment variable name matches the flag name. Note: we
		// don't want to automatically generate the name so that grepping for a flag
		// name in the code yields the flag definition.
		correctName := "COCKROACH_" + strings.ToUpper(strings.Replace(f.Name, "-", "_", -1))
		if f.EnvVar != correctName {
			panic(fmt.Sprintf("incorrect EnvVar %s for flag %s (should be %s)",
				f.EnvVar, f.Name, correctName))
		}
		s = s + "Environment variable: " + f.EnvVar + "\n"
	}
	// github.com/spf13/pflag appends the default value after the usage text. Add
	// the correct indentation (7 spaces) here. This is admittedly fragile.
	return text.Indent(s, strings.Repeat(" ", usageIndentation)) +
		strings.Repeat(" ", usageIndentation-1)
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

  --attrs=x16c:gpu`,
	}

	Locality = FlagInfo{
		Name: "locality",
		Description: `
An ordered, comma-separated list of key-value pairs that describe the topography
of the machine. Topography might include country, datacenter or rack
designations. Data is automatically replicated to maximize diversities of each
tier. The order of tiers is used to determine the priority of the diversity, so
the more inclusive localities like country should come before less inclusive
localities like datacenter. The tiers and order must be the same on all nodes.
Including more tiers is better than including fewer. For example:
<PRE>

  --locality=country=us,region=us-west,datacenter=us-west-1b,rack=12
  --locality=country=ca,region=ca-east,datacenter=ca-east-2,rack=4

  --locality=planet=earth,province=manitoba,colo=secondary,power=3`,
	}

	Background = FlagInfo{
		Name: "background",
		Description: `
Start the server in the background. This is similar to appending "&"
to the command line, but when the server is started with --background,
control is not returned to the shell until the server is ready to
accept requests.`,
	}

	SQLMem = FlagInfo{
		Name: "max-sql-memory",
		Description: `
Maximum memory capacity available to store temporary data for SQL clients,
including prepared queries and intermediate data rows during query execution.
Accepts numbers interpreted as bytes, size suffixes (e.g. 1GB and 1GiB) or a
percentage of physical memory (e.g. .25).
If left unspecified, defaults to 25% of physical memory.
`,
	}

	SQLAuditLogDirName = FlagInfo{
		Name: "sql-audit-dir",
		Description: `
If non-empty, create a SQL audit log in this drectory.
`,
	}

	SQLTempStorage = FlagInfo{
		Name: "max-disk-temp-storage",
		Description: `
Maximum storage capacity available to store temporary disk-based data for SQL
queries that exceed the memory budget (e.g. join, sorts, etc are sometimes able
to spill intermediate results to disk).
Accepts numbers interpreted as bytes, size suffixes (e.g. 32GB and 32GiB) or a
percentage of disk size (e.g. 10%).
If left unspecified, defaults to 32GiB.

The location of the temporary files is within the first store dir (see --store).
If expressed as a percentage, --max-disk-temp-storage is interpreted relative to
the size of the storage device on which the first store is placed. The temp
space usage is never counted towards any store usage (although it does share the
device with the first store) so, when configuring this, make sure that the size
of this temp storage plus the size of the first store don't exceed the capacity
of the storage device.
If the first store is an in-memory one (i.e. type=mem), then this temporary "disk"
data is also kept in-memory. A percentage value is interpreted as a percentage
of the available internal memory. If not specified, the default shifts to 100MiB
when the first store is in-memory.
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

	DumpTime = FlagInfo{
		Name: "as-of",
		Description: `
Dumps the data as of the specified timestamp. Formats supported are the same
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
results of each SQL statement are printed on the standard output.`,
	}

	Watch = FlagInfo{
		Name: "watch",
		Description: `
Repeat the SQL statement(s) specified with --execute
with the specified period. The client will stop watching
if an execution of the SQL statement(s) fail.`,
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

	SafeUpdates = FlagInfo{
		Name: "safe-updates",
		Description: `
Disable SQL statements that may have undesired side effects. For
example a DELETE or UPDATE without a WHERE clause. By default, this
setting is enabled (true) and such statements are rejected to prevent
accidents. This can also be overridden in a session with SET
sql_safe_updates = FALSE.`,
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
Selects how to display table rows in results. Possible values: tsv,
csv, table, records, sql, raw, html. If left unspecified, defaults to
tsv for non-interactive sessions and table for interactive sessions.`,
	}

	ClusterName = FlagInfo{
		Name: "cluster-name",
		Description: `
Sets a name to verify the identity of a remote node or cluster. The
value must match between this node and the remote node(s) specified
via --join.

This can be used as an additional verification when either the node or
cluster, or both, have not yet been initialized and do not yet know
their cluster ID.

To introduce a cluster name into an already-initialized cluster, pair
this flag with --disable-cluster-name-verification.`,
	}

	DisableClusterNameVerification = FlagInfo{
		Name: "disable-cluster-name-verification",
		Description: `
Tell the server to ignore cluster name mismatches. This is meant for
use when opting an existing cluster into starting to use cluster name
verification, or when changing the cluster name.

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

  --join=localhost:1234,localhost:2345 --join=localhost:3456

</PRE>`,
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
Unless --sql-addr is also specified, this address is also
used to accept SQL client connections.
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
NAT is present between cluster nodes.`,
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
If left unspecified, the address specified by --listen-addr will be
used for both RPC and SQL connections.
<PRE>

</PRE>
If specified but the address part is omitted, the address part
defaults to the address part of --listen-addr.
If specified but the port number is omitted, the port
number defaults to 26257.
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
example [::1]:26257 or [fe80::f6f2:::]:26257.`,
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

	UnencryptedLocalhostHTTP = FlagInfo{
		Name: "unencrypted-localhost-http",
		Description: `
When specified, restricts HTTP connections to localhost-only and disables
TLS for the HTTP interface. The hostname part of --http-addr, if specified,
is then ignored. This flag is intended for use to facilitate
local testing without requiring certificate setups in web browsers.`,
	}

	LocalityAdvertiseAddr = FlagInfo{
		Name: "locality-advertise-addr",
		Description: `
List of ports to advertise to other CockroachDB nodes for intra-cluster
communication for some locality. This should be specified as a commma
separated list of locality@address. Addresses can also include ports.
For example:
<PRE>
"region=us-west@127.0.0.1,datacenter=us-west-1b@127.0.0.1"
"region=us-west@127.0.0.1:26257,datacenter=us-west-1b@127.0.0.1:26258"
</PRE>
`,
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

PostgreSQL clients only take a port number and directory as input
and construct the socket name programmatically.

To use, for example: psql -h /path/to -p NNNN ...
`,
	}

	ClientInsecure = FlagInfo{
		Name:   "insecure",
		EnvVar: "COCKROACH_INSECURE",
		Description: `
Connect to an insecure cluster. This is strongly discouraged for
production usage.`,
	}

	ServerInsecure = FlagInfo{
		Name: "insecure",
		Description: `
Start an insecure node, using unencrypted (non-TLS) connections,
listening on all IP addresses (unless --listen-addr is provided) and
disabling password authentication for all database users. This is
strongly discouraged for production usage and should never be used on
a public network without combining it with --listen-addr.`,
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

	GeneratePKCS8Key = FlagInfo{
		Name:        "also-generate-pkcs8-key",
		Description: `Also write the key in pkcs8 format to <certs-dir>/client.<username>.key.pk8.`,
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
fields.
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
Override HLC to use PTP hardware clock user space API when querying for current time.
The value corresponds to the clock device to be used. This is currently only tested
and supported on Linux.
<PRE>

  --clock-device=/dev/ptp0

</PRE>
`,
	}

	MaxOffset = FlagInfo{
		Name: "max-offset",
		Description: `
Maximum allowed clock offset for the cluster. If observed clock offsets exceed
this limit, servers will crash to minimize the likelihood of reading
inconsistent data. Increasing this value will increase the time to recovery of
failures as well as the frequency of uncertainty-based read restarts.
<PRE>

</PRE>
Note that this value must be the same on all nodes in the cluster. In order to
change it, all nodes in the cluster must be stopped simultaneously and restarted
with the new value.`,
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
Commas are forbidden in all values, since they are used to separate fields.
Also, if you use equal signs in the file path to a store, you must use the
"path" field label.`,
	}

	StorageEngine = FlagInfo{
		Name: "storage-engine",
		Description: `
Storage engine to use for all stores on this cockroach node. Options are default,
rocksdb, or pebble.

If default is specified, the storage engine last used to write to the first
store directory is used (see --store). If the store directory is uninitialized
and default is specified, rocksdb is used as the default storage engine.`,
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
  --size=.2              -> 20% of available space

</PRE>`,
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

Note: operations in a distributed cluster can run across many nodes, so reading
or writing to any given node's local file system in a distributed cluster is not
usually useful unless that filesystem is actually backed by something like NFS.

If left empty, defaults to the "extern" subdirectory of the first store directory.

The value "disabled" will disable all local file I/O. `,
	}

	URL = FlagInfo{
		Name:   "url",
		EnvVar: "COCKROACH_URL",
		Description: `
Connection URL, e.g. "postgresql://myuser@localhost:26257/mydb".
If left empty, the connection flags are used (host, port, user,
database, insecure, certs-dir).`,
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

	DrainWait = FlagInfo{
		Name: "drain-wait",
		Description: `
When non-zero, wait for the specified amount of time for the node to
drain all active client connections and migrate away range leases.`,
	}

	Wait = FlagInfo{
		Name: "wait",
		Description: `
Specifies when to return after having marked the targets as decommissioning.
Takes any of the following values:
<PRE>

  - all:  waits until all target nodes' replica counts have dropped to zero.
    This is the default.
  - none: marks the targets as decommissioning, but does not wait for the process to complete.
    Use when polling manually from an external system.

</PRE>`,
	}

	Timeout = FlagInfo{
		Name: "timeout",
		Description: `
		If nonzero, return with an error if the operation does not conclude within the specified timeout.
		The timeout is specified with a suffix of 's' for seconds, 'm' for minutes, and 'h' for hours.`,
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

	DemoNodes = FlagInfo{
		Name:        "nodes",
		Description: `How many in-memory nodes to create for the demo.`,
	}

	DemoNodeSQLMemSize = FlagInfo{
		Name: "max-sql-memory",
		Description: `
Maximum memory capacity available for each node to store temporary data for SQL clients,
including prepared queries and intermediate data rows during query execution.
Accepts numbers interpreted as bytes, size suffixes (e.g. 1GB and 1GiB) or a
percentage of physical memory (e.g. .25).
If left unspecified, defaults to 128MiB.
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

	DemoNodeLocality = FlagInfo{
		Name: "demo-locality",
		Description: `
Locality information for each demo node. The input is a colon separated
list of localities for each node. The i'th locality in the colon separated
list sets the locality for the i'th demo cockroach node. For example:
<PRE>

--demo-locality=region=us-east1,az=1:region=us-east1,az=2:region=us-east1,az=3

Assigns node1's region to us-east1 and availability zone to 1, node 2's
region to us-east1 and availability zone to 2, and node 3's region
to us-east1 and availability zone to 3.
`,
	}

	DemoGeoPartitionedReplicas = FlagInfo{
		Name: "geo-partitioned-replicas",
		Description: `
When used with the Movr dataset, create a 9 node cluster and automatically apply
the geo-partitioned replicas topology across 3 virtual regions named us-east1, us-west1, and
europe-west1. This command will fail with an error if an enterprise license could not
be acquired, or if the Movr dataset is not used. More information about the geo-partitioned 
replicas topology can be found at this URL: 
https://www.cockroachlabs.com/docs/v19.1/topology-geo-partitioned-replicas.html
		`,
	}

	DemoNoLicense = FlagInfo{
		Name: "disable-demo-license",
		Description: `
If set, disable cockroach demo from attempting to obtain a temporary license.`,
	}

	UseEmptyDatabase = FlagInfo{
		Name: "empty",
		Description: `
Start with an empty database: avoid pre-loading a default dataset in
the demo shell.`,
	}

	GeoLibsDir = FlagInfo{
		Name: "geo-libs",
		Description: `
The location where all libraries for Geospatial operations is located.`,
	}

	Global = FlagInfo{
		Name: "global",
		Description: `
Simulate a global cluster. This adds artificial latencies to nodes in different
regions. This flag only works with the default node localities. This setting is experimental.`,
	}

	LogDir = FlagInfo{
		Name: "log-dir",
		Description: `
If non-empty, write log files in this directory. If empty, write log files to
<store-dir>/logs where <store-dir> is the directory of the first on disk store.
`,
	}

	LogDirMaxSize = FlagInfo{
		Name: "log-group-max-size",
		Description: `
Maximum combined size of all log files in a logging group.
`,
	}

	LogFileMaxSize = FlagInfo{
		Name: "log-file-max-size",
		Description: `
Maximum size of each log file.
`,
	}

	LogFileVerbosity = FlagInfo{
		Name: "log-file-verbosity",
		Description: `
Minimum verbosity of messages written to the log file.
`,
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

	ZipRedactLogs = FlagInfo{
		Name: "redact-logs",
		Description: `
Redact text that may contain confidential data or PII from retrieved
log entries. Note that this flag only operates on log entries;
other items retrieved by the zip command may still consider
confidential data or PII.
`,
	}
)
