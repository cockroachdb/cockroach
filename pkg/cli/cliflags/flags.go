// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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

	ZoneConfig = FlagInfo{
		Name:      "file",
		Shorthand: "f",
		Description: `
File to read the zone configuration from. Specify "-" to read from standard input.`,
	}

	ZoneDisableReplication = FlagInfo{
		Name: "disable-replication",
		Description: `
Disable replication in the zone by setting the desired replica count to 1.
Equivalent to setting 'num_replicas: 1' via -f.`,
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
If left unspecified, defaults to 128MiB.
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

	Cache = FlagInfo{
		Name: "cache",
		Description: `
Total size in bytes for caches, shared evenly if there are multiple
storage devices. Size suffixes are supported (e.g. 1GB and 1GiB).
If left unspecified, defaults to 128MiB. A percentage of physical memory
can also be specified (e.g. .25).`,
	}

	ClientHost = FlagInfo{
		Name:        "host",
		EnvVar:      "COCKROACH_HOST",
		Description: `Database server host to connect to.`,
	}

	ClientPort = FlagInfo{
		Name:        "port",
		Shorthand:   "p",
		EnvVar:      "COCKROACH_PORT",
		Description: `Database server port to connect to.`,
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

	EchoSQL = FlagInfo{
		Name: "echo-sql",
		Description: `
Reveal the SQL statements sent implicitly by the command-line utility.`,
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

	TableDisplayFormat = FlagInfo{
		Name: "format",
		Description: `
Selects how to display table rows in results. Possible values: tsv,
csv, pretty, records, sql, raw, html. If left unspecified, defaults to
tsv for non-interactive sessions and pretty for interactive
sessions.`,
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

	ServerHost = FlagInfo{
		Name: "host",
		Description: `
The hostname to listen on. The node will also advertise itself using this
hostname if advertise-host is not specified.`,
	}

	ServerPort = FlagInfo{
		Name:      "port",
		Shorthand: "p",
		Description: `
The port to bind to.`,
	}

	AdvertiseHost = FlagInfo{
		Name: "advertise-host",
		Description: `
The hostname to advertise to other CockroachDB nodes for intra-cluster
communication; it must resolve from other nodes in the cluster.`,
	}

	AdvertisePort = FlagInfo{
		Name: "advertise-port",
		Description: `
The port to advertise to other CockroachDB nodes for intra-cluster
communication.`,
	}

	ServerHTTPHost = FlagInfo{
		Name:        "http-host",
		Description: `The hostname or IP address to bind to for HTTP requests.`,
	}

	ServerHTTPPort = FlagInfo{
		Name:        "http-port",
		Description: `The port to bind to for HTTP requests.`,
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
		Name:   "socket",
		EnvVar: "COCKROACH_SOCKET",
		Description: `
Unix socket file, postgresql protocol only.
Note: when given a path to a unix socket, most postgres clients will
open "<given path>/.s.PGSQL.<server port>"`,
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
listening on all IP addresses (unless --host is provided) and
disabling password authentication for all database users. This is
strongly discouraged for production usage and should never be used on
a public network without combining it with --host.`,
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

	Password = FlagInfo{
		Name:        "password",
		Description: `Prompt for the new user's password.`,
	}

	CertsDir = FlagInfo{
		Name:   "certs-dir",
		EnvVar: "COCKROACH_CERTS_DIR",
		Description: `
The path to the directory containing SSL certificates and keys.
<PRE>

Cockroach looks for certificates and keys inside the directory using the
following naming scheme:

  - CA certificate and key: ca.crt, ca.key
  - Server certificate and key: node.crt, node.key
  - Client certificate and key: client.<user>.crt, client.<user>.key

When running client commands, the user can be specified with the --user flag.
</PRE>

Keys have a minimum permission requirement of 0700 (rwx------). This restriction can be
disabled by setting the environment variable COCKROACH_SKIP_KEY_PERMISSION_CHECK to true.`,
	}

	// Server version of the certs directory flag, cannot be set through environment.
	ServerCertsDir = FlagInfo{
		Name:        "certs-dir",
		Description: CertsDir.Description,
	}

	CAKey = FlagInfo{
		Name:        "ca-key",
		EnvVar:      "COCKROACH_CA_KEY",
		Description: `Path to the CA key.`,
	}

	// TODO(tschottdorf): once clockless mode becomes non-experimental, explain it here:
	// <PRE>
	//
	// </PRE>
	// Specifying the string value 'experimental-clockless' instead of a duration runs the cluster
	// in clockless reads mode. In that mode, reads are routed through Raft and subsequently
	// performance is reduced, but clock synchronization is not relied upon for correctness.
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
Connection url. eg: postgresql://myuser@localhost:26257/mydb
If left empty, the connection flags are used (host, port, user,
database, insecure, certs).`,
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
Start key and format as [<format>:]<key>. Supported formats: raw, human,
rangeID. The raw format supports escaped text. For example, "raw:\x01k" is the
prefix for range local keys.`,
	}

	To = FlagInfo{
		Name: "to",
		Description: `
Exclusive end key and format as [<format>:]<key>. Supported formats: raw,
human, rangeID. The raw format supports escaped text. For example, "raw:\x01k"
is the prefix for range local keys.`}

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

	Decommission = FlagInfo{
		Name: "decommission",
		Description: `
If specified, decommissions the node and waits for it to rebalance before
shutting down the node.`,
	}

	Wait = FlagInfo{
		Name: "wait",
		Description: `
Specifies when to return after having marked the targets as decommissioning.
Takes any of the following values:
<PRE>

  - all:  waits until all target nodes' replica counts have dropped to zero.
    This is the default. Use this unless you are targeting down nodes. In the presence
    of down nodes, this will likely wait forever.
  - live: waits until all live target nodes' replica counts have dropped to zero.
    Use this when targeting down nodes only. When the process returns, manually verify
    that the cluster is fully replicated before proceeding with node removal.
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
)
