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
//
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package cliflags

// FlagInfo contains the static information for a CLI flag.
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
Total size in bytes available for use to store temporary data for SQL
clients, including prepared queries and intermediate data rows during
query execution. Size suffixes are supported (e.g. 1GB and 1GiB). If
left unspecified, defaults to 25% of the physical memory, or 512MB if
the memory size cannot be determined.`,
	}

	Cache = FlagInfo{
		Name: "cache",
		Description: `
Total size in bytes for caches, shared evenly if there are multiple
storage devices. Size suffixes are supported (e.g. 1GB and 1GiB).
If left unspecified, defaults to 25% of the physical memory, or
512MB if the memory size cannot be determined.`,
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

	TableDisplayFormat = FlagInfo{
		Name: "format",
		Description: `
Selects how to display table rows in results. Possible values: tsv,
csv, pretty, records, sql, html. If left unspecified, defaults to tsv
for non-interactive sessions and pretty for interactive sessions.`,
	}

	Join = FlagInfo{
		Name:      "join",
		Shorthand: "j",
		Description: `
The address of node which acts as bootstrap when a new node is
joining an existing cluster. This flag can be specified
separately for each address, for example:
<PRE>

  --join=localhost:1234 --join=localhost:2345

</PRE>
Or can be specified as a comma separated list in single flag,
or both forms can be used together, for example:
<PRE>

  --join=localhost:1234,localhost:2345 --join=localhost:3456

</PRE>
Each address in the list has an optional type: [type=]<address>.
An unspecified type means ip address or dns. Type is one of:
<PRE>

  - tcp: (default if type is omitted): plain ip address or hostname.
  - http-lb: HTTP load balancer: we query
             http(s)://<address>/_status/details/local`,
	}

	ServerHost = FlagInfo{
		Name: "host",
		Description: `
The hostname to listen on. The node will also advertise itself using this
hostname if advertise-host is not specified.`,
	}

	ServerPort = FlagInfo{
		Name:        "port",
		Shorthand:   "p",
		Description: `The port to bind to.`,
	}

	AdvertiseHost = FlagInfo{
		Name: "advertise-host",
		Description: `
The hostname to advertise to other CockroachDB nodes for intra-cluster
communication; it must resolve from other nodes in the cluster.`,
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

	MaxResults = FlagInfo{
		Name:        "max-results",
		Description: `Define the maximum number of results that will be retrieved.`,
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
change it, every node in the cluster must be stopped and restarted with the new
value.`,
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
  --store-path=/mnt/ssd01,size=20GB            -> 20000000000 bytes
  --store-path=/mnt/ssd01,size=20GiB           -> 21474836480 bytes
  --store-path=/mnt/ssd01,size=0.02TiB         -> 21474836480 bytes
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
)
