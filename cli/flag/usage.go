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

package flag

import (
	"fmt"
	"strings"

	"github.com/kr/text"
)

var flagUsage = map[string]string{
	AttrsName: wrapText(`
An ordered, colon-separated list of node attributes. Attributes are
arbitrary strings specifying topography or machine
capabilities. Topography might include datacenter designation
(e.g. "us-west-1a", "us-west-1b", "us-east-1c"). Machine capabilities
might include specialized hardware or number of cores (e.g. "gpu",
"x16c"). The relative geographic proximity of two nodes is inferred
from the common prefix of the attributes list, so topographic
attributes should be specified first and in the same order for all
nodes. For example:`) + `

  --attrs=us-west-1b:gpu
`,

	CacheName: wrapText(`
Total size in bytes for caches, shared evenly if there are multiple
storage devices. Size suffixes are supported (e.g. 1GB and 1GiB).`),

	ForClient(HostName): wrapText(`
Database server host to connect to.`),

	ForClient(PortName): wrapText(`
Database server port to connect to.`),

	ForClient(HTTPPortName): wrapText(`
Database server port to connect to for HTTP requests.`),

	DatabaseName: wrapText(`
The name of the database to connect to.`),

	ExecuteName: wrapText(`
Execute the SQL statement(s) on the command line, then exit. This flag may be
specified multiple times and each value may contain multiple semicolon
separated statements. If an error occurs in any statement, the command exits
with a non-zero status code and further statements are not executed. The
results of each SQL statement are printed on the standard output.`),

	JoinName: wrapText(`
A comma-separated list of addresses to use when a new node is joining
an existing cluster. For the first node in a cluster, --join should
NOT be specified. Each address in the list has an optional type:
[type=]<address>. An unspecified type means ip address or dns. Type
is one of:`) + `

  - tcp: (default if type is omitted): plain ip address or hostname.
  - http-lb: HTTP load balancer: we query
             http(s)://<address>/_status/details/local
`,

	ForServer(HostName): wrapText(`
The address to listen on. The node will also advertise itself using this
hostname; it must resolve from other nodes in the cluster.`),

	ForServer(PortName): wrapText(`
The port to bind to.`),

	ForServer(HTTPPortName): wrapText(`
The port to bind to for HTTP requests.`),

	SocketName: wrapText(`
Unix socket file, postgresql protocol only.
Note: when given a path to a unix socket, most postgres clients will
open "<given path>/.s.PGSQL.<server port>"`),

	InsecureName: wrapText(`
Run over non-encrypted (non-TLS) connections. This is strongly discouraged for
production usage and this flag must be explicitly specified in order for the
server to listen on an external address in insecure mode.`),

	KeySizeName: wrapText(`
Key size in bits for CA/Node/Client certificates.`),

	MaxResultsName: wrapText(`
Define the maximum number of results that will be retrieved.`),

	PasswordName: wrapText(`
The created user's password. If provided, disables prompting. Pass '-' to
provide the password on standard input.`),

	CACertName: wrapText(`
Path to the CA certificate. Needed by clients and servers in secure mode.`),

	CAKeyName: wrapText(`
Path to the key protecting --ca-cert. Only needed when signing new certificates.`),

	CertName: wrapText(`
Path to the client or server certificate. Needed in secure mode.`),

	KeyName: wrapText(`
Path to the key protecting --cert. Needed in secure mode.`),

	StoreName: wrapText(`
The file path to a storage device. This flag must be specified separately for
each storage device, for example:`) + `

  --store=/mnt/ssd01 --store=/mnt/ssd02 --store=/mnt/hda1

` + wrapText(`
For each store, the "attrs" and "size" fields can be used to specify device
attributes and a maximum store size (see below). When one or both of these
fields are set, the "path" field label must be used for the path to the storage
device, for example:`) + `

  --store=path=/mnt/ssd01,attrs=ssd,size=20GiB

` + wrapText(`
In most cases, node-level attributes are preferable to store-level attributes.
However, the "attrs" field can be used to match capabilities for storage of
individual databases or tables. For example, an OLTP database would probably
want to allocate space for its tables only on solid state devices, whereas
append-only time series might prefer cheaper spinning drives. Typical
attributes include whether the store is flash (ssd), spinny disk (hdd), or
in-memory (mem), as well as speeds and other specs. Attributes can be arbitrary
strings separated by colons, for example: :`) + `

  --store=path=/mnt/hda1,attrs=hdd:7200rpm

` + wrapText(`
The store size in the "size" field is not a guaranteed maximum but is used when
calculating free space for rebalancing purposes. The size can be specified
either in a bytes-based unit or as a percentage of hard drive space,
for example: :`) + `

  --store=path=/mnt/ssd01,size=10000000000     -> 10000000000 bytes
  --store-path=/mnt/ssd01,size=20GB            -> 20000000000 bytes
  --store-path=/mnt/ssd01,size=20GiB           -> 21474836480 bytes
  --store-path=/mnt/ssd01,size=0.02TiB         -> 21474836480 bytes
  --store=path=/mnt/ssd01,size=20%             -> 20% of available space
  --store=path=/mnt/ssd01,size=0.2             -> 20% of available space
  --store=path=/mnt/ssd01,size=.2              -> 20% of available space

` + wrapText(`
For an in-memory store, the "type" and "size" fields are required, and the
"path" field is forbidden. The "type" field must be set to "mem", and the
"size" field must be set to the true maximum bytes or percentage of available
memory that the store may consume, for example:`) + `

  --store=type=mem,size=20GiB
  --store=type=mem,size=90%

` + wrapText(`
Commas are forbidden in all values, since they are used to separate fields.
Also, if you use equal signs in the file path to a store, you must use the
"path" field label.`),
	"time-until-store-dead": wrapText(`
Adjusts the timeout for stores. If there's been no gossiped update
from a store after this time, the store is considered unavailable.
Replicas on an unavailable store will be moved to available ones.`),

	URLName: wrapText(`
Connection url. eg: postgresql://myuser@localhost:26257/mydb
If left empty, the connection flags are used (host, port, user,
database, insecure, certs).`),

	UserName: wrapText(`
Database user name.`),

	FromName: wrapText(`
Start key in pretty-printed format. See also --raw.`),

	ToName: wrapText(`
Exclusive end key in pretty-printed format. See also --raw.`),

	RawName: wrapText(`
Interpret keys as raw bytes.`),

	ValuesName: wrapText(`
Print values along with their associated key.`),
}

const usageIndentation = 8
const wrapWidth = 79 - usageIndentation

func wrapText(s string) string {
	return text.Wrap(s, wrapWidth)
}

// Usage returns the usage information for a given flag identifier. The
// identifier is always the flag's name, except in the case where a client/server
// distinction for the same flag is required.
func Usage(flagID string) string {
	s, ok := flagUsage[flagID]
	if !ok {
		panic(fmt.Sprintf("flag usage not defined for %q", flagID))
	}
	s = "\n" + strings.TrimSpace(s) + "\n"
	// github.com/spf13/pflag appends the default value after the usage text. Add
	// the correct indentation (7 spaces) here. This is admittedly fragile.
	return text.Indent(s, strings.Repeat(" ", usageIndentation)) +
		strings.Repeat(" ", usageIndentation-1)
}

// ForServer maps a general flag name into a server-specific flag identifier.
func ForServer(name string) string {
	return fmt.Sprintf("server-%s", name)
}

// ForClient maps a general flag name into a client-specific flag identifier.
func ForClient(name string) string {
	return fmt.Sprintf("client-%s", name)
}
