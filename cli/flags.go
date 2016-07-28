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
// Author: Daniel Theophanes (kardianos@gmail.com)

package cli

import (
	"flag"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/kr/text"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/cockroachdb/cockroach/base"
	"github.com/cockroachdb/cockroach/cli/cliflags"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util/envutil"
	"github.com/cockroachdb/cockroach/util/humanizeutil"
	"github.com/cockroachdb/cockroach/util/log/logflags"
)

var maxResults int64

var connURL string
var connUser, connHost, connPort, httpPort, httpAddr, connDBName, zoneConfig string
var startBackground bool
var undoFreezeCluster bool

var serverCtx = server.MakeContext()
var baseCtx = serverCtx.Context
var cliCtx = cliContext{Context: baseCtx}
var sqlCtx = sqlContext{cliContext: &cliCtx}
var debugCtx = debugContext{
	startKey: engine.NilKey,
	endKey:   engine.MVCCKeyMax,
}

var cacheSize *bytesValue
var insecure *insecureValue

var flagUsage = map[string]string{
	cliflags.AttrsName: wrapText(`
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

	cliflags.ZoneConfigName: wrapText(`
File to read the zone configuration from. Specify "-" to read from standard input.`),

	cliflags.BackgroundName: wrapText(`
Start the server in the background. This is similar to appending "&"
to the command line, but when the server is started with --background,
control is not returned to the shell until the server is ready to
accept requests.`),

	cliflags.CacheName: wrapText(`
Total size in bytes for caches, shared evenly if there are multiple
storage devices. Size suffixes are supported (e.g. 1GB and 1GiB).
If left unspecified, defaults to 25% of the physical memory, or
512MB if the memory size cannot be determined.`),

	forClient(cliflags.HostName): wrapText(`
Database server host to connect to.`),

	forClient(cliflags.PortName): wrapText(`
Database server port to connect to.`),

	forClient(cliflags.HTTPPortName): wrapText(`
Database server port to connect to for HTTP requests.`),

	forClient(cliflags.HTTPAddrName): wrapText(`
Database server (hostname or IP address) to connect to for HTTP requests.`),

	cliflags.DatabaseName: wrapText(`
The name of the database to connect to.`),

	cliflags.DepsName: wrapText(`
Include dependency versions`),

	cliflags.ExecuteName: wrapText(`
Execute the SQL statement(s) on the command line, then exit. This flag may be
specified multiple times and each value may contain multiple semicolon
separated statements. If an error occurs in any statement, the command exits
with a non-zero status code and further statements are not executed. The
results of each SQL statement are printed on the standard output.`),

	cliflags.PrettyName: wrapText(`
Causes table rows to be formatted as tables using ASCII art.
When not specified, table rows are printed as tab-separated values (TSV).`),

	cliflags.JoinName: wrapText(`
The address of node which acts as bootstrap when a new node is
joining an existing cluster. This flag can be specified
separately for each address, for example:`) + `

  --join=localhost:1234 --join=localhost:2345

` + wrapText(`
Or can be specified as a comma separated list in single flag,
or both forms can be used together, for example:`) + `

  --join=localhost:1234,localhost:2345 --join=localhost:3456

` + wrapText(`
Each address in the list has an optional type: [type=]<address>.
An unspecified type means ip address or dns. Type is one of:`) + `

  - tcp: (default if type is omitted): plain ip address or hostname.
  - http-lb: HTTP load balancer: we query
             http(s)://<address>/_status/details/local
`,

	forServer(cliflags.HostName): wrapText(`
The address to listen on. The node will also advertise itself using this
hostname; it must resolve from other nodes in the cluster.`),

	forServer(cliflags.PortName): wrapText(`
The port to bind to.`),

	forServer(cliflags.HTTPPortName): wrapText(`
The port to bind to for HTTP requests.`),

	forServer(cliflags.HTTPAddrName): wrapText(`
The hostname or IP address to bind to for HTTP requests.`),

	cliflags.SocketName: wrapText(`
Unix socket file, postgresql protocol only.
Note: when given a path to a unix socket, most postgres clients will
open "<given path>/.s.PGSQL.<server port>"`),

	cliflags.InsecureName: wrapText(`
Run over non-encrypted (non-TLS) connections. This is strongly discouraged for
production usage and this flag must be explicitly specified in order for the
server to listen on an external address in insecure mode.`),

	cliflags.KeySizeName: wrapText(`
Key size in bits for CA/Node/Client certificates.`),

	cliflags.MaxResultsName: wrapText(`
Define the maximum number of results that will be retrieved.`),

	cliflags.PasswordName: wrapText(`
The created user's password. If provided, disables prompting. Pass '-' to
provide the password on standard input.`),

	cliflags.CACertName: wrapText(`
Path to the CA certificate. Needed by clients and servers in secure mode.`),

	cliflags.CAKeyName: wrapText(`
Path to the key protecting --ca-cert. Only needed when signing new certificates.`),

	cliflags.CertName: wrapText(`
Path to the client or server certificate. Needed in secure mode.`),

	cliflags.KeyName: wrapText(`
Path to the key protecting --cert. Needed in secure mode.`),

	cliflags.StoreName: wrapText(`
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

	cliflags.URLName: wrapText(`
Connection url. eg: postgresql://myuser@localhost:26257/mydb
If left empty, the connection flags are used (host, port, user,
database, insecure, certs).`),

	cliflags.UserName: wrapText(`
Database user name.`),

	cliflags.FromName: wrapText(`
Start key and format as [<format>:]<key>. Supported formats:
`) + fmt.Sprintf("\n%+v", keyTypes()),

	cliflags.ToName: wrapText(`
Exclusive end key and format as [<format>:]<key>. Supported formats:
`) + fmt.Sprintf("\n%+v", keyTypes()),

	cliflags.ValuesName: wrapText(`
Print values along with their associated key.`),

	cliflags.SizesName: wrapText(`
Print key and value sizes along with their associated key.`),

	cliflags.RaftTickIntervalName: wrapText(`
The resolution of the Raft timer; other raft timeouts are
defined in terms of multiples of this value.`),

	cliflags.UndoFreezeClusterName: wrapText(`
Attempt to undo an earlier attempt to freeze the cluster.`),
}

const usageIndentation = 8
const wrapWidth = 79 - usageIndentation

func wrapText(s string) string {
	return text.Wrap(s, wrapWidth)
}

// makeUsageString returns the usage information for a given flag identifier. The
// identifier is always the flag's name, except in the case where a client/server
// distinction for the same flag is required.
func makeUsageString(flagID string, hasEnv bool) string {
	s, ok := flagUsage[flagID]
	if !ok {
		panic(fmt.Sprintf("flag usage not defined for %q", flagID))
	}
	s = "\n" + strings.TrimSpace(s) + "\n"
	if hasEnv {
		s = s + "Environment variable: " + envutil.VarName(flagID) + "\n"
	}
	// github.com/spf13/pflag appends the default value after the usage text. Add
	// the correct indentation (7 spaces) here. This is admittedly fragile.
	return text.Indent(s, strings.Repeat(" ", usageIndentation)) +
		strings.Repeat(" ", usageIndentation-1)
}

func usageEnv(name string) string   { return makeUsageString(name, true) }
func usageNoEnv(name string) string { return makeUsageString(name, false) }

// forServer maps a general flag name into a server-specific flag identifier.
func forServer(name string) string {
	return fmt.Sprintf("server-%s", name)
}

// forClient maps a general flag name into a client-specific flag identifier.
func forClient(name string) string {
	return name
}

type bytesValue struct {
	val   *int64
	isSet bool
}

func newBytesValue(val *int64) *bytesValue {
	return &bytesValue{val: val}
}

func (b *bytesValue) Set(s string) error {
	v, err := humanizeutil.ParseBytes(s)
	if err != nil {
		return err
	}
	*b.val = v
	b.isSet = true
	return nil
}

func (b *bytesValue) Type() string {
	return "bytes"
}

func (b *bytesValue) String() string {
	// This uses the MiB, GiB, etc suffixes. If we use humanize.Bytes() we get
	// the MB, GB, etc suffixes, but the conversion is done in multiples of 1000
	// vs 1024.
	return humanizeutil.IBytes(*b.val)
}

type insecureValue struct {
	ctx   *base.Context
	isSet bool
}

func newInsecureValue(ctx *base.Context) *insecureValue {
	return &insecureValue{ctx: ctx}
}

func (b *insecureValue) IsBoolFlag() bool {
	return true
}

func (b *insecureValue) Set(s string) error {
	v, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}
	b.isSet = true
	b.ctx.Insecure = v
	if b.ctx.Insecure {
		// If --insecure is specified, clear any of the existing security flags if
		// they were set. This allows composition of command lines where a later
		// specification of --insecure clears an earlier security specification.
		b.ctx.SSLCA = ""
		b.ctx.SSLCAKey = ""
		b.ctx.SSLCert = ""
		b.ctx.SSLCertKey = ""
	}
	return nil
}

func (b *insecureValue) Type() string {
	return "bool"
}

func (b *insecureValue) String() string {
	return fmt.Sprint(b.ctx.Insecure)
}

func init() {
	// Change the logging defaults for the main cockroach binary.
	if err := flag.Lookup(logflags.LogToStderrName).Value.Set("false"); err != nil {
		panic(err)
	}

	// Map any flags registered in the standard "flag" package into the
	// top-level cockroach command.
	pf := cockroachCmd.PersistentFlags()
	flag.VisitAll(func(f *flag.Flag) {
		flag := pflag.PFlagFromGoFlag(f)
		// TODO(peter): Decide if we want to make the lightstep flags visible.
		if strings.HasPrefix(flag.Name, "lightstep_") {
			flag.Hidden = true
		}
		pf.AddFlag(flag)
	})

	// The --log-dir default changes depending on the command. Avoid confusion by
	// simply clearing it.
	pf.Lookup(logflags.LogDirName).DefValue = ""
	// If no value is specified for --alsologtostderr output everything.
	pf.Lookup(logflags.AlsoLogToStderrName).NoOptDefVal = "INFO"

	// Security flags.
	baseCtx.Insecure = true
	insecure = newInsecureValue(baseCtx)

	{
		f := startCmd.Flags()

		// Server flags.
		f.StringVar(&connHost, cliflags.HostName, "", usageNoEnv(forServer(cliflags.HostName)))
		f.StringVarP(&connPort, cliflags.PortName, "p", base.DefaultPort, usageNoEnv(forServer(cliflags.PortName)))
		f.StringVar(&httpPort, cliflags.HTTPPortName, base.DefaultHTTPPort, usageNoEnv(forServer(cliflags.HTTPPortName)))
		f.StringVar(&httpAddr, cliflags.HTTPAddrName, "", usageNoEnv(forServer(cliflags.HTTPAddrName)))
		f.StringVar(&serverCtx.Attrs, cliflags.AttrsName, serverCtx.Attrs, usageNoEnv(cliflags.AttrsName))
		f.VarP(&serverCtx.Stores, cliflags.StoreName, "s", usageNoEnv(cliflags.StoreName))
		f.DurationVar(&serverCtx.RaftTickInterval, cliflags.RaftTickIntervalName, base.DefaultRaftTickInterval, usageNoEnv(cliflags.RaftTickIntervalName))
		f.BoolVar(&startBackground, cliflags.BackgroundName, false, usageNoEnv(cliflags.BackgroundName))

		// Usage for the unix socket is odd as we use a real file, whereas
		// postgresql and clients consider it a directory and build a filename
		// inside it using the port.
		// Thus, we keep it hidden and use it for testing only.
		f.StringVar(&serverCtx.SocketFile, cliflags.SocketName, "", usageEnv(cliflags.SocketName))
		_ = f.MarkHidden(cliflags.SocketName)

		insecureF := f.VarPF(insecure, cliflags.InsecureName, "", usageEnv(cliflags.InsecureName))
		insecureF.NoOptDefVal = envutil.EnvOrDefaultString(cliflags.InsecureName, "true")

		// Certificate flags.
		f.StringVar(&baseCtx.SSLCA, cliflags.CACertName, baseCtx.SSLCA, usageNoEnv(cliflags.CACertName))
		f.StringVar(&baseCtx.SSLCert, cliflags.CertName, baseCtx.SSLCert, usageNoEnv(cliflags.CertName))
		f.StringVar(&baseCtx.SSLCertKey, cliflags.KeyName, baseCtx.SSLCertKey, usageNoEnv(cliflags.KeyName))

		// Cluster joining flags.
		f.VarP(&serverCtx.JoinList, cliflags.JoinName, "j", usageNoEnv(cliflags.JoinName))

		// Engine flags.
		setDefaultCacheSize(&serverCtx)
		cacheSize = newBytesValue(&serverCtx.CacheSize)
		f.Var(cacheSize, cliflags.CacheName, usageNoEnv(cliflags.CacheName))
	}

	for _, cmd := range certCmds {
		f := cmd.Flags()
		// Certificate flags.
		f.StringVar(&baseCtx.SSLCA, cliflags.CACertName, baseCtx.SSLCA, usageNoEnv(cliflags.CACertName))
		f.StringVar(&baseCtx.SSLCAKey, cliflags.CAKeyName, baseCtx.SSLCAKey, usageNoEnv(cliflags.CAKeyName))
		f.StringVar(&baseCtx.SSLCert, cliflags.CertName, baseCtx.SSLCert, usageNoEnv(cliflags.CertName))
		f.StringVar(&baseCtx.SSLCertKey, cliflags.KeyName, baseCtx.SSLCertKey, usageNoEnv(cliflags.KeyName))
		f.IntVar(&keySize, cliflags.KeySizeName, defaultKeySize, usageNoEnv(cliflags.KeySizeName))
	}

	setUserCmd.Flags().StringVar(&password, cliflags.PasswordName, envutil.EnvOrDefaultString(cliflags.PasswordName, ""), usageEnv(cliflags.PasswordName))

	clientCmds := []*cobra.Command{
		sqlShellCmd, quitCmd, freezeClusterCmd, dumpCmd, /* startCmd is covered above */
	}
	clientCmds = append(clientCmds, kvCmds...)
	clientCmds = append(clientCmds, rangeCmds...)
	clientCmds = append(clientCmds, userCmds...)
	clientCmds = append(clientCmds, zoneCmds...)
	clientCmds = append(clientCmds, nodeCmds...)
	for _, cmd := range clientCmds {
		f := cmd.PersistentFlags()
		f.StringVar(&connHost, cliflags.HostName, envutil.EnvOrDefaultString(cliflags.HostName, ""), usageEnv(forClient(cliflags.HostName)))

		insecureF := f.VarPF(insecure, cliflags.InsecureName, "", usageEnv(cliflags.InsecureName))
		insecureF.NoOptDefVal = envutil.EnvOrDefaultString(cliflags.InsecureName, "true")

		// Certificate flags.
		f.StringVar(&baseCtx.SSLCA, cliflags.CACertName, envutil.EnvOrDefaultString(cliflags.CACertName, baseCtx.SSLCA), usageEnv(cliflags.CACertName))
		f.StringVar(&baseCtx.SSLCert, cliflags.CertName, envutil.EnvOrDefaultString(cliflags.CertName, baseCtx.SSLCert), usageEnv(cliflags.CertName))
		f.StringVar(&baseCtx.SSLCertKey, cliflags.KeyName, envutil.EnvOrDefaultString(cliflags.KeyName, baseCtx.SSLCertKey), usageEnv(cliflags.KeyName))

		// By default, client commands print their output as
		// pretty-formatted tables on terminals, and TSV when redirected
		// to a file. The user can override with --pretty.
		f.BoolVar(&cliCtx.prettyFmt, cliflags.PrettyName, isInteractive, usageNoEnv(cliflags.PrettyName))
	}
	{
		f := setZoneCmd.Flags()
		f.StringVarP(&zoneConfig, cliflags.ZoneConfigName, "f", "", usageNoEnv(cliflags.ZoneConfigName))
	}
	{
		f := sqlShellCmd.Flags()
		f.VarP(&sqlCtx.execStmts, cliflags.ExecuteName, "e", usageNoEnv(cliflags.ExecuteName))
	}
	{
		f := freezeClusterCmd.PersistentFlags()
		f.BoolVar(&undoFreezeCluster, cliflags.UndoFreezeClusterName, false, usageNoEnv(cliflags.UndoFreezeClusterName))
	}

	// Commands that need the cockroach port.
	simpleCmds := []*cobra.Command{quitCmd, freezeClusterCmd}
	simpleCmds = append(simpleCmds, kvCmds...)
	simpleCmds = append(simpleCmds, rangeCmds...)
	simpleCmds = append(simpleCmds, nodeCmds...)
	for _, cmd := range simpleCmds {
		f := cmd.PersistentFlags()
		f.StringVarP(&connPort, cliflags.PortName, "p", envutil.EnvOrDefaultString(cliflags.PortName, base.DefaultPort), usageEnv(forClient(cliflags.PortName)))
	}

	// Commands that establish a SQL connection.
	sqlCmds := []*cobra.Command{sqlShellCmd, dumpCmd}
	sqlCmds = append(sqlCmds, zoneCmds...)
	sqlCmds = append(sqlCmds, userCmds...)
	for _, cmd := range sqlCmds {
		f := cmd.PersistentFlags()
		f.StringVar(&connURL, cliflags.URLName, envutil.EnvOrDefaultString(cliflags.URLName, ""), usageEnv(cliflags.URLName))

		f.StringVarP(&connUser, cliflags.UserName, "u", envutil.EnvOrDefaultString(cliflags.UserName, security.RootUser), usageEnv(cliflags.UserName))
		f.StringVarP(&connPort, cliflags.PortName, "p", envutil.EnvOrDefaultString(cliflags.PortName, base.DefaultPort), usageEnv(forClient(cliflags.PortName)))
		f.StringVarP(&connDBName, cliflags.DatabaseName, "d", envutil.EnvOrDefaultString(cliflags.DatabaseName, ""), usageEnv(cliflags.DatabaseName))
	}

	// Max results flag for scan, reverse scan, and range list.
	for _, cmd := range []*cobra.Command{scanCmd, reverseScanCmd, lsRangesCmd} {
		f := cmd.Flags()
		f.Int64Var(&maxResults, cliflags.MaxResultsName, 1000, usageNoEnv(cliflags.MaxResultsName))
	}

	// Debug commands.
	{
		f := debugKeysCmd.Flags()
		f.Var((*mvccKey)(&debugCtx.startKey), cliflags.FromName, usageNoEnv(cliflags.FromName))
		f.Var((*mvccKey)(&debugCtx.endKey), cliflags.ToName, usageNoEnv(cliflags.ToName))
		f.BoolVar(&debugCtx.values, cliflags.ValuesName, false, usageNoEnv(cliflags.ValuesName))
		f.BoolVar(&debugCtx.sizes, cliflags.SizesName, false, usageNoEnv(cliflags.SizesName))
	}

	{
		f := versionCmd.Flags()
		f.BoolVar(&versionIncludesDeps, cliflags.DepsName, false, usageNoEnv(cliflags.DepsName))
	}

	cobra.OnInitialize(extraFlagInit)
}

// extraFlagInit is a standalone function so we can test more easily.
func extraFlagInit() {
	// If any of the security flags have been set, clear the insecure
	// setting. Note that we do the inverse when the --insecure flag is
	// set. See insecureValue.Set().
	if baseCtx.SSLCA != "" || baseCtx.SSLCAKey != "" ||
		baseCtx.SSLCert != "" || baseCtx.SSLCertKey != "" {
		baseCtx.Insecure = false
	}

	serverCtx.Addr = net.JoinHostPort(connHost, connPort)
	if httpAddr == "" {
		httpAddr = connHost
	}
	serverCtx.HTTPAddr = net.JoinHostPort(httpAddr, httpPort)
}
