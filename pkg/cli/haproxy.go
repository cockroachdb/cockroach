// Copyright 2017 The Cockroach Authors.
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

package cli

import (
	"context"
	"flag"
	"html/template"
	"io"
	"os"
	"strings"

	"io/ioutil"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/spf13/cobra"
)

var haProxyPath string

var genHAProxyCmd = &cobra.Command{
	Use:   "haproxy",
	Short: "generate haproxy.cfg for the connected cluster",
	Long: `This command generates a minimal haproxy configuration file for the cluster
reached through the client flags.
The file is written to --out. Use "--out -" for stdout.

The addresses used are those advertized by the nodes themselves. Make sure haproxy
can resolve the hostnames in the configuration file, either by using full-qualified names, or
running haproxy in the same network.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runGenHAProxyCmd),
}

type haProxyNodeInfo struct {
	NodeID   roachpb.NodeID
	NodeAddr string
	// The port on which health checks are performed.
	CheckPort string
}

func nodeStatusesToNodeInfos(statuses []status.NodeStatus) []haProxyNodeInfo {
	fs := flag.NewFlagSet("haproxy", flag.ContinueOnError)
	checkPort := fs.String(cliflags.ServerHTTPPort.Name, base.DefaultHTTPPort, "" /* usage */)

	// Discard parsing output.
	fs.SetOutput(ioutil.Discard)

	nodeInfos := make([]haProxyNodeInfo, len(statuses))
	for i, status := range statuses {
		nodeInfos[i].NodeID = status.Desc.NodeID
		nodeInfos[i].NodeAddr = status.Desc.Address.AddressField

		*checkPort = base.DefaultHTTPPort
		// Iterate over the arguments until the ServerHTTPPort flag is found and
		// parse the remainder of the arguments. This is done because Parse returns
		// when it encounters an undefined flag and we do not want to define all
		// possible flags.
		for i, arg := range status.Args {
			if strings.Contains(arg, cliflags.ServerHTTPPort.Name) {
				_ = fs.Parse(status.Args[i:])
			}
		}

		nodeInfos[i].CheckPort = *checkPort
	}
	return nodeInfos
}

func runGenHAProxyCmd(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	configTemplate, err := template.New("haproxy template").Parse(haProxyTemplate)
	if err != nil {
		return err
	}

	conn, _, finish, err := getClientGRPCConn(ctx)
	if err != nil {
		return err
	}
	defer finish()
	c := serverpb.NewStatusClient(conn)

	nodeStatuses, err := c.Nodes(ctx, &serverpb.NodesRequest{})
	if err != nil {
		return err
	}

	var w io.Writer
	var f *os.File
	if haProxyPath == "-" {
		w = os.Stdout
	} else if f, err = os.OpenFile(haProxyPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755); err != nil {
		return err
	} else {
		w = f
	}

	err = configTemplate.Execute(w, nodeStatusesToNodeInfos(nodeStatuses.Nodes))
	if err != nil {
		// Return earliest error, but still close the file.
		_ = f.Close()
		return err
	}

	if f != nil {
		return f.Close()
	}

	return nil
}

const haProxyTemplate = `
global
  maxconn 4096

defaults
    mode                tcp
    # Timeout values should be configured for your specific use.
    # See: https://cbonte.github.io/haproxy-dconv/1.8/configuration.html#4-timeout%20connect
    timeout connect     10s
    timeout client      1m
    timeout server      1m
    # TCP keep-alive on client side. Server already enables them.
    option              clitcpka

listen psql
    bind :26257
    mode tcp
    balance roundrobin
    option httpchk GET /health?ready=1
{{range .}}    server cockroach{{.NodeID}} {{.NodeAddr}} check port {{.CheckPort}}
{{end}}
`
