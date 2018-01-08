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
	"html/template"
	"io"
	"os"

	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
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
	RunE: MaybeDecorateGRPCError(runGenHAProxyCmd),
}

func runGenHAProxyCmd(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		return usageAndError(cmd)
	}

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
	} else if f, err = os.OpenFile(haProxyPath, os.O_RDWR|os.O_CREATE, 0755); err != nil {
		return err
	} else {
		w = f
	}

	err = configTemplate.Execute(w, nodeStatuses.Nodes)
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
{{range .}}    server cockroach{{.Desc.NodeID}} {{.Desc.Address.AddressField}} check
{{end}}
`
