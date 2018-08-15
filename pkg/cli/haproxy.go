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
	"fmt"
	"html/template"
	"io"
	"os"
	"regexp"
	"sort"
	"strings"

	"io/ioutil"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var haProxyPath string
var haProxyLocality roachpb.Locality

var genHAProxyCmd = &cobra.Command{
	Use:   "haproxy",
	Short: "generate haproxy.cfg for the connected cluster",
	Long: `This command generates a minimal haproxy configuration file for the cluster
reached through the client flags.
The file is written to --out. Use "--out -" for stdout.

The addresses used are those advertized by the nodes themselves. Make sure haproxy
can resolve the hostnames in the configuration file, either by using full-qualified names, or
running haproxy in the same network.

Nodes to include can be filtered by localities matching the '--locality' regular expression. eg:
  --locality=region=us-east                  # Nodes in region "us-east"
  --locality=region=us.*                     # Nodes in the US
  --locality=region=us.*,deployment=testing  # Nodes in the US AND in deployment tier "testing"

A regular expression can be specified per locality tier and all specified must match.
An error is returned if no nodes match the locality filter.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runGenHAProxyCmd),
}

type haProxyNodeInfo struct {
	NodeID   roachpb.NodeID
	NodeAddr string
	// The port on which health checks are performed.
	CheckPort string
	Locality  roachpb.Locality
}

func nodeStatusesToNodeInfos(statuses []status.NodeStatus) []haProxyNodeInfo {
	fs := flag.NewFlagSet("haproxy", flag.ContinueOnError)
	checkPort := fs.String(cliflags.ListenHTTPPort.Name, base.DefaultHTTPPort, "" /* usage */)

	// Discard parsing output.
	fs.SetOutput(ioutil.Discard)

	nodeInfos := make([]haProxyNodeInfo, len(statuses))
	for i, status := range statuses {
		nodeInfos[i].NodeID = status.Desc.NodeID
		nodeInfos[i].NodeAddr = status.Desc.Address.AddressField
		nodeInfos[i].Locality = status.Desc.Locality

		*checkPort = base.DefaultHTTPPort
		// Iterate over the arguments until the ServerHTTPPort flag is found and
		// parse the remainder of the arguments. This is done because Parse returns
		// when it encounters an undefined flag and we do not want to define all
		// possible flags.
		for i, arg := range status.Args {
			if strings.Contains(arg, cliflags.ListenHTTPPort.Name) {
				_ = fs.Parse(status.Args[i:])
			}
		}

		nodeInfos[i].CheckPort = *checkPort
	}
	return nodeInfos
}

func localityMatches(locality roachpb.Locality, desired roachpb.Locality) (bool, error) {
	for _, filterTier := range desired.Tiers {
		// It's a little silly to recompile the regexp for each node, but not a big deal.
		var b strings.Builder
		b.WriteString("^")
		b.WriteString(filterTier.Value)
		b.WriteString("$")
		re, err := regexp.Compile(b.String())
		if err != nil {
			return false, errors.Wrapf(err, "could not compile regular expression for %s", filterTier)
		}

		keyFound := false
		for _, nodeTier := range locality.Tiers {
			if filterTier.Key != nodeTier.Key {
				continue
			}

			keyFound = true
			if !re.MatchString(nodeTier.Value) {
				// Mistmatched tier value.
				return false, nil
			}

			break
		}

		if !keyFound {
			// Tier not found.
			return false, nil
		}
	}

	return true, nil
}

func filterByLocality(nodeInfos []haProxyNodeInfo) ([]haProxyNodeInfo, error) {
	if len(haProxyLocality.Tiers) == 0 {
		// No filter.
		return nodeInfos, nil
	}

	result := make([]haProxyNodeInfo, 0)
	availableLocalities := make(map[string]struct{})

	for _, info := range nodeInfos {
		l := info.Locality
		if len(l.Tiers) == 0 {
			continue
		}

		// Save seen locality.
		availableLocalities[l.String()] = struct{}{}

		matches, err := localityMatches(l, haProxyLocality)
		if err != nil {
			return nil, err
		}

		if matches {
			result = append(result, info)
		}
	}

	if len(result) == 0 {
		seenLocalities := make([]string, len(availableLocalities))
		i := 0
		for l := range availableLocalities {
			seenLocalities[i] = l
			i++
		}
		sort.Strings(seenLocalities)
		return nil, fmt.Errorf("no nodes match locality filter %s. Found localities: %v", haProxyLocality.String(), seenLocalities)
	}

	return result, nil
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

	nodeInfos := nodeStatusesToNodeInfos(nodeStatuses.Nodes)
	filteredNodeInfos, err := filterByLocality(nodeInfos)
	if err != nil {
		return err
	}

	err = configTemplate.Execute(w, filteredNodeInfos)
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
