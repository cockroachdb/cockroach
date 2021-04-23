// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"context"
	"fmt"
	"html/template"
	"io"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

var haProxyPath string
var haProxyLocality roachpb.Locality

var genHAProxyCmd = &cobra.Command{
	Use:   "haproxy",
	Short: "generate haproxy.cfg for the connected cluster",
	Long: `This command generates a minimal haproxy configuration file for the cluster
reached through the client flags.
The file is written to --out. Use "--out -" for stdout.

The addresses used are those advertised by the nodes themselves. Make sure haproxy
can resolve the hostnames in the configuration file, either by using full-qualified names, or
running haproxy in the same network.

Nodes that have been decommissioned are excluded from the generated configuration.

Nodes to include can be filtered by localities matching the '--locality' regular expression. eg:
  --locality=region=us-east                  # Nodes in region "us-east"
  --locality=region=us.*                     # Nodes in the US
  --locality=region=us.*,deployment=testing  # Nodes in the US AND in deployment tier "testing"

A regular expression can be specified per locality tier and all specified tiers must match.
The key (eg: 'region') must be fully specified, only values (eg: 'us-east1') can be regular expressions.
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

func nodeStatusesToNodeInfos(nodes *serverpb.NodesResponse) []haProxyNodeInfo {
	fs := pflag.NewFlagSet("haproxy", pflag.ContinueOnError)

	httpAddr := ""
	httpPort := base.DefaultHTTPPort
	fs.Var(addrSetter{&httpAddr, &httpPort}, cliflags.ListenHTTPAddr.Name, "" /* usage */)
	fs.Var(aliasStrVar{&httpPort}, cliflags.ListenHTTPPort.Name, "" /* usage */)

	// Discard parsing output.
	fs.SetOutput(ioutil.Discard)

	nodeInfos := make([]haProxyNodeInfo, 0, len(nodes.Nodes))

	// The response can present nodes in arbitrary order. We want them sorted.
	nodeIDs := make([]int, 0, len(nodes.Nodes))
	statusByID := make(map[roachpb.NodeID]statuspb.NodeStatus)
	for _, status := range nodes.Nodes {
		statusByID[status.Desc.NodeID] = status
		nodeIDs = append(nodeIDs, int(status.Desc.NodeID))
	}
	sort.Ints(nodeIDs)

	for _, inodeID := range nodeIDs {
		nodeID := roachpb.NodeID(inodeID)
		status := statusByID[nodeID]
		liveness := nodes.LivenessByNodeID[nodeID]
		switch liveness {
		case livenesspb.NodeLivenessStatus_DECOMMISSIONING:
			fmt.Fprintf(stderr, "warning: node %d status is %s, excluding from haproxy configuration\n",
				nodeID, liveness)
			fallthrough
		case livenesspb.NodeLivenessStatus_DECOMMISSIONED:
			continue
		}

		info := haProxyNodeInfo{
			NodeID:   nodeID,
			NodeAddr: status.Desc.Address.AddressField,
			Locality: status.Desc.Locality,
		}

		httpPort = base.DefaultHTTPPort
		// Iterate over the arguments until the ServerHTTPPort flag is found and
		// parse the remainder of the arguments. This is done because Parse returns
		// when it encounters an undefined flag and we do not want to define all
		// possible flags.
		//
		// TODO(knz): this logic is horrendously broken and
		// incorrect. Replace it.
		for j, arg := range status.Args {
			if strings.Contains(arg, cliflags.ListenHTTPPort.Name) ||
				strings.Contains(arg, cliflags.ListenHTTPAddr.Name) {
				_ = fs.Parse(status.Args[j:])
				break
			}
		}

		info.CheckPort = httpPort
		nodeInfos = append(nodeInfos, info)
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
			return false, errors.Wrapf(err, "could not compile regular expression for %q", filterTier)
		}

		keyFound := false
		for _, nodeTier := range locality.Tiers {
			if filterTier.Key != nodeTier.Key {
				continue
			}

			keyFound = true
			if !re.MatchString(nodeTier.Value) {
				// Mismatched tier value.
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

	conn, _, finish, err := getClientGRPCConn(ctx, serverCfg)
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
	} else if f, err = os.OpenFile(haProxyPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644); err != nil {
		return err
	} else {
		w = f
	}

	nodeInfos := nodeStatusesToNodeInfos(nodeStatuses)
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

    # With the timeout connect 5 secs,
    # if the backend server is not responding, haproxy will make a total
    # of 3 connection attempts waiting 5s each time before giving up on the server,
    # for a total of 15 seconds.
    retries             2
    timeout connect     5s

    # timeout client and server govern the maximum amount of time of TCP inactivity.
    # The server node may idle on a TCP connection either because it takes time to
    # execute a query before the first result set record is emitted, or in case of
    # some trouble on the server. So these timeout settings should be larger than the
    # time to execute the longest (most complex, under substantial concurrent workload)
    # query, yet not too large so truly failed connections are lingering too long
    # (resources associated with failed connections should be freed reasonably promptly).
    timeout client      10m
    timeout server      10m

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
