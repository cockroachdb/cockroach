// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package config

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestAlloyConfig(t *testing.T) {
	datadriven.Walk(t, datapathutils.TestDataPath(t, "datadriven"), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "file":
				// Read from the specified file
				filename := td.CmdArgs[0].Key
				data, err := os.ReadFile(datapathutils.TestDataPath(t, filename))
				if err != nil {
					return err.Error()
				}

				// Decode
				var cfg AlloyConfig
				err = Decode(bytes.NewReader(data), &cfg)
				if err != nil {
					return err.Error()
				}
				return encodeConfig(&cfg)

			case "add-nodes":
				cfg := createBaseConfig(t, td)
				var clusterName string
				td.ScanArgs(t, "cluster", &clusterName)

				nodes := parseNodeAddresses(t, td.CmdArgs, "nodes")
				err := cfg.AddNodes(clusterName, nodes)
				if err != nil {
					return err.Error()
				}
				return encodeConfig(&cfg)

			case "remove-nodes":
				cfg := createBaseConfig(t, td)
				var clusterName string
				td.ScanArgs(t, "cluster", &clusterName)

				nodeNums := parseNodeList(t, td.CmdArgs, "nodes")
				err := cfg.RemoveNodes(clusterName, nodeNums)
				if err != nil {
					return err.Error()
				}
				return encodeConfig(&cfg)

			case "update-target":
				var clusterName, secureCookie string
				td.ScanArgs(t, "cluster", &clusterName)
				td.ScanArgs(t, "secure-cookie", &secureCookie)

				// Check if we're creating a new target or updating existing
				cfg := createBaseConfig(t, td)
				if td.HasArg("new-target") {
					// Create a fresh config with just the writer
					cfg = AlloyConfig{
						PyroscopeWrite: []*PyroscopeWriteConfig{
							{
								Label: "local",
								Endpoints: []*EndpointConfig{
									{URL: "http://pyroscope:4040"},
								},
							},
						},
					}
				}
				cfg.UpdateTarget(clusterName, secureCookie)
				return encodeConfig(&cfg)

			default:
				require.Failf(t, "unknown command", "%q", td.Cmd)
			}
			return ""
		})
	})
}

func parseNodeAddresses(
	t *testing.T, cmdArgs []datadriven.CmdArg, key string,
) map[install.Node]string {
	nodes := make(map[install.Node]string)
	for _, arg := range cmdArgs {
		if arg.Key == key && len(arg.Vals) == 2 {
			nodeNum, err := strconv.Atoi(arg.Vals[0])
			require.NoError(t, err, "invalid node number: %s", arg.Vals[0])
			nodes[install.Node(nodeNum)] = arg.Vals[1]
		}
	}
	return nodes
}

func parseNodeList(t *testing.T, cmdArgs []datadriven.CmdArg, key string) install.Nodes {
	var nodeNums install.Nodes
	for _, arg := range cmdArgs {
		if arg.Key == key {
			for _, val := range arg.Vals {
				nodeNum, err := strconv.Atoi(val)
				require.NoError(t, err, "invalid node number: %s", val)
				nodeNums = append(nodeNums, install.Node(nodeNum))
			}
		}
	}
	return nodeNums
}

func createBaseConfig(t *testing.T, td *datadriven.TestData) AlloyConfig {
	cfg := AlloyConfig{
		PyroscopeWrite: []*PyroscopeWriteConfig{
			{
				Label: "local",
				Endpoints: []*EndpointConfig{
					{URL: "http://pyroscope:4040"},
				},
			},
		},
	}

	var clusterName, initialSecureCookie string
	td.ScanArgs(t, "cluster", &clusterName)
	td.MaybeScanArgs(t, "initial-secure-cookie", &initialSecureCookie)

	// Create the scrape target
	cfg.UpdateTarget(clusterName, initialSecureCookie)

	// Add initial nodes if specified
	initialNodes := parseNodeAddresses(t, td.CmdArgs, "initial-nodes")
	if len(initialNodes) > 0 {
		err := cfg.AddNodes(clusterName, initialNodes)
		require.NoError(t, err, "failed to add initial nodes")
	}
	return cfg
}

func encodeConfig(cfg *AlloyConfig) string {
	var buf bytes.Buffer
	err := Encode(&buf, cfg)
	if err != nil {
		return fmt.Sprintf("ERROR: %v", err)
	}
	return buf.String()
}
