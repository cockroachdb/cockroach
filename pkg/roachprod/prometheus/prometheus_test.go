// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package prometheus

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/stretchr/testify/require"
)

var nodeIPMap = map[install.Node]string{
	install.Node(1): "127.0.0.1",
	install.Node(2): "127.0.0.2",
	install.Node(3): "127.0.0.3",
	install.Node(4): "127.0.0.4",
	install.Node(5): "127.0.0.5",
	install.Node(6): "127.0.0.6",
	install.Node(7): "127.0.0.7",
	install.Node(8): "127.0.0.8",
	install.Node(9): "127.0.0.9",
}

func TestMakeYAMLConfig(t *testing.T) {
	testCases := []struct {
		testfile              string
		useWorkloadHelpers    bool
		cluster               install.Nodes
		workloadScrapeConfigs []ScrapeConfig
	}{
		{
			testfile:           "multipleScrapeNodes.txt",
			useWorkloadHelpers: false,
			workloadScrapeConfigs: []ScrapeConfig{
				{
					JobName:     "workload0",
					MetricsPath: "/b",
					ScrapeNodes: []ScrapeNode{
						{
							Node: install.Node(1),
							Port: 2002,
						},
						{
							Node: install.Node(3),
							Port: 2003,
						},
						{
							Node: install.Node(4),
							Port: 2003,
						},
						{
							Node: install.Node(5),
							Port: 2003,
						},
					},
				},
				{
					JobName:     "workload1",
					MetricsPath: "/c",
					ScrapeNodes: []ScrapeNode{
						{
							Node: install.Node(6),
							Port: 2009,
						},
					},
				},
			},
		},
		{
			testfile:           "usingMakeCommands.txt",
			useWorkloadHelpers: true,
			cluster:            install.Nodes{8, 9},
			workloadScrapeConfigs: []ScrapeConfig{
				{
					ScrapeNodes: []ScrapeNode{
						{
							Node: install.Node(3),
							Port: 2005,
						},
						{
							Node: install.Node(4),
							Port: 2005,
						},
						{
							Node: install.Node(5),
							Port: 2005,
						},
						{
							Node: install.Node(6),
							Port: 2009,
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testfile, func(t *testing.T) {
			var promCfg Config
			for i, workloadConfig := range tc.workloadScrapeConfigs {
				if tc.useWorkloadHelpers {
					for _, scrapeNode := range workloadConfig.ScrapeNodes {
						// test appending to same workload
						promCfg.WithWorkload(
							"workload"+fmt.Sprint(i),
							scrapeNode.Node,
							scrapeNode.Port)
					}

				} else {
					promCfg.ScrapeConfigs = append(promCfg.ScrapeConfigs, workloadConfig)
				}

			}
			if tc.cluster != nil {
				promCfg.WithCluster(tc.cluster)
			}
			cfg, err := makeYAMLConfig(promCfg.ScrapeConfigs, nodeIPMap)
			require.NoError(t, err)
			echotest.Require(t, cfg, testutils.TestDataPath(t, tc.testfile))
		})
	}
}
