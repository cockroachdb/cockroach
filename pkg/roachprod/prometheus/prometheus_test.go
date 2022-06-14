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
	"github.com/stretchr/testify/require"
)

type clusterSpec struct {
	nodes install.Nodes
	ips   []string
}

func TestMakeYAMLConfig(t *testing.T) {
	testCases := []struct {
		desc                  string
		useWorkloadHelpers    bool
		cluster               *clusterSpec
		workloadScrapeConfigs []ScrapeConfig
		expected              string
	}{
		{
			desc:               "multiple scrape nodes",
			useWorkloadHelpers: false,
			workloadScrapeConfigs: []ScrapeConfig{
				{
					JobName:     "workload0",
					MetricsPath: "/b",
					ScrapeNodes: []ScrapeNode{
						{
							Nodes: install.Nodes{1},
							IPs:   []string{"127.0.0.1"},
							Port:  2002,
						},
						{
							Nodes: install.Nodes{3, 4, 5},
							IPs:   []string{"127.0.0.3", "127.0.0.4", "127.0.0.5"},
							Port:  2003,
						},
					},
				},
				{
					JobName:     "workload1",
					MetricsPath: "/c",
					ScrapeNodes: []ScrapeNode{
						{
							Nodes: install.Nodes{6},
							IPs:   []string{"127.0.0.6"},
							Port:  2009,
						},
					},
				},
			},
			expected: `global:
  scrape_interval: 10s
  scrape_timeout: 5s
scrape_configs:
- job_name: workload0
  static_configs:
  - targets:
    - 127.0.0.1:2002
    - 127.0.0.3:2003
    - 127.0.0.4:2003
    - 127.0.0.5:2003
  metrics_path: /b
- job_name: workload1
  static_configs:
  - targets:
    - 127.0.0.6:2009
  metrics_path: /c
`,
		},
		{
			desc:               "using make commands",
			useWorkloadHelpers: true,
			cluster: &clusterSpec{
				nodes: install.Nodes{8, 9},
				ips:   []string{"127.0.0.8", "127.0.0.9"},
			},
			workloadScrapeConfigs: []ScrapeConfig{
				{
					ScrapeNodes: []ScrapeNode{
						{
							Nodes: install.Nodes{3, 4, 5},
							IPs:   []string{"127.0.0.3", "127.0.0.4", "127.0.0.5"},
							Port:  2005,
						},
						{
							Nodes: install.Nodes{6},
							IPs:   []string{"127.0.0.6"},
							Port:  2009,
						},
					},
				},
			},
			expected: `global:
  scrape_interval: 10s
  scrape_timeout: 5s
scrape_configs:
- job_name: workload0
  static_configs:
  - targets:
    - 127.0.0.3:2005
    - 127.0.0.4:2005
    - 127.0.0.5:2005
    - 127.0.0.6:2009
  metrics_path: /
- job_name: cockroach-n8
  static_configs:
  - labels:
      node: "8"
    targets:
    - 127.0.0.8:26258
  metrics_path: /_status/vars
- job_name: cockroach-n9
  static_configs:
  - labels:
      node: "9"
    targets:
    - 127.0.0.9:26258
  metrics_path: /_status/vars
`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			var promCfg Config
			for i, workloadConfig := range tc.workloadScrapeConfigs {
				if tc.useWorkloadHelpers {
					if len(workloadConfig.ScrapeNodes) == 1 {
						err := promCfg.WithWorkload(
							"workload"+fmt.Sprint(i),
							workloadConfig.ScrapeNodes[0].Nodes,
							workloadConfig.ScrapeNodes[0].Port,
							workloadConfig.ScrapeNodes[0].IPs)
						require.NoError(t, err)
					} else {
						promCfg.ScrapeConfigs = append(promCfg.ScrapeConfigs,
							MakeWorkloadScrapeConfig("workload"+fmt.Sprint(i), workloadConfig.ScrapeNodes))
					}
				} else {
					promCfg.ScrapeConfigs = append(promCfg.ScrapeConfigs, workloadConfig)
				}

			}
			if tc.cluster != nil {
				require.NoError(t, promCfg.WithCluster(tc.cluster.nodes, tc.cluster.ips))
			}
			cfg, err := makeYAMLConfig(
				promCfg.ScrapeConfigs,
			)
			require.NoError(t, err)
			require.Equal(t, tc.expected, cfg)
		})
	}
}
