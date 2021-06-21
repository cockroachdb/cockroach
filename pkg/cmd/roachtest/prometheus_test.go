// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestMakePrometheusConfig(t *testing.T) {
	ctx := context.Background()
	testCases := []struct {
		desc string

		mockCluster func(ctrl *gomock.Controller) Cluster
		jobName     string
		scrapeNodes []prometheusScrapeNode

		expected string
	}{
		{
			desc:    "workload with multiple scrape nodes",
			jobName: "workload",

			mockCluster: func(ctrl *gomock.Controller) Cluster {
				c := NewMockCluster(ctrl)
				c.EXPECT().
					ExternalIP(ctx, []int{1}).
					Return([]string{"127.0.0.1"}, nil)
				c.EXPECT().
					ExternalIP(ctx, []int{3, 4, 5}).
					Return([]string{"127.0.0.3", "127.0.0.4", "127.0.0.5"}, nil)
				return c
			},
			scrapeNodes: []prometheusScrapeNode{
				{
					node: option.NodeListOption([]int{1}),
					port: 2002,
				},
				{
					node: option.NodeListOption([]int{3, 4, 5}),
					port: 2003,
				},
			},
			expected: `global:
  scrape_interval: 10s
  scrape_timeout: 5s
scrape_configs:
- job_name: workload
  static_configs:
  - targets:
    - 127.0.0.1:2002
    - 127.0.0.3:2003
    - 127.0.0.4:2003
    - 127.0.0.5:2003
  metrics_path: /
`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			cfg, err := makePrometheusConfig(
				ctx,
				tc.mockCluster(ctrl),
				tc.jobName,
				tc.scrapeNodes,
			)
			require.NoError(t, err)
			require.Equal(t, tc.expected, cfg)
		})
	}
}
