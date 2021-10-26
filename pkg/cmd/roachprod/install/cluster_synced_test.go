// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package install

import (
	"fmt"
	"testing"
)

// TestRoachprodEnv tests the roachprodEnvRegex and roachprodEnvValue methods.
func TestRoachprodEnv(t *testing.T) {
	cases := []struct {
		clusterName string
		node        int
		tag         string
		value       string
		regex       string
	}{
		{
			clusterName: "a",
			node:        1,
			tag:         "",
			value:       "1",
			regex:       `ROACHPROD=1[ \/]`,
		},
		{
			clusterName: "local-foo",
			node:        2,
			tag:         "",
			value:       "local-foo/2",
			regex:       `ROACHPROD=local-foo\/2[ \/]`,
		},
		{
			clusterName: "a",
			node:        3,
			tag:         "foo",
			value:       "3/foo",
			regex:       `ROACHPROD=3\/foo[ \/]`,
		},
		{
			clusterName: "a",
			node:        4,
			tag:         "foo/bar",
			value:       "4/foo/bar",
			regex:       `ROACHPROD=4\/foo\/bar[ \/]`,
		},
		{
			clusterName: "local-foo",
			node:        5,
			tag:         "tag",
			value:       "local-foo/5/tag",
			regex:       `ROACHPROD=local-foo\/5\/tag[ \/]`,
		},
	}

	for idx, tc := range cases {
		t.Run(fmt.Sprintf("%d", idx+1), func(t *testing.T) {
			var c SyncedCluster
			c.Name = tc.clusterName
			c.Tag = tc.tag
			if value := c.roachprodEnvValue(tc.node); value != tc.value {
				t.Errorf("expected value `%s`, got `%s`", tc.value, value)
			}
			if regex := c.roachprodEnvRegex(tc.node); regex != tc.regex {
				t.Errorf("expected regex `%s`, got `%s`", tc.regex, regex)
			}
		})
	}
}
