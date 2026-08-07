// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/stretchr/testify/require"
)

func TestNetworkLatencyRulesMatchPublicIPAvailability(t *testing.T) {
	for _, hasPublicIP := range []bool{false, true} {
		t.Run(fmt.Sprintf("public=%t", hasPublicIP), func(t *testing.T) {
			cmd := constructLatencyFilterCmd(
				"eth0", 1, 10, 10*time.Millisecond, install.Node(2), hasPublicIP,
			)
			require.Contains(t, cmd, "{ip:2}/32")
			if hasPublicIP {
				require.Contains(t, cmd, "{ip:2:public}/32")
			} else {
				require.NotContains(t, cmd, ":public}")
			}
		})
	}
}
