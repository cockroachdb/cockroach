// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package failures

import (
	"fmt"
	"strings"
	"testing"

	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/stretchr/testify/require"
)

func TestGenericFailureHasPublicIP(t *testing.T) {
	failure := GenericFailure{c: &install.SyncedCluster{
		Cluster: cloudcluster.Cluster{VMs: vm.List{
			{PrivateIP: "10.0.0.1"},
			{PrivateIP: "10.0.0.2", PublicIP: "192.0.2.2"},
		}},
	}}

	hasPublicIP, err := failure.hasPublicIP(1)
	require.NoError(t, err)
	require.False(t, hasPublicIP)
	hasPublicIP, err = failure.hasPublicIP(2)
	require.NoError(t, err)
	require.True(t, hasPublicIP)
	_, err = failure.hasPublicIP(3)
	require.ErrorContains(t, err, "outside cluster of size 2")
}

func TestIPTablesPartitionRulesMatchPublicIPAvailability(t *testing.T) {
	for _, partitionType := range AllPartitionTypes {
		var privateCmd, publicCmd string
		switch partitionType {
		case Bidirectional:
			privateCmd, publicCmd = bidirectionalPartitionCmd, bidirectionalPublicPartitionCmd
		case Incoming:
			privateCmd, publicCmd = asymmetricInputPartitionCmd, asymmetricPublicInputPartitionCmd
		case Outgoing:
			privateCmd, publicCmd = asymmetricOutputPartitionCmd, asymmetricPublicOutputPartitionCmd
		default:
			t.Fatalf("unhandled partition type %s", partitionType)
		}
		for _, hasPublicIP := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/public=%t", partitionType, hasPublicIP), func(t *testing.T) {
				for _, addRule := range []bool{false, true} {
					cmd := constructIPTablesRule(privateCmd, publicCmd, 2, addRule, hasPublicIP)
					require.Contains(t, cmd, "{ip:2}")
					if hasPublicIP {
						require.Contains(t, cmd, "{ip:2:public}")
					} else {
						require.False(t, strings.Contains(cmd, ":public}"))
					}
				}
			})
		}
	}
}
