// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package helpers

import (
	"strings"

	"github.com/cockroachdb/errors"
	"golang.org/x/exp/maps"
)

const (
	DrtLarge = "drt-large"
	DrtChaos = "drt-chaos"
	DrtUA1   = "drt-ua1"
	DrtUA2   = "drt-ua2"
	DrtTest  = "drt-test" // only for testing!!

	DefaultProject = "cockroach-drt"
	DefaultDns     = "drt.crdb.io"

	DNSZone = "drt"
)

// clusterNames are all the supported drt cluster names
var clusterNames = map[string]struct{}{
	DrtLarge: {}, DrtChaos: {}, DrtUA1: {}, DrtUA2: {}, DrtTest: {},
}

// GetAllClusterNamesAsCSV returns the cluster names as CSV
func GetAllClusterNamesAsCSV() string {
	return strings.Join(maps.Keys(clusterNames), ", ")
}

// ValidateClusterName returns an error if the cluster name is not a valid cluster name
func ValidateClusterName(clusterName string) error {
	if _, ok := clusterNames[clusterName]; !ok {
		return errors.Newf("%s is not a valid cluster name. Supported cluster names: <%s>",
			clusterName, GetAllClusterNamesAsCSV())
	}
	return nil
}
