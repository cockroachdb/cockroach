// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

type liveClusterRegions map[descpb.Region]struct{}

func (s *liveClusterRegions) isActive(region descpb.Region) bool {
	_, ok := (*s)[region]
	return ok
}

func (s *liveClusterRegions) toStrings() []string {
	ret := make([]string, 0, len(*s))
	for region := range *s {
		ret = append(ret, string(region))
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i] < ret[j]
	})
	return ret
}

// getLiveClusterRegions returns a set of live region names in the cluster.
// A region name is deemed active if there is at least one alive node
// in the cluster in with locality set to a given region.
func (p *planner) getLiveClusterRegions() (liveClusterRegions, error) {
	nodes, err := getAllNodeDescriptors(p)
	if err != nil {
		return nil, err
	}
	var ret liveClusterRegions = make(map[descpb.Region]struct{})
	for _, node := range nodes {
		for _, tier := range node.Locality.Tiers {
			if tier.Key == "region" {
				ret[descpb.Region(tier.Value)] = struct{}{}
				break
			}
		}
	}
	return ret, nil
}

// checkLiveClusterRegion checks whether a region can be added to a database
// based on whether the cluster regions are alive.
func checkLiveClusterRegion(liveClusterRegions liveClusterRegions, region descpb.Region) error {
	if !liveClusterRegions.isActive(region) {
		return errors.WithHintf(
			pgerror.Newf(
				pgcode.InvalidName,
				"region %q does not exist",
				region,
			),
			"valid regions: %s",
			strings.Join(liveClusterRegions.toStrings(), ", "),
		)
	}
	return nil
}
