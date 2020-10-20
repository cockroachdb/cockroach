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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
)

type activeRegions map[string]struct{}

func (s *activeRegions) isActive(region string) bool {
	_, ok := (*s)[region]
	return ok
}

func (s *activeRegions) toStrings() []string {
	ret := make([]string, 0, len(*s))
	for region := range *s {
		ret = append(ret, region)
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i] < ret[j]
	})
	return ret
}

// getActiveRegions returns a set of active region names in the cluster.
// A region name is deemed active if there is at least one alive node
// in the cluster in with locality set to a given region.
func (p *planner) getActiveRegions() (activeRegions, error) {
	// TODO(#multiregion): this currently excludes nodes which are no longer
	// active. Should they include inactive regions?
	nodes, err := getAllNodeDescriptors(p)
	if err != nil {
		return nil, err
	}
	var ret activeRegions = make(map[string]struct{})
	for _, node := range nodes {
		for _, tier := range node.Locality.Tiers {
			if tier.Key == "region" {
				ret[tier.Value] = struct{}{}
				break
			}
		}
	}
	return ret, nil
}

// checkCanAddRegion checks whether a region can be added.
func checkCanAddRegion(activeRegions activeRegions, region string) error {
	if !activeRegions.isActive(region) {
		return errors.WithHintf(
			pgerror.Newf(
				pgcode.InvalidName,
				"region %q does not exist",
				region,
			),
			"valid regions: %s",
			strings.Join(activeRegions.toStrings(), ", "),
		)
	}
	return nil
}
