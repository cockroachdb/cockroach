// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"bytes"
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
)

// Distribution represents the physical distribution of data for a relational
// operator. It is used to describe where the data will be physically located
// during execution, to enable more accurate costing of each operator by taking
// latency and network bandwidth into account.
type Distribution struct {
	// Regions is the set of regions that make up this Distribution. They should
	// be sorted in lexicographical order.
	// TODO(rytaft): Consider abstracting this to a list of "neighborhoods" to
	// support more different types of localities.
	// TODO(rytaft): Consider mapping the region strings to integers and storing
	// this as a intsets.Fast.
	Regions []string
}

// Any is true if this Distribution allows any set of regions.
func (d Distribution) Any() bool {
	return len(d.Regions) == 0
}

func (d Distribution) String() string {
	var buf bytes.Buffer
	d.format(&buf)
	return buf.String()
}

func (d Distribution) format(buf *bytes.Buffer) {
	for i, r := range d.Regions {
		if i > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(r)
	}
}

// Equals returns true if the two Distributions are identical.
func (d Distribution) Equals(rhs Distribution) bool {
	if len(d.Regions) != len(rhs.Regions) {
		return false
	}

	for i := range d.Regions {
		if d.Regions[i] != rhs.Regions[i] {
			return false
		}
	}
	return true
}

// Union unions the other distribution with the given distribution,
// removing duplicates. It assumes both distributions are sorted.
func (d Distribution) Union(rhs Distribution) Distribution {
	regions := make([]string, 0, len(d.Regions)+len(rhs.Regions))
	l, r := 0, 0
	for l < len(d.Regions) && r < len(rhs.Regions) {
		if d.Regions[l] == rhs.Regions[r] {
			regions = append(regions, d.Regions[l])
			l++
			r++
		} else if d.Regions[l] < rhs.Regions[r] {
			regions = append(regions, d.Regions[l])
			l++
		} else {
			regions = append(regions, rhs.Regions[r])
			r++
		}
	}
	if l < len(d.Regions) {
		regions = append(regions, d.Regions[l:]...)
	} else if r < len(rhs.Regions) {
		regions = append(regions, rhs.Regions[r:]...)
	}
	return Distribution{Regions: regions}
}

const regionKey = "region"

// FromLocality sets the Distribution with the region from the given locality
// (if any).
func (d *Distribution) FromLocality(locality roachpb.Locality) {
	if region, ok := locality.Find(regionKey); ok {
		d.Regions = []string{region}
	}
}

// FromIndexScan sets the Distribution that results from scanning the given
// index with the given constraint c (c can be nil).
func (d *Distribution) FromIndexScan(
	ctx context.Context,
	evalCtx *eval.Context,
	tabMeta *opt.TableMeta,
	ord cat.IndexOrdinal,
	c *constraint.Constraint,
) {
	tab := tabMeta.Table
	index := tab.Index(ord)
	if index.Table().IsVirtualTable() {
		// Virtual tables do not have zone configurations.
		return
	}

	var regions map[string]struct{}
	for i, n := 0, index.PartitionCount(); i < n; i++ {
		part := index.Partition(i)

		// If the index scan is constrained, see if we can prune this partition.
		if c != nil {
			prefixes := part.PartitionByListPrefixes()
			var found bool
			for _, datums := range prefixes {
				if len(datums) == 0 {
					// This indicates a DEFAULT value, so we can't easily prune this partition.
					found = true
					break
				}
				key := constraint.MakeCompositeKey(datums...)
				var span constraint.Span
				span.Init(key, constraint.IncludeBoundary, key, constraint.IncludeBoundary)
				if c.IntersectsSpan(ctx, evalCtx, &span) {
					found = true
					break
				}
			}
			if !found {
				// This partition does not intersect the constraint, so skip it.
				continue
			}
		}

		// Add the regions from this partition to the distribution.
		zoneRegions := getRegionsFromZone(part.Zone())
		if len(regions) == 0 {
			regions = zoneRegions
		} else {
			for r := range zoneRegions {
				regions[r] = struct{}{}
			}
		}
	}

	// Populate the distribution for GLOBAL tables and REGIONAL BY TABLE.
	if len(regions) == 0 {
		regionsPopulated := false

		if tab.IsGlobalTable() {
			// Global tables can always be treated as local to the gateway region.
			gatewayRegion, found := evalCtx.Locality.Find("region")
			if found {
				regions = make(map[string]struct{})
				regions[gatewayRegion] = struct{}{}
				regionsPopulated = true
			}
		} else if homeRegion, ok := tab.HomeRegion(); ok {
			regions = make(map[string]struct{})
			regions[homeRegion] = struct{}{}
			regionsPopulated = true
		} else {
			// Use the leaseholder region(s), which should be the same as the home
			// region of REGIONAL BY TABLE tables.
			regions = getRegionsFromZone(index.Zone())
			regionsPopulated = regions != nil
		}
		if !regionsPopulated {
			// If the above methods failed to find a distribution, then the
			// distribution is all regions in the database.
			regionsNames, ok := tabMeta.GetRegionsInDatabase(ctx, evalCtx.Planner)
			if !ok && evalCtx.Planner != nil && evalCtx.Planner.EnforceHomeRegion() {
				err := pgerror.Newf(pgcode.QueryHasNoHomeRegion,
					"Query has no home region. Try accessing only tables defined in multi-region databases. %s",
					sqlerrors.EnforceHomeRegionFurtherInfo)
				panic(err)
			}
			if ok {
				regions = make(map[string]struct{})
				for _, regionName := range regionsNames {
					regions[string(regionName)] = struct{}{}
				}
			}
		}
	}

	// Convert to a slice and sort regions.
	d.Regions = make([]string, 0, len(regions))
	for r := range regions {
		d.Regions = append(d.Regions, r)
	}
	sort.Strings(d.Regions)
}

// GetSingleRegion returns the single region name of the distribution,
// if there is exactly one.
func (d *Distribution) GetSingleRegion() (region string, ok bool) {
	if d == nil {
		return "", false
	}
	if len(d.Regions) == 1 {
		return d.Regions[0], true
	}
	return "", false
}

// IsLocal returns true if this distribution matches
// the gateway region of the connection.
func (d *Distribution) IsLocal(evalCtx *eval.Context) bool {
	if d == nil {
		return false
	}
	gatewayRegion, foundLocalRegion := evalCtx.Locality.Find("region")
	if foundLocalRegion {
		if distributionRegion, ok := d.GetSingleRegion(); ok {
			return distributionRegion == gatewayRegion
		}
	}
	return false
}

// getRegionsFromZone returns the regions of the given zone config, if any. It
// attempts to find the smallest set of regions likely to hold the leaseholder.
func getRegionsFromZone(zone cat.Zone) map[string]struct{} {
	// First find any regional replica constraints. If there is exactly one, we
	// can return early.
	regions := getReplicaRegionsFromZone(zone)
	if len(regions) == 1 {
		return regions
	}

	// Next check the voter replica constraints. Once again, if there is exactly
	// one regional constraint, we can return early.
	var voterRegions map[string]struct{}
	for i, n := 0, zone.VoterConstraintsCount(); i < n; i++ {
		replicaConstraint := zone.VoterConstraint(i)
		for j, m := 0, replicaConstraint.ConstraintCount(); j < m; j++ {
			constraint := replicaConstraint.Constraint(j)
			if region, ok := getRegionFromConstraint(constraint); ok {
				if voterRegions == nil {
					voterRegions = make(map[string]struct{})
				}
				voterRegions[region] = struct{}{}
			}
		}
	}
	if len(voterRegions) == 1 {
		return voterRegions
	}

	// Use the lease preferences as a tie breaker. We only really care about the
	// first one, since subsequent lease preferences only apply in edge cases.
	if zone.LeasePreferenceCount() > 0 {
		leasePref := zone.LeasePreference(0)
		for i, n := 0, leasePref.ConstraintCount(); i < n; i++ {
			constraint := leasePref.Constraint(i)
			if region, ok := getRegionFromConstraint(constraint); ok {
				return map[string]struct{}{region: {}}
			}
		}
	}

	if len(voterRegions) > 0 {
		return voterRegions
	}
	return regions
}

// getRegionFromConstraint returns the region and ok=true if the given
// constraint is a required region constraint. Otherwise, returns ok=false.
func getRegionFromConstraint(constraint cat.Constraint) (region string, ok bool) {
	if constraint.GetKey() != regionKey {
		// We only care about constraints on the region.
		return "", false /* ok */
	}
	if constraint.IsRequired() {
		// The region is required.
		return constraint.GetValue(), true /* ok */
	}
	// The region is prohibited.
	return "", false /* ok */
}

// getReplicaRegionsFromZone returns the replica regions of the given zone
// config, if any.
func getReplicaRegionsFromZone(zone cat.Zone) map[string]struct{} {
	var regions map[string]struct{}
	for i, n := 0, zone.ReplicaConstraintsCount(); i < n; i++ {
		replicaConstraint := zone.ReplicaConstraints(i)
		for j, m := 0, replicaConstraint.ConstraintCount(); j < m; j++ {
			constraint := replicaConstraint.Constraint(j)
			if region, ok := getRegionFromConstraint(constraint); ok {
				if regions == nil {
					regions = make(map[string]struct{})
				}
				regions[region] = struct{}{}
			}
		}
	}
	return regions
}
