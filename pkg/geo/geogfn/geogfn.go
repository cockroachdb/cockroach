// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package geogfn contains functions that are used for geography-based builtins.
package geogfn

import (
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/golang/geo/s2"
)

// binaryPredicate is a generic wrapper for any binary predicate.
type binaryPredicate interface {
	// op returns true if aRegion op bRegion is true.
	op(aRegion s2.Region, bRegion s2.Region) (bool, error)
}

// doBinaryPredicate applies the binary predicate on a and b.
// In collections, any single item in collection A has a binary predicate to be true
// in all of collection B will return true.
func doBinaryPredicate(a *geo.Geography, b *geo.Geography, pred binaryPredicate) (bool, error) {
	aRegions, err := a.AsS2()
	if err != nil {
		return false, err
	}
	bRegions, err := b.AsS2()
	if err != nil {
		return false, err
	}
	for _, aRegion := range aRegions {
		result, err := doBinaryPredicateOnS2Regions(aRegion, bRegions, pred)
		if err != nil {
			return false, err
		}
		if result {
			return true, nil
		}
	}
	return false, nil
}

// doBinaryPredicateOnS2Regions returns true if the binary predicate is true
// for all bRegions on aRegion.
func doBinaryPredicateOnS2Regions(
	aRegion s2.Region, bRegions []s2.Region, pred binaryPredicate,
) (bool, error) {
	for _, bRegion := range bRegions {
		result, err := pred.op(aRegion, bRegion)
		if err != nil {
			return false, err
		}
		if !result {
			return false, nil
		}
	}
	return true, nil
}
