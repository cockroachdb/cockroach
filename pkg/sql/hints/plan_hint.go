// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package hints

import (
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// NewPlanHints converts the raw bytes from system.plan_hints into a PlanHints
// object.
func NewPlanHints(bytes []byte) (*PlanHints, error) {
	res := &PlanHints{}
	if err := protoutil.Unmarshal(bytes, res); err != nil {
		return nil, err
	}
	return res, nil
}

// ToBytes converts the PlanHints to a raw bytes representation that can be
// inserted into the system.plan_hints table.
func (hints *PlanHints) ToBytes() ([]byte, error) {
	return protoutil.Marshal(hints)
}

// MergePlanHints combines the given PlanHints. If one is nil, MergePlanHints
// returns the other. If both are nil, MergePlanHints returns nil.
func MergePlanHints(l, r *PlanHints) *PlanHints {
	if l == nil {
		return r
	} else if r == nil {
		return l
	}
	ret := &PlanHints{
		Settings: make([]*SettingHint, 0, len(l.Settings)+len(r.Settings)),
	}
	ret.Settings = append(ret.Settings, l.Settings...)
	ret.Settings = append(ret.Settings, r.Settings...)

	// For AstHint, the right-hand side takes precedence (last one wins).
	if r.AstHint != nil {
		ret.AstHint = r.AstHint
	} else if l.AstHint != nil {
		ret.AstHint = l.AstHint
	}

	return ret
}

// FingerprintHashForPlanHints returns a 64-bit hash for the given query
// fingerprint to be used in the system.plan_hints table.
func FingerprintHashForPlanHints(queryFingerprint string) int64 {
	fnv := util.MakeFNV64()
	for _, c := range queryFingerprint {
		fnv.Add(uint64(c))
	}
	return int64(fnv.Sum())
}
