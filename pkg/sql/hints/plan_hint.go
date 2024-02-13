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

import "github.com/cockroachdb/cockroach/pkg/util/protoutil"

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

// Equal returns true if the given PlanHints are equivalent.
func (hints *PlanHints) Equal(other *PlanHints) bool {
	if len(hints.Settings) != len(other.Settings) {
		return false
	}
	for i := range hints.Settings {
		if hints.Settings[i].SettingName != other.Settings[i].SettingName ||
			hints.Settings[i].SettingValue != other.Settings[i].SettingValue {
			return false
		}
	}
	return true
}
