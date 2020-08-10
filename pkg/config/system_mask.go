// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package config

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// SystemConfigMask is a mask that can be applied to a set of system config
// entries to filter out unwanted entries.
type SystemConfigMask struct {
	allowed []roachpb.Key
}

// MakeSystemConfigMask constructs a new SystemConfigMask that passes through
// only the specified keys when applied.
func MakeSystemConfigMask(allowed ...roachpb.Key) SystemConfigMask {
	sort.Slice(allowed, func(i, j int) bool {
		return allowed[i].Compare(allowed[j]) < 0
	})
	return SystemConfigMask{allowed: allowed}
}

// Apply applies the mask to the provided set of system config entries,
// returning a filtered down set of entries.
func (m SystemConfigMask) Apply(entries SystemConfigEntries) SystemConfigEntries {
	var res SystemConfigEntries
	for _, key := range m.allowed {
		i := sort.Search(len(entries.Values), func(i int) bool {
			return entries.Values[i].Key.Compare(key) >= 0
		})
		if i < len(entries.Values) && entries.Values[i].Key.Equal(key) {
			res.Values = append(res.Values, entries.Values[i])
		}
	}
	return res
}
