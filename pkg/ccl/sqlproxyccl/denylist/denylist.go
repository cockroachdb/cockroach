// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package denylist

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// Denylist represents an in-memory cache for the current denylist.
// It also handles the logic of deciding what to be denied.
type Denylist struct {
	timeSource timeutil.TimeSource
	entries    map[DenyEntity]*DenyEntry
}

// DenyEntry records info about one denied entity,
// the reason and the expiration time.
// This also serves as spec for the yaml config format.
type DenyEntry struct {
	Entity     DenyEntity `yaml:"entity"`
	Expiration time.Time  `yaml:"expiration"`
	Reason     string     `yaml:"reason"`
}

// DenyEntity represent one denied entity.
// This also serves as the spec for the config format.
type DenyEntity struct {
	Item string `yaml:"item"`
	Type Type   `yaml:"type"`
}

// Type is the type of the denied entity.
type Type int

// Enum values for Type.
const (
	IPAddrType Type = iota + 1
	ClusterType
	UnknownType
)

// Denied returns an error if the entity is denied access. The error message
// describes the reason for the denial.
func (dl *Denylist) Denied(entity DenyEntity) error {
	if ent, ok := dl.entries[entity]; ok &&
		(ent.Expiration.IsZero() || !ent.Expiration.Before(dl.timeSource.Now())) {
		return errors.Newf("%s", ent.Reason)
	}
	return nil
}

func emptyList() *Denylist {
	return &Denylist{
		timeSource: timeutil.DefaultTimeSource{},
		entries:    make(map[DenyEntity]*DenyEntry)}
}
