// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catalog

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
)

// IsSystemID returns true if the ID is part of the system database.
func IsSystemID(c keys.SystemIDChecker, id descpb.ID) bool {
	return c.IsSystemID(uint32(id))
}

// SystemIDChecker is a higher-level interface above the keys.SystemIDChecker.
// See the comments on that interface for semantics.
type SystemIDChecker struct {
	keys.SystemIDChecker
}

var _ keys.SystemIDChecker = SystemIDChecker{}

// IsSystemID returns true if the ID is part of the system database.
func (s SystemIDChecker) IsSystemID(id uint32) bool {
	return s.SystemIDChecker.IsSystemID(id)
}
