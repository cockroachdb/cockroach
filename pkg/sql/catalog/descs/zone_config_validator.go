// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
)

// ZoneConfigValidator is used to validate zone configs
type ZoneConfigValidator interface {
	ValidateDbZoneConfig(ctx context.Context, db catalog.DatabaseDescriptor) error
}
