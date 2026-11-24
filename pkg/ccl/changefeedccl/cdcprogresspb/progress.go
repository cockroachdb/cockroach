// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cdcprogresspb

import (
	_ "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb" // Needed for progress.proto.
	_ "github.com/cockroachdb/cockroach/pkg/util/uuid"          // Needed for progress.proto.
)
