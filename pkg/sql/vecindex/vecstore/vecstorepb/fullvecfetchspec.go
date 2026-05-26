// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecstorepb

import "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"

// Import descpb package for the cast in the generated protobuf code.
var _ descpb.FamilyID
