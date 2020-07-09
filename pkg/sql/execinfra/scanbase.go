// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfra

import "github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"

// Prettier aliases for execinfrapb.ScanVisibility values.
const (
	ScanVisibilityPublic             = execinfrapb.ScanVisibility_PUBLIC
	ScanVisibilityPublicAndNotPublic = execinfrapb.ScanVisibility_PUBLIC_AND_NOT_PUBLIC
)
