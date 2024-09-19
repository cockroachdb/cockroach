// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multitenantccl

import (
	// Imports for the CCL init hooks.
	_ "github.com/cockroachdb/cockroach/pkg/ccl/multitenantccl/tenantcostclient"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/multitenantccl/tenantcostserver"
)
