// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package license

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

// This file serves as a bridge to the license code in the CCL packages.
// Directly importing CCL is not possible, so this file maps functions
// and types from that package to something usable in this package.

// RegisterCallbackOnLicenseChange is a pointer to a function that will register
// a callback when the license changes. This is initially empty here. When
// initializing the ccl package, this variable will be set to a valid function.
var RegisterCallbackOnLicenseChange = func(context.Context, *cluster.Settings, *Enforcer) {}

// LicType is the type to define the license type, as needed by the license
// enforcer.
type LicType int

const (
	LicTypeNone LicType = iota
	LicTypeTrial
	LicTypeFree
	LicTypeEnterprise
	LicTypeEvaluation
)
