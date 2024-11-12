// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package license

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/redact"
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

var _ redact.SafeValue = LicType(0)

// SafeValue implements the redact.SafeValue interface.
func (i LicType) SafeValue() {}

//go:generate stringer -type=LicType -linecomment
const (
	LicTypeNone       LicType = iota // none
	LicTypeTrial                     // trial
	LicTypeFree                      // free
	LicTypeEnterprise                // enterprise
	LicTypeEvaluation                // evaluation
)
