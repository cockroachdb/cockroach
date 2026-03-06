// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package license

import "github.com/cockroachdb/redact"

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
