// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package license

import (
	"time"

	"github.com/cockroachdb/redact"
)

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

// Edition represents the capability tier of the license. This is orthogonal
// to LicType, which represents the commercial relationship.
type Edition int

var _ redact.SafeValue = Edition(0)

// SafeValue implements the redact.SafeValue interface.
func (e Edition) SafeValue() {}

//go:generate stringer -type=Edition -linecomment
const (
	EditionUnspecified     Edition = iota // unspecified
	EditionStandard                       // standard
	EditionEnterprise                     // enterprise
	EditionMissionCritical                // mission-critical
)

// AddOn represents an optional feature bundle purchasable on top of an edition.
type AddOn int

var _ redact.SafeValue = AddOn(0)

// SafeValue implements the redact.SafeValue interface.
func (a AddOn) SafeValue() {}

//go:generate stringer -type=AddOn -linecomment
const (
	AddOnUnspecified        AddOn = iota // unspecified
	AddOnDataReplication                 // data-replication
	AddOnAdvancedWorkload                // advanced-workload-mgmt
	AddOnDataSync                        // data-synchronization
	AddOnAdvancedCompliance              // advanced-compliance
)

// LicenseInfo holds license metadata extracted from a decoded license proto.
// Used to pass license details to the enforcer without exposing proto types.
type LicenseInfo struct {
	Type         LicType
	Expiry       time.Time
	Edition      Edition
	AddOns       []AddOn
	VCPUEntitled int32
}
