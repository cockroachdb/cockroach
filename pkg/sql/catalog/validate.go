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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
)

// ValidationLevel defines up to which degree to perform validation in Validate.
type ValidationLevel uint32

const (
	// NoValidation means don't perform any validation checks at all.
	NoValidation ValidationLevel = 0
	// ValidationLevelSelfOnly means only validate internal descriptor consistency.
	ValidationLevelSelfOnly = 1<<(iota+1) - 1
	// ValidationLevelCrossReferences means do the above and also check
	// cross-references.
	ValidationLevelCrossReferences
	// ValidationLevelNamespace means do the above and also check namespace
	// table records.
	ValidationLevelNamespace
	// ValidationLevelAllPreTxnCommit means do the above and also perform
	// pre-txn-commit checks. This is the level of validation required when
	// writing a descriptor to storage.
	// Errors accumulated when validating up to this level come with additional
	// telemetry.
	ValidationLevelAllPreTxnCommit
)

// ValidationTelemetry defines the kind of telemetry keys to add to the errors.
type ValidationTelemetry int

const (
	// NoValidationTelemetry means no telemetry keys are added.
	NoValidationTelemetry ValidationTelemetry = iota
	// ValidationReadTelemetry means telemetry keys are added for descriptor
	// reads.
	ValidationReadTelemetry
	// ValidationWriteTelemetry means telemetry keys are added for descriptor
	// writes.
	ValidationWriteTelemetry
)

// ValidationErrorAccumulator is used by the validation methods on Descriptor
// to accumulate any encountered validation errors which are then processed by
// the Validate function.
type ValidationErrorAccumulator interface {

	// Report is called by the validation methods to report a possible error.
	// No-ops when err is nil.
	Report(err error)

	// IsActive is used to confirm if a certain version of validation should
	// be enabled, by comparing against the active version. If the active version
	// is unknown the validation in question will always be enabled.
	IsActive(version clusterversion.Key) bool
}

// ValidationErrors is the error container returned by Validate which contains
// all errors accumulated during validation.
type ValidationErrors []error

// CombinedError returns all errors reduced to one error.
func (ve ValidationErrors) CombinedError() error {
	var combinedErr error
	var extraTelemetryKeys []string
	for i := len(ve) - 1; i >= 0; i-- {
		combinedErr = errors.CombineErrors(ve[i], combinedErr)
	}
	// Decorate the combined error with all validation telemetry keys.
	// Otherwise, those not in the causal chain will be ignored.
	for _, err := range ve {
		for _, key := range errors.GetTelemetryKeys(err) {
			if strings.HasPrefix(key, telemetry.ValidationTelemetryKeyPrefix) {
				extraTelemetryKeys = append(extraTelemetryKeys, key)
			}
		}
	}
	if extraTelemetryKeys == nil {
		return combinedErr
	}
	return errors.WithTelemetry(combinedErr, extraTelemetryKeys...)
}

// ValidationDescGetter is used by the validation methods on Descriptor.
type ValidationDescGetter interface {

	// GetDatabaseDescriptor returns the corresponding DatabaseDescriptor or an error instead.
	GetDatabaseDescriptor(id descpb.ID) (DatabaseDescriptor, error)

	// GetSchemaDescriptor returns the corresponding SchemaDescriptor or an error instead.
	GetSchemaDescriptor(id descpb.ID) (SchemaDescriptor, error)

	// GetTableDescriptor returns the corresponding TableDescriptor or an error instead.
	GetTableDescriptor(id descpb.ID) (TableDescriptor, error)

	// GetTypeDescriptor returns the corresponding TypeDescriptor or an error instead.
	GetTypeDescriptor(id descpb.ID) (TypeDescriptor, error)
}
