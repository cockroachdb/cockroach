// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catalog

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
)

// ValidationLevel defines up to which degree to perform validation in Validate.
type ValidationLevel uint32

const (
	// NoValidation means don't perform any validation checks at all.
	NoValidation ValidationLevel = 0
	// ValidationLevelSelfOnly means only validate internal descriptor consistency.
	ValidationLevelSelfOnly ValidationLevel = 1<<(iota+1) - 1
	// ValidationLevelForwardReferences means do the above and also check
	// forward references.
	ValidationLevelForwardReferences
	// ValidationLevelBackReferences means do the above and also check
	// backward references.
	ValidationLevelBackReferences
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

	// GetFunctionDescriptor returns the corresponding FunctionDescriptor or an error instead.
	GetFunctionDescriptor(id descpb.ID) (FunctionDescriptor, error)

	// GetDescriptor returns the corresponding descriptor type or an error instead.
	GetDescriptor(id descpb.ID) (Descriptor, error)
}

// ValidateOutboundFunctionRef validates outbound reference to a function descriptor
// depID.
func ValidateOutboundFunctionRef(depID descpb.ID, vdg ValidationDescGetter) error {
	referencedFunction, err := vdg.GetFunctionDescriptor(depID)
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "invalid depends-on function reference")
	}
	if referencedFunction.Dropped() {
		return errors.AssertionFailedf("depends-on function %q (%d) is dropped",
			referencedFunction.GetName(), referencedFunction.GetID())
	}
	return nil
}

// ValidateOutboundTableRef validates outbound reference to relation descriptor
// depID from descriptor selfID.
func ValidateOutboundTableRef(depID descpb.ID, vdg ValidationDescGetter) error {
	referencedTable, err := vdg.GetTableDescriptor(depID)
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "invalid depends-on relation reference")
	}
	if referencedTable.Dropped() {
		return errors.AssertionFailedf("depends-on relation %q (%d) is dropped",
			referencedTable.GetName(), referencedTable.GetID())
	}
	return nil
}

// ValidateOutboundTableRefBackReference validates that the outbound reference
// to a table descriptor has a matching back-reference.
func ValidateOutboundTableRefBackReference(selfID descpb.ID, ref TableDescriptor) error {
	if ref == nil || ref.Dropped() {
		// Don't follow up on backward references for invalid or irrelevant forward
		// references.
		return nil
	}
	for _, by := range ref.TableDesc().DependedOnBy {
		if by.ID == selfID {
			return nil
		}
	}
	return errors.AssertionFailedf("depends-on relation %q (%d) has no corresponding depended-on-by back reference",
		ref.GetName(), ref.GetID())
}

// ValidateOutboundTypeRef validates outbound reference to type descriptor.
func ValidateOutboundTypeRef(typeID descpb.ID, vdg ValidationDescGetter) error {
	typ, err := vdg.GetTypeDescriptor(typeID)
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "invalid depends-on type reference")
	}
	if typ.Dropped() {
		return errors.AssertionFailedf("depends-on type %q (%d) is dropped",
			typ.GetName(), typ.GetID())
	}
	return nil
}

// ValidateOutboundTypeRefBackReference validates that the outbound reference
// to a type descriptor has a matching back-reference.
func ValidateOutboundTypeRefBackReference(selfID descpb.ID, typ TypeDescriptor) error {
	if typ == nil || typ.Dropped() {
		// Don't follow up on backward references for invalid or irrelevant forward
		// references.
		return nil
	}

	for ord := 0; ord < typ.NumReferencingDescriptors(); ord++ {
		if typ.GetReferencingDescriptorID(ord) == selfID {
			return nil
		}
	}
	return errors.AssertionFailedf("depends-on type %q (%d) has no corresponding referencing-descriptor back references",
		typ.GetName(), typ.GetID())
}

// ValidateRolesInDescriptor validates roles within a descriptor.
func ValidateRolesInDescriptor(
	descriptor Descriptor, RoleExists func(username username.SQLUsername) (bool, error),
) error {
	// Validate the owner.
	exists, err := RoleExists(descriptor.GetPrivileges().Owner())
	if err != nil {
		return err
	}
	if !exists {
		return errors.AssertionFailedf(
			"descriptor %q (%d) is owned by a role %q that doesn't exist",
			descriptor.GetName(), descriptor.GetID(), descriptor.GetPrivileges().Owner(),
		)
	}
	// Validate the privileges.
	for _, priv := range descriptor.GetPrivileges().Users {
		exists, err := RoleExists(priv.User())
		if err != nil {
			return err
		}
		if !exists {
			return errors.AssertionFailedf("descriptor %q (%d) has privilege on a role %q that doesn't exist",
				descriptor.GetName(),
				descriptor.GetID(),
				priv.User())
		}
	}
	// If a table descriptor, validate the roles that are stored in the policies.
	if tbDesc, isTable := descriptor.(TableDescriptor); isTable {
		for _, p := range tbDesc.GetPolicies() {
			for _, r := range p.RoleNames {
				exists, err := RoleExists(username.MakeSQLUsernameFromPreNormalizedString(r))
				if err != nil {
					return err
				}
				if !exists {
					return errors.AssertionFailedf("policy %q on table %q has a role %q that doesn't exist",
						p.Name, tbDesc.GetName(), r)
				}
			}
		}
	}
	return nil
}

// ValidateRolesInDefaultPrivilegeDescriptor validates roles within a
// catalog.DefaultPrivilegeDescriptor.
func ValidateRolesInDefaultPrivilegeDescriptor(
	defaultDesc DefaultPrivilegeDescriptor, roleExists func(username.SQLUsername) (bool, error),
) error {
	err := defaultDesc.ForEachDefaultPrivilegeForRole(func(defaultPriv catpb.DefaultPrivilegesForRole) error {
		// If we have a default privilege on a specific role, validate whether the
		// role exists.
		if defaultPriv.IsExplicitRole() {
			user := defaultPriv.GetExplicitRole().UserProto.Decode()
			exists, err := roleExists(user)
			if err != nil {
				return err
			}
			if !exists {
				return errors.AssertionFailedf("a default privilege exists on a role %q that doesn't exist",
					user)
			}
		}
		// Loop through to find which users have a default privilege assigned to
		// them. If any user does not exist, return an error.
		for _, privDesc := range defaultPriv.DefaultPrivilegesPerObject {
			for _, userPriv := range privDesc.Users {
				user := userPriv.User()
				exists, err := roleExists(user)
				if err != nil {
					return err
				}
				if !exists {
					return errors.AssertionFailedf("a default privilege exists for a role %q that doesn't exist",
						user)
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}
