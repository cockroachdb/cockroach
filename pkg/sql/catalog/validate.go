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
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// Validate performs validation checks on the provided descriptors, up to the
// specified level.
// Some of these checks may require cross-referencing with other descriptors,
// hence the need for a ctx and a DescGetter. If the DescGetter is also a
// BatchDescGetter, then its batching functionality is used.
// If one of these checks surfaces an error, that error is wrapped with a prefix
// identifying the descriptor being validated.
// Validate returns a ValidationErrors interface which can provide the errors
// either as a slice or combined as one.
func Validate(
	ctx context.Context,
	maybeBatchDescGetter DescGetter,
	level ValidationLevel,
	descriptors ...Descriptor,
) ValidationErrors {
	// Check internal descriptor consistency.
	var vea validationErrorAccumulator
	handleDescGetterError := func(descGetterErr error, errPrefix string) {
		// Contrary to errors collected during Validate via vea.Report(),
		// the descGetterErr may be a transaction error, which may trigger retries.
		// It's therefore important that the combined error produced by the
		// returned ValidationErrors interface unwraps to descGetterErr. For this
		// reason we place it at the head of the errors slice.
		vea.errors = append(make([]error, 1, 1+len(vea.errors)), vea.errors...)
		vea.errors[0] = errors.Wrap(descGetterErr, errPrefix)
	}
	if level == NoValidation {
		return &vea
	}
	for _, desc := range descriptors {
		vea.withContext(ValidationLevelSelfOnly, level, desc, func() {
			desc.ValidateSelf(&vea)
		})
	}
	if level <= ValidationLevelSelfOnly || len(vea.errors) > 0 {
		return &vea
	}
	// Collect descriptors referenced by the validated descriptors.
	// These are their immediate neighbors in the reference graph, and in some
	// special cases those neighbors' immediate neighbors also.
	vdg, descGetterErr := collectDescriptorsForValidation(ctx, maybeBatchDescGetter, descriptors)
	if descGetterErr != nil {
		handleDescGetterError(descGetterErr, "collecting referenced descriptors")
		return &vea
	}
	// Perform cross-reference checks.
	for _, desc := range descriptors {
		vea.withContext(ValidationLevelCrossReferences, level, desc, func() {
			desc.ValidateCrossReferences(&vea, vdg)
		})
	}
	if level <= ValidationLevelCrossReferences {
		return &vea
	}
	if level&ValidationLevelNamespace != 0 {
		// Collect descriptor namespace table entries.
		descGetterErr = vdg.addNamespaceEntries(ctx, descriptors, maybeBatchDescGetter)
		if descGetterErr != nil {
			handleDescGetterError(descGetterErr, "collecting namespace table entries")
			return &vea
		}
		// Perform Namespace checks
		for _, desc := range descriptors {
			vea.withContext(ValidationLevelNamespace, level, desc, func() {
				validateNamespace(desc, &vea, vdg.Namespace)
			})
		}
	}
	if level <= ValidationLevelNamespace {
		return &vea
	}
	// Perform pre-txn-commit checks.
	for _, desc := range descriptors {
		vea.withContext(ValidationLevelNamespace, level, desc, func() {
			desc.ValidateTxnCommit(&vea, vdg)
		})
	}
	return &vea
}

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
	// pre-txn-commit checks.
	ValidationLevelAllPreTxnCommit
)

// ValidateSelf is a convenience function for validate called at the
// ValidationLevelSelfOnly level and combining the resulting errors.
func ValidateSelf(descriptors ...Descriptor) error {
	return Validate(context.TODO(), nil, ValidationLevelSelfOnly, descriptors...).CombinedError()
}

// ValidateSelfAndCrossReferences is a convenience function for Validate called at the
// ValidationLevelCrossReferences level and combining the resulting errors.
func ValidateSelfAndCrossReferences(
	ctx context.Context, maybeBatchDescGetter DescGetter, descriptors ...Descriptor,
) error {
	return Validate(ctx, maybeBatchDescGetter, ValidationLevelCrossReferences, descriptors...).CombinedError()
}

// ValidationTelemetryKeySuffix is a string type used when reporting errors to a
// ValidationErrorAccumulator. It's defined to encourage the use of shared
// constants as much as possible, as opposed to haphazard string literals.
type ValidationTelemetryKeySuffix string

// ValidationErrorAccumulator is used by the validation methods on Descriptor
// to accumulate any encountered validation errors which are then processed by
// the Validate function.
// This interface is sealed to ensure that the validation methods only get
// called via the Validate function.
type ValidationErrorAccumulator interface {

	// PushContextf pushes a telemetry and error wrap message prefix to the
	// accumulator context, narrowing the scope of the subsequently reported
	// errors.
	PushContextf(telemetryKeyPrefix string, errPrefixFmt string, args ...interface{})

	// PopContext undoes the effects of a PushContextf.
	// For safety, this should _always_ be called with defer right after a
	// PushContextf.
	PopContext()

	// Report is called by the validation methods to report a possible error.
	// Does nothing and returns false iff err is nil.
	Report(telemetryKeySuffix ValidationTelemetryKeySuffix, err error) bool

	// ReportPG is a convenience method for pgerror.New.
	ReportPG(telemetryKeySuffix ValidationTelemetryKeySuffix, code pgcode.Code, msg string) bool

	// ReportPG is a convenience method for pgerror.Newf.
	ReportPGf(telemetryKeySuffix ValidationTelemetryKeySuffix, code pgcode.Code, fmt string, args ...interface{}) bool

	// ReportInternalf is a convenience method for errors.AssertionFailedf.
	ReportInternalf(telemetryKeySuffix ValidationTelemetryKeySuffix, fmt string, args ...interface{}) bool

	// ReportUnimplemented is a convenience method for `unimplemented` errors.
	// These have no associated telemetry.
	ReportUnimplemented(issue int, msg string) bool

	// Seals this interface.
	sealed()
}

// ValidationErrors is the interface returned by Validate which contains
// all of the errors accumulated during validation.
type ValidationErrors interface {

	// Errors returns all of the errors accumulated during validation.
	Errors() []error

	// CombinedError returns all of the above reduced to one error.
	CombinedError() error

	// Seals this interface.
	sealed()
}

type validationErrors struct {
	errors []error
}

var _ ValidationErrors = &validationErrors{}

// sealed implements the ValidationErrors interface.
func (*validationErrors) sealed() {}

// Errors implements the ValidationErrors interface.
func (ve *validationErrors) Errors() []error {
	return ve.errors
}

// CombinedError implements the ValidationErrors interface.
func (ve *validationErrors) CombinedError() error {
	var combinedErr error
	for i := len(ve.errors) - 1; i >= 0; i-- {
		combinedErr = errors.CombineErrors(ve.errors[i], combinedErr)
	}
	return combinedErr
}

type veaContext struct {
	telemetry string
	fmt       string
	args      []interface{}
}

type validationErrorAccumulator struct {
	validationErrors
	contextStack []veaContext
}

var _ ValidationErrorAccumulator = &validationErrorAccumulator{}

// Report implements the ValidationErrorAccumulator interface.
func (vea *validationErrorAccumulator) Report(
	telemetryKeySuffix ValidationTelemetryKeySuffix, err error,
) bool {
	if err == nil {
		return false
	}
	t := make([]string, 0, 1+len(vea.contextStack))
	for _, c := range vea.contextStack {
		t = append(t, c.telemetry)
	}
	t = append(t, string(telemetryKeySuffix))
	vea.reportInternal(errors.WithTelemetry(err, strings.Join(t, ".")))
	return true
}

func (vea *validationErrorAccumulator) reportInternal(err error) {
	fmts := make([]string, 0, len(vea.contextStack))
	args := make([]interface{}, 0, 2*len(vea.contextStack))
	for _, c := range vea.contextStack {
		fmts = append(fmts, c.fmt)
		args = append(args, c.args...)
	}
	err = errors.Wrapf(err, strings.Join(fmts, ": "), args...)
	vea.errors = append(vea.errors, err)
}

// ReportUnimplemented implements the ValidationErrorAccumulator interface.
func (vea *validationErrorAccumulator) ReportUnimplemented(issue int, msg string) bool {
	vea.reportInternal(unimplemented.NewWithIssue(issue, msg))
	return false
}

// ReportPG implements the ValidationErrorAccumulator interface.
func (vea *validationErrorAccumulator) ReportPG(
	telemetryKeySuffix ValidationTelemetryKeySuffix, code pgcode.Code, msg string,
) bool {
	return vea.Report(telemetryKeySuffix, pgerror.New(code, msg))
}

// ReportPGf implements the ValidationErrorAccumulator interface.
func (vea *validationErrorAccumulator) ReportPGf(
	telemetryKeySuffix ValidationTelemetryKeySuffix,
	code pgcode.Code,
	fmt string,
	args ...interface{},
) bool {
	return vea.Report(telemetryKeySuffix, pgerror.Newf(code, fmt, args...))
}

// ReportInternalf implements the ValidationErrorAccumulator interface.
func (vea *validationErrorAccumulator) ReportInternalf(
	telemetryKeySuffix ValidationTelemetryKeySuffix, fmt string, args ...interface{},
) bool {
	return vea.Report(telemetryKeySuffix, errors.AssertionFailedf(fmt, args...))
}

// PushContextf implements the ValidationErrorAccumulator interface.
func (vea *validationErrorAccumulator) PushContextf(
	telemetryKeyPrefix string, fmt string, args ...interface{},
) {
	vea.contextStack = append(vea.contextStack, veaContext{
		telemetry: telemetryKeyPrefix,
		fmt:       fmt,
		args:      args,
	})
}

// PopContext implements the ValidationErrorAccumulator interface.
func (vea *validationErrorAccumulator) PopContext() {
	if len(vea.contextStack) > 0 {
		vea.contextStack = vea.contextStack[:len(vea.contextStack)-1]
	}
}

func (vea *validationErrorAccumulator) withContext(
	currentLevel ValidationLevel, level ValidationLevel, desc Descriptor, validation func(),
) {
	if level&currentLevel == 0 {
		return
	}
	rwScope := `read`
	if level == ValidationLevelAllPreTxnCommit {
		rwScope = `write`
	}
	levelScope, ok := map[ValidationLevel]string{
		ValidationLevelSelfOnly:        `self`,
		ValidationLevelCrossReferences: `crossref`,
		ValidationLevelNamespace:       `ns`,
		ValidationLevelAllPreTxnCommit: `pretxncommit`,
	}[currentLevel]
	if !ok {
		return
	}
	typ := string(desc.DescriptorType())
	telemetryKeyPrefix := fmt.Sprintf("sql.schema.validation.%s.%s.%s", rwScope, levelScope, typ)
	vea.PushContextf(telemetryKeyPrefix, typ+" %q (%d)", desc.GetName(), desc.GetID())
	defer vea.PopContext()
	validation()
}

// ValidationDescGetter is used by the validation methods on Descriptor.
// This interface is sealed to ensure those methods only get called via
// the Validate function.
type ValidationDescGetter interface {

	// GetDatabaseDescriptor returns the corresponding DatabaseDescriptor
	// or an internal error instead.
	GetDatabaseDescriptor(id descpb.ID) (DatabaseDescriptor, error)

	// GetSchemaDescriptor returns the corresponding SchemaDescriptor
	// or an internal error instead.
	GetSchemaDescriptor(id descpb.ID) (SchemaDescriptor, error)

	// GetTableDescriptor returns the corresponding TableDescriptor
	// or an internal error instead.
	GetTableDescriptor(id descpb.ID) (TableDescriptor, error)

	// GetTypeDescriptor returns the corresponding TypeDescriptor
	// or an internal error instead.
	GetTypeDescriptor(id descpb.ID) (TypeDescriptor, error)

	// Seals this interface.
	sealed()
}

type validationDescGetterImpl MapDescGetter

var _ ValidationDescGetter = (*validationDescGetterImpl)(nil)

// sealed implements the ValidationDescGetter interface.
func (*validationDescGetterImpl) sealed() {}

// GetDatabaseDescriptor implements the ValidationDescGetter interface.
func (vdg *validationDescGetterImpl) GetDatabaseDescriptor(
	id descpb.ID,
) (DatabaseDescriptor, error) {
	desc, found := vdg.Descriptors[id]
	if !found || desc == nil {
		return nil, errors.WithAssertionFailure(WrapDatabaseDescRefErr(id, ErrDescriptorNotFound))
	}
	return AsDatabaseDescriptor(desc)
}

// GetSchemaDescriptor implements the ValidationDescGetter interface.
func (vdg *validationDescGetterImpl) GetSchemaDescriptor(id descpb.ID) (SchemaDescriptor, error) {
	desc, found := vdg.Descriptors[id]
	if !found || desc == nil {
		return nil, errors.WithAssertionFailure(WrapSchemaDescRefErr(id, ErrDescriptorNotFound))
	}
	return AsSchemaDescriptor(desc)
}

// GetTableDescriptor implements the ValidationDescGetter interface.
func (vdg *validationDescGetterImpl) GetTableDescriptor(id descpb.ID) (TableDescriptor, error) {
	desc, found := vdg.Descriptors[id]
	if !found || desc == nil {
		return nil, errors.WithAssertionFailure(WrapTableDescRefErr(id, ErrDescriptorNotFound))
	}
	return AsTableDescriptor(desc)
}

// GetTypeDescriptor implements the ValidationDescGetter interface.
func (vdg *validationDescGetterImpl) GetTypeDescriptor(id descpb.ID) (TypeDescriptor, error) {
	desc, found := vdg.Descriptors[id]
	if !found || desc == nil {
		return nil, errors.WithAssertionFailure(WrapTypeDescRefErr(id, ErrDescriptorNotFound))
	}
	return AsTypeDescriptor(desc)
}

func (vdg *validationDescGetterImpl) addNamespaceEntries(
	ctx context.Context, descriptors []Descriptor, maybeBatchDescGetter DescGetter,
) (err error) {
	reqs := make([]descpb.NameInfo, 0, len(descriptors))
	for _, desc := range descriptors {
		reqs = append(reqs, descpb.NameInfo{
			ParentID:       desc.GetParentID(),
			ParentSchemaID: desc.GetParentSchemaID(),
			Name:           desc.GetName(),
		})
		reqs = append(reqs, desc.GetDrainingNames()...)
	}

	if bdg, ok := maybeBatchDescGetter.(BatchDescGetter); ok {
		ids, err := bdg.GetNamespaceEntries(ctx, reqs)
		if err != nil {
			return err
		}
		for i, r := range reqs {
			vdg.Namespace[r] = ids[i]
		}
		return nil
	}

	for _, r := range reqs {
		vdg.Namespace[r], err = maybeBatchDescGetter.GetNamespaceEntry(ctx, r.ParentID, r.ParentSchemaID, r.Name)
		if err != nil {
			return err
		}
	}
	return nil
}

// collectorState is used by collectDescriptorsForValidation
type collectorState struct {
	vdg          validationDescGetterImpl
	referencedBy DescriptorIDSet
}

// addDirectReferences adds all immediate neighbors of desc to the state.
func (cs *collectorState) addDirectReferences(desc Descriptor) {
	cs.vdg.Descriptors[desc.GetID()] = desc
	desc.GetReferencedDescIDs().ForEach(cs.referencedBy.Add)
}

// getMissingDescs fetches the descriptors which have corresponding IDs in the
// state but which are otherwise missing.
func (cs *collectorState) getMissingDescs(
	ctx context.Context, maybeBatchDescGetter DescGetter,
) (resps []Descriptor, err error) {
	reqs := make([]descpb.ID, 0, cs.referencedBy.Len())
	for _, id := range cs.referencedBy.Ordered() {
		if _, exists := cs.vdg.Descriptors[id]; !exists {
			reqs = append(reqs, id)
		}
	}
	if len(reqs) == 0 {
		return nil, nil
	}
	if bdg, ok := maybeBatchDescGetter.(BatchDescGetter); ok {
		resps, err = bdg.GetDescs(ctx, reqs)
		if err != nil {
			return nil, err
		}
	} else {
		resps = make([]Descriptor, len(reqs))
		for i, id := range reqs {
			resps[i], err = maybeBatchDescGetter.GetDesc(ctx, id)
			if err != nil {
				return nil, err
			}
		}
	}
	for _, desc := range resps {
		if desc != nil {
			cs.vdg.Descriptors[desc.GetID()] = desc
		}
	}
	return resps, nil
}

// collectDescriptorsForValidation is used by Validate to provide it with all
// possible descriptors required for validation.
func collectDescriptorsForValidation(
	ctx context.Context, maybeBatchDescGetter DescGetter, descriptors []Descriptor,
) (*validationDescGetterImpl, error) {
	cs := collectorState{
		vdg: validationDescGetterImpl{
			Descriptors: make(map[descpb.ID]Descriptor, len(descriptors)),
			Namespace:   make(map[descpb.NameInfo]descpb.ID, len(descriptors)),
		},
		referencedBy: MakeDescriptorIDSet(),
	}
	for _, desc := range descriptors {
		cs.addDirectReferences(desc)
	}
	newDescs, err := cs.getMissingDescs(ctx, maybeBatchDescGetter)
	if err != nil {
		return nil, err
	}
	for _, newDesc := range newDescs {
		if newDesc == nil {
			continue
		}
		switch newDesc.(type) {
		case DatabaseDescriptor, TypeDescriptor:
			cs.addDirectReferences(newDesc)
		}
	}
	_, err = cs.getMissingDescs(ctx, maybeBatchDescGetter)
	if err != nil {
		return nil, err
	}
	return &cs.vdg, nil
}

const (
	namespaceTableID  = 2
	namespace2TableID = 30
)

const (
	// Common ValidationTelemetryKeySuffix constants.

	// BadFormat counts encoding format failures.
	BadFormat ValidationTelemetryKeySuffix = `bad_format`
	// BadID counts invalid IDs.
	BadID = `bad_id`
	// BadName counts invalid names.
	BadName = `bad_name`
	// BadOrder counts invalid orderings.
	BadOrder = `bad_order`
	// BadParentID counts invalid parent IDs.
	BadParentID = `bad_parent_id`
	// BadParentSchemaID counts invalid parent schema IDs.
	BadParentSchemaID = `bad_parent_schema_id`
	// BadPrivileges counts invalid privilege descriptors.
	BadPrivileges = `bad_privileges`
	// BadState counts unspecified inconsistencies.
	BadState = `bad_state`
	// Mismatch counts unspecified mismatches.
	Mismatch = `mismatch`
	// NotFound counts invalid references.
	NotFound = `not_found`
	// NotSet counts things that should have been set.
	NotSet = `not_set`
	// NotUnique counts things that should have been unique.
	NotUnique = `not_unique`
	// NotUnset counts things that should not have been set.
	NotUnset = `not_unset`
	// Unclassified counts things which should never happen.
	Unclassified = `unclassified`
)

// validateNamespace checks that the namespace entries associated with a
// descriptor are sane.
func validateNamespace(
	desc Descriptor, vea ValidationErrorAccumulator, namespace map[descpb.NameInfo]descpb.ID,
) {
	if desc.GetID() == namespaceTableID || desc.GetID() == namespace2TableID {
		return
	}

	checkNamespaceEntry := func(nameInfo descpb.NameInfo) {
		id := namespace[nameInfo]
		if id == descpb.InvalidID {
			vea.ReportInternalf(NotFound, "expected matching namespace entry, found none")
		} else if id != desc.GetID() {
			vea.ReportInternalf(BadID, "expected matching namespace entry value, instead found %d", id)
		}
	}

	// Check that correct entry for descriptor exists and has correct value,
	// unless if the descriptor is dropped.
	if !desc.Dropped() {
		checkNamespaceEntry(descpb.NameInfo{
			ParentID:       desc.GetParentID(),
			ParentSchemaID: desc.GetParentSchemaID(),
			Name:           desc.GetName(),
		})
	}

	// Check that all draining name entries exist and are correct.
	for _, dn := range desc.GetDrainingNames() {
		func() {
			vea.PushContextf(`draining_name`, "draining name (%d, %d, %s)",
				dn.ParentID, dn.ParentSchemaID, dn.Name)
			defer vea.PopContext()
			checkNamespaceEntry(dn)
		}()
	}
}
