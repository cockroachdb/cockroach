// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package validate contains all the descriptor validation logic.
package validate

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/errors"
)

// Self is a convenience function for Validate called at the
// ValidationLevelSelfOnly level and combining the resulting errors.
func Self(version clusterversion.ClusterVersion, descriptors ...catalog.Descriptor) error {
	results := Validate(context.TODO(), version, nil, catalog.NoValidationTelemetry, catalog.ValidationLevelSelfOnly, descriptors...)
	return results.CombinedError()
}

// Validate performs validation checks on the provided descriptors, up to the
// specified level.
// Some of these checks may require cross-referencing with other descriptors,
// hence the need for a ctx and a ValidationDereferencer.
// If one of these checks surfaces an error, that error is wrapped with a prefix
// identifying the descriptor being validated.
// Validate returns a catalog.ValidationErrors interface which can provide the
// errors either as a slice or combined as one.
func Validate(
	ctx context.Context,
	version clusterversion.ClusterVersion,
	vd ValidationDereferencer,
	telemetry catalog.ValidationTelemetry,
	targetLevel catalog.ValidationLevel,
	descriptors ...catalog.Descriptor,
) catalog.ValidationErrors {
	vea := validationErrorAccumulator{
		ValidationTelemetry: telemetry,
		targetLevel:         targetLevel,
		activeVersion:       version,
	}
	// Internal descriptor consistency checks.
	if !vea.validateDescriptorsAtLevel(
		catalog.ValidationLevelSelfOnly,
		descriptors,
		func(desc catalog.Descriptor) {
			desc.ValidateSelf(&vea)
			validateSchemaChangerState(desc, &vea)
		}) {
		return vea.errors
	}
	// Collect descriptors referenced by the validated descriptors.
	// These are their immediate neighbors in the reference graph, and in some
	// special cases those neighbors' immediate neighbors also.
	vdg, descGetterErr := collectDescriptorsForValidation(ctx, vd, version, descriptors)
	if descGetterErr != nil {
		vea.reportDescGetterError(collectingReferencedDescriptors, descGetterErr)
		return vea.errors
	}
	// Descriptor cross-reference checks.
	if !vea.validateDescriptorsAtLevel(
		catalog.ValidationLevelCrossReferences,
		descriptors,
		func(desc catalog.Descriptor) {
			if !desc.Dropped() {
				desc.ValidateCrossReferences(&vea, vdg)
			}
		}) {
		return vea.errors
	}
	// Collect descriptor namespace table entries, if running namespace checks.
	if catalog.ValidationLevelNamespace&targetLevel != 0 {
		descGetterErr = vdg.addNamespaceEntries(ctx, descriptors, vd)
		if descGetterErr != nil {
			vea.reportDescGetterError(collectingNamespaceEntries, descGetterErr)
			return vea.errors
		}
	}
	// Namespace validation checks
	if !vea.validateDescriptorsAtLevel(
		catalog.ValidationLevelNamespace,
		descriptors,
		func(desc catalog.Descriptor) {
			validateNamespace(desc, &vea, vdg.namespace)
		}) {
		return vea.errors
	}
	// Descriptor pre-txn-commit checks.
	_ = vea.validateDescriptorsAtLevel(
		catalog.ValidationLevelAllPreTxnCommit,
		descriptors,
		func(desc catalog.Descriptor) {
			if !desc.Dropped() {
				desc.ValidateTxnCommit(&vea, vdg)
			}
		})
	return vea.errors
}

// ValidationDereferencer is an interface to retrieve descriptors and namespace
// entries.
// Lookups are performed on a best-effort basis. When a descriptor or namespace
// entry cannot be found, the zero-value is returned, with no error.
type ValidationDereferencer interface {
	DereferenceDescriptors(ctx context.Context, version clusterversion.ClusterVersion, reqs []descpb.ID) ([]catalog.Descriptor, error)
	DereferenceDescriptorIDs(ctx context.Context, requests []descpb.NameInfo) ([]descpb.ID, error)
}

type validationErrorAccumulator struct {
	// Accumulated errors end up in here.
	errors catalog.ValidationErrors

	// The remaining fields represent the internal state of the Validate function
	// Used to decorate errors with appropriate prefixes and telemetry keys.
	catalog.ValidationTelemetry                         // set at initialization
	targetLevel                 catalog.ValidationLevel // set at initialization
	activeVersion               clusterversion.ClusterVersion
	currentState                validationErrorAccumulatorState
	currentLevel                catalog.ValidationLevel
	currentDescriptor           catalog.Descriptor
}

type validationErrorAccumulatorState int

const (
	validatingDescriptor validationErrorAccumulatorState = iota
	collectingReferencedDescriptors
	collectingNamespaceEntries
)

var _ catalog.ValidationErrorAccumulator = &validationErrorAccumulator{}

// Report implements the ValidationErrorAccumulator interface.
func (vea *validationErrorAccumulator) Report(err error) {
	if err == nil {
		return
	}
	vea.errors = append(vea.errors, vea.decorate(err))
}

func (vea *validationErrorAccumulator) validateDescriptorsAtLevel(
	level catalog.ValidationLevel,
	descs []catalog.Descriptor,
	validationFn func(descriptor catalog.Descriptor),
) bool {
	vea.currentState = validatingDescriptor
	vea.currentLevel = level
	if vea.currentLevel&vea.targetLevel != 0 {
		for _, desc := range descs {
			if desc == nil {
				continue
			}
			vea.currentDescriptor = desc
			validationFn(desc)
		}
	}
	vea.currentDescriptor = nil // ensures we don't needlessly hold a reference.
	// Stop validating when self-validation is unsuccessful.
	// This prevents panics in subsequent validation levels.
	if level == catalog.ValidationLevelSelfOnly && len(vea.errors) > 0 {
		return false
	}
	// Stop validating when target level is reached.
	if vea.targetLevel <= vea.currentLevel {
		return false
	}
	return true
}

// IsActive implements the ValidationErrorAccumulator interface.
func (vea *validationErrorAccumulator) IsActive(version clusterversion.Key) bool {
	return vea.activeVersion.IsActive(version)
}

func (vea *validationErrorAccumulator) reportDescGetterError(
	state validationErrorAccumulatorState, err error,
) {
	vea.currentState = state
	// Contrary to errors collected during Validate via vea.Report(), this error
	// may be a transaction error, which may trigger retries.  It's therefore
	// important that the combined error produced by the returned ValidationErrors
	// interface unwraps to this error. For this reason we place it at the head of
	// the errors slice.
	vea.errors = append(make([]error, 1, 1+len(vea.errors)), vea.errors...)
	vea.errors[0] = vea.decorate(err)
}

func (vea *validationErrorAccumulator) decorate(err error) error {
	var tkSuffix string
	switch vea.currentState {
	case collectingReferencedDescriptors:
		err = errors.Wrap(err, "collecting referenced descriptors")
		tkSuffix = "read_referenced_descriptors"
	case collectingNamespaceEntries:
		err = errors.Wrap(err, "collecting namespace table entries")
		tkSuffix = "read_namespace_table"
	case validatingDescriptor:
		name := vea.currentDescriptor.GetName()
		id := vea.currentDescriptor.GetID()
		// This contrived switch case is required to make the linter happy.
		switch vea.currentDescriptor.DescriptorType() {
		case catalog.Table:
			err = errors.Wrapf(err, catalog.Table+" %q (%d)", name, id)
		case catalog.Database:
			err = errors.Wrapf(err, catalog.Database+" %q (%d)", name, id)
		case catalog.Schema:
			err = errors.Wrapf(err, catalog.Schema+" %q (%d)", name, id)
		case catalog.Type:
			err = errors.Wrapf(err, catalog.Type+" %q (%d)", name, id)
		default:
			return err
		}
		switch vea.currentLevel {
		case catalog.ValidationLevelSelfOnly:
			tkSuffix = "self"
		case catalog.ValidationLevelCrossReferences:
			tkSuffix = "cross_references"
		case catalog.ValidationLevelNamespace:
			tkSuffix = "namespace"
		case catalog.ValidationLevelAllPreTxnCommit:
			tkSuffix = "pre_txn_commit"
		default:
			return err
		}
		tkSuffix += "." + string(vea.currentDescriptor.DescriptorType())
	}
	switch vea.ValidationTelemetry {
	case catalog.ValidationReadTelemetry:
		tkSuffix = "read." + tkSuffix
	case catalog.ValidationWriteTelemetry:
		tkSuffix = "write." + tkSuffix
	default:
		return err
	}
	return errors.WithTelemetry(err, telemetry.ValidationTelemetryKeyPrefix+tkSuffix)
}

type validationDescGetterImpl struct {
	descriptors map[descpb.ID]catalog.Descriptor
	namespace   map[descpb.NameInfo]descpb.ID
}

var _ catalog.ValidationDescGetter = (*validationDescGetterImpl)(nil)

// GetDatabaseDescriptor implements the ValidationDescGetter interface.
func (vdg *validationDescGetterImpl) GetDatabaseDescriptor(
	id descpb.ID,
) (catalog.DatabaseDescriptor, error) {
	desc, found := vdg.descriptors[id]
	if !found || desc == nil {
		return nil, catalog.WrapDatabaseDescRefErr(id, catalog.ErrReferencedDescriptorNotFound)
	}
	return catalog.AsDatabaseDescriptor(desc)
}

// GetSchemaDescriptor implements the ValidationDescGetter interface.
func (vdg *validationDescGetterImpl) GetSchemaDescriptor(
	id descpb.ID,
) (catalog.SchemaDescriptor, error) {
	desc, found := vdg.descriptors[id]
	if !found || desc == nil {
		return nil, catalog.WrapSchemaDescRefErr(id, catalog.ErrReferencedDescriptorNotFound)
	}
	return catalog.AsSchemaDescriptor(desc)
}

// GetTableDescriptor implements the ValidationDescGetter interface.
func (vdg *validationDescGetterImpl) GetTableDescriptor(
	id descpb.ID,
) (catalog.TableDescriptor, error) {
	desc, found := vdg.descriptors[id]
	if !found || desc == nil {
		return nil, catalog.WrapTableDescRefErr(id, catalog.ErrReferencedDescriptorNotFound)
	}
	return catalog.AsTableDescriptor(desc)
}

// GetTypeDescriptor implements the ValidationDescGetter interface.
func (vdg *validationDescGetterImpl) GetTypeDescriptor(
	id descpb.ID,
) (catalog.TypeDescriptor, error) {
	desc, found := vdg.descriptors[id]
	if !found || desc == nil {
		return nil, catalog.WrapTypeDescRefErr(id, catalog.ErrReferencedDescriptorNotFound)
	}
	descriptor, err := catalog.AsTypeDescriptor(desc)
	if err != nil {
		return nil, err
	}
	return descriptor, err
}

func (vdg *validationDescGetterImpl) addNamespaceEntries(
	ctx context.Context, descriptors []catalog.Descriptor, vd ValidationDereferencer,
) error {
	reqs := make([]descpb.NameInfo, 0, len(descriptors))
	for _, desc := range descriptors {
		if desc == nil {
			continue
		}
		reqs = append(reqs, descpb.NameInfo{
			ParentID:       desc.GetParentID(),
			ParentSchemaID: desc.GetParentSchemaID(),
			Name:           desc.GetName(),
		})
		reqs = append(reqs, desc.GetDrainingNames()...)
	}

	ids, err := vd.DereferenceDescriptorIDs(ctx, reqs)
	if err != nil {
		return err
	}
	for i, r := range reqs {
		vdg.namespace[r] = ids[i]
	}
	return nil
}

// collectorState is used by collectDescriptorsForValidation
type collectorState struct {
	vdg          validationDescGetterImpl
	referencedBy catalog.DescriptorIDSet
}

// addDirectReferences adds all immediate neighbors of desc to the state.
func (cs *collectorState) addDirectReferences(desc catalog.Descriptor) error {
	cs.vdg.descriptors[desc.GetID()] = desc
	idSet, err := desc.GetReferencedDescIDs()
	if err != nil {
		return err
	}
	idSet.ForEach(cs.referencedBy.Add)
	return nil
}

// getMissingDescs fetches the descriptors which have corresponding IDs in the
// state but which are otherwise missing.
func (cs *collectorState) getMissingDescs(
	ctx context.Context, vd ValidationDereferencer, version clusterversion.ClusterVersion,
) ([]catalog.Descriptor, error) {
	reqs := make([]descpb.ID, 0, cs.referencedBy.Len())
	for _, id := range cs.referencedBy.Ordered() {
		if _, exists := cs.vdg.descriptors[id]; !exists {
			reqs = append(reqs, id)
		}
	}
	if len(reqs) == 0 {
		return nil, nil
	}
	resps, err := vd.DereferenceDescriptors(ctx, version, reqs)
	if err != nil {
		return nil, err
	}
	for _, desc := range resps {
		if desc != nil {
			cs.vdg.descriptors[desc.GetID()] = desc
		}
	}
	return resps, nil
}

// collectDescriptorsForValidation is used by Validate to provide it with all
// possible descriptors required for validation.
func collectDescriptorsForValidation(
	ctx context.Context,
	vd ValidationDereferencer,
	version clusterversion.ClusterVersion,
	descriptors []catalog.Descriptor,
) (*validationDescGetterImpl, error) {
	cs := collectorState{
		vdg: validationDescGetterImpl{
			descriptors: make(map[descpb.ID]catalog.Descriptor, len(descriptors)),
			namespace:   make(map[descpb.NameInfo]descpb.ID, len(descriptors)),
		},
		referencedBy: catalog.MakeDescriptorIDSet(),
	}
	for _, desc := range descriptors {
		if desc == nil {
			continue
		}
		if err := cs.addDirectReferences(desc); err != nil {
			return nil, err
		}
	}
	newDescs, err := cs.getMissingDescs(ctx, vd, version)
	if err != nil {
		return nil, err
	}
	for _, newDesc := range newDescs {
		if newDesc == nil {
			continue
		}
		switch newDesc.(type) {
		case catalog.DatabaseDescriptor, catalog.TypeDescriptor:
			if err := cs.addDirectReferences(newDesc); err != nil {
				return nil, err
			}
		}
	}
	_, err = cs.getMissingDescs(ctx, vd, version)
	if err != nil {
		return nil, err
	}
	return &cs.vdg, nil
}

// validateNamespace checks that the namespace entries associated with a
// descriptor are sane.
func validateNamespace(
	desc catalog.Descriptor,
	vea catalog.ValidationErrorAccumulator,
	namespace map[descpb.NameInfo]descpb.ID,
) {
	if desc.GetID() == keys.NamespaceTableID || desc.GetID() == keys.DeprecatedNamespaceTableID {
		return
	}

	key := descpb.NameInfo{
		ParentID:       desc.GetParentID(),
		ParentSchemaID: desc.GetParentSchemaID(),
		Name:           desc.GetName(),
	}
	id := namespace[key]
	// Check that correct entry for descriptor exists and has correct value,
	// unless if the descriptor is dropped.
	if !desc.Dropped() {
		if id == descpb.InvalidID {
			vea.Report(errors.Errorf("expected matching namespace entry, found none"))
		} else if id != desc.GetID() {
			vea.Report(errors.Errorf("expected matching namespace entry value, instead found %d", id))
		}
	}

	// Check that all draining name entries exist and are correct.
	for _, dn := range desc.GetDrainingNames() {
		id := namespace[dn]
		if id == descpb.InvalidID {
			vea.Report(errors.Errorf("expected matching namespace entry for draining name (%d, %d, %s), found none",
				dn.ParentID, dn.ParentSchemaID, dn.Name))
		} else if id != desc.GetID() {
			vea.Report(errors.Errorf("expected matching namespace entry value for draining name (%d, %d, %s), instead found %d",
				dn.ParentID, dn.ParentSchemaID, dn.Name, id))
		}
	}
}
