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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
	if level == NoValidation {
		return &vea
	}
	for _, desc := range descriptors {
		if level&ValidationLevelSelfOnly == 0 {
			continue
		}
		vea.setPrefix(desc)
		desc.ValidateSelf(&vea)
	}
	if level <= ValidationLevelSelfOnly || len(vea.errors) > 0 {
		return &vea
	}
	// Collect descriptors referenced by the validated descriptors.
	// These are their immediate neighbors in the reference graph, and in some
	// special cases those neighbors' immediate neighbors also.
	vdg, descGetterErr := collectDescriptorsForValidation(ctx, maybeBatchDescGetter, descriptors)
	if descGetterErr != nil {
		// Contrary to all other errors collected during Validate via vea.Report(),
		// the descGetterErr may be a transaction error, which may trigger retries.
		// It's therefore important that the combined error produced by the
		// returned ValidationErrors interface unwraps to descGetterErr. For this
		// reason we place it at the head of the errors slice.
		vea.errors = append(make([]error, 1, 1+len(vea.errors)), vea.errors...)
		vea.errors[0] = errors.Wrap(descGetterErr, "collecting referenced descriptors")
		return &vea
	}
	// Perform cross-reference checks.
	for _, desc := range descriptors {
		if level&ValidationLevelSelfAndCrossReferences == 0 || desc.Dropped() {
			continue
		}
		vea.setPrefix(desc)
		desc.ValidateCrossReferences(&vea, vdg)
	}
	if level <= ValidationLevelSelfAndCrossReferences {
		return &vea
	}
	// Perform pre-txn-commit checks.
	for _, desc := range descriptors {
		if level&ValidationLevelAllPreTxnCommit == 0 || desc.Dropped() {
			continue
		}
		vea.setPrefix(desc)
		desc.ValidateTxnCommit(&vea, vdg)
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
	// ValidationLevelSelfAndCrossReferences means do the above and also check
	// cross-references.
	ValidationLevelSelfAndCrossReferences
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
// ValidationLevelSelfAndCrossReferences level and combining the resulting errors.
func ValidateSelfAndCrossReferences(
	ctx context.Context, maybeBatchDescGetter DescGetter, descriptors ...Descriptor,
) error {
	return Validate(ctx, maybeBatchDescGetter, ValidationLevelSelfAndCrossReferences, descriptors...).CombinedError()
}

// ValidationErrorAccumulator is used by the validation methods on Descriptor
// to accumulate any encountered validation errors which are then processed by
// the Validate function.
// This interface is sealed to ensure that the validation methods only get
// called via the Validate function.
type ValidationErrorAccumulator interface {

	// Report is called by the validation methods to report a possible error.
	// No-ops when err is nil.
	Report(err error)

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

type validationErrorAccumulator struct {
	validationErrors
	wrapPrefix string
}

var _ ValidationErrorAccumulator = &validationErrorAccumulator{}

// Report implements the ValidationErrorAccumulator interface.
func (vea *validationErrorAccumulator) Report(err error) {
	if err == nil {
		return
	}
	if vea.wrapPrefix != "" {
		err = errors.Wrapf(err, "%s", vea.wrapPrefix)
	}
	vea.errors = append(vea.errors, err)
}

func (vea *validationErrorAccumulator) setPrefix(desc Descriptor) {
	vea.wrapPrefix = fmt.Sprintf("%s %q (%d)", desc.DescriptorType(), desc.GetName(), desc.GetID())
}

// ValidationDescGetter is used by the validation methods on Descriptor.
// This interface is sealed to ensure those methods only get called via
// the Validate function.
type ValidationDescGetter interface {

	// GetDatabaseDescriptor returns the corresponding DatabaseDescriptor or an error instead.
	GetDatabaseDescriptor(id descpb.ID) (DatabaseDescriptor, error)

	// GetSchemaDescriptor returns the corresponding SchemaDescriptor or an error instead.
	GetSchemaDescriptor(id descpb.ID) (SchemaDescriptor, error)

	// GetTableDescriptor returns the corresponding TableDescriptor or an error instead.
	GetTableDescriptor(id descpb.ID) (TableDescriptor, error)

	// GetTypeDescriptor returns the corresponding TypeDescriptor or an error instead.
	GetTypeDescriptor(id descpb.ID) (TypeDescriptor, error)

	// Seals this interface.
	sealed()
}

type validationDescGetterImpl MapDescGetter

var _ ValidationDescGetter = validationDescGetterImpl{}

// sealed implements the ValidationDescGetter interface.
func (validationDescGetterImpl) sealed() {}

// GetDatabaseDescriptor implements the ValidationDescGetter interface.
func (vdg validationDescGetterImpl) GetDatabaseDescriptor(
	id descpb.ID,
) (DatabaseDescriptor, error) {
	desc, found := vdg[id]
	if !found || desc == nil {
		return nil, WrapDatabaseDescRefErr(id, ErrDescriptorNotFound)
	}
	return AsDatabaseDescriptor(desc)
}

// GetSchemaDescriptor implements the ValidationDescGetter interface.
func (vdg validationDescGetterImpl) GetSchemaDescriptor(id descpb.ID) (SchemaDescriptor, error) {
	desc, found := vdg[id]
	if !found || desc == nil {
		return nil, WrapSchemaDescRefErr(id, ErrDescriptorNotFound)
	}
	return AsSchemaDescriptor(desc)
}

// GetTableDescriptor implements the ValidationDescGetter interface.
func (vdg validationDescGetterImpl) GetTableDescriptor(id descpb.ID) (TableDescriptor, error) {
	desc, found := vdg[id]
	if !found || desc == nil {
		return nil, WrapTableDescRefErr(id, ErrDescriptorNotFound)
	}
	return AsTableDescriptor(desc)
}

// GetTypeDescriptor implements the ValidationDescGetter interface.
func (vdg validationDescGetterImpl) GetTypeDescriptor(id descpb.ID) (TypeDescriptor, error) {
	desc, found := vdg[id]
	if !found || desc == nil {
		return nil, WrapTypeDescRefErr(id, ErrDescriptorNotFound)
	}
	return AsTypeDescriptor(desc)
}

// collectorState is used by collectDescriptorsForValidation
type collectorState struct {
	descs        validationDescGetterImpl
	referencedBy DescriptorIDSet
}

// addDirectReferences adds all immediate neighbors of desc to the state.
func (cs *collectorState) addDirectReferences(desc Descriptor) {
	cs.descs[desc.GetID()] = desc
	desc.GetReferencedDescIDs().ForEach(cs.referencedBy.Add)
}

// getMissingDescs fetches the descs which have corresponding IDs in the state
// but which are otherwise missing.
func (cs *collectorState) getMissingDescs(
	ctx context.Context, maybeBatchDescGetter DescGetter,
) (resps []Descriptor, err error) {
	reqs := make([]descpb.ID, 0, cs.referencedBy.Len())
	for _, id := range cs.referencedBy.Ordered() {
		if _, exists := cs.descs[id]; !exists {
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
			cs.descs[desc.GetID()] = desc
		}
	}
	return resps, nil
}

// collectDescriptorsForValidation is used by Validate to provide it with all
// possible descriptors required for validation.
func collectDescriptorsForValidation(
	ctx context.Context, maybeBatchDescGetter DescGetter, descriptors []Descriptor,
) (ValidationDescGetter, error) {
	cs := collectorState{
		descs:        make(map[descpb.ID]Descriptor, len(descriptors)),
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
	return cs.descs, nil
}
