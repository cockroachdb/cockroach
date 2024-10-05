// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/catkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/errors"
)

// Validate returns any descriptor validation errors after validating using the
// descriptor collection for retrieving referenced descriptors and namespace
// entries, if applicable.
func (tc *Collection) Validate(
	ctx context.Context,
	txn *kv.Txn,
	telemetry catalog.ValidationTelemetry,
	targetLevel catalog.ValidationLevel,
	descriptors ...catalog.Descriptor,
) (err error) {
	if !tc.validationModeProvider.ValidateDescriptorsOnRead() && !tc.validationModeProvider.ValidateDescriptorsOnWrite() {
		return nil
	}
	vd := tc.newValidationDereferencer(txn)
	version := tc.settings.Version.ActiveVersion(ctx)
	return validate.Validate(
		ctx,
		version,
		vd,
		telemetry,
		targetLevel,
		descriptors...).CombinedError()
}

// ValidateUncommittedDescriptors validates all uncommitted descriptors.
// Validation includes cross-reference checks. Referenced descriptors are
// read from the store unless they happen to also be part of the uncommitted
// descriptor set. We purposefully avoid using leased descriptors as those may
// be one version behind, in which case it's possible (and legitimate) that
// those are missing back-references which would cause validation to fail.
// Optionally, the zone config will be validated if validateZoneConfigs is
// set to true.
func (tc *Collection) ValidateUncommittedDescriptors(
	ctx context.Context,
	txn *kv.Txn,
	validateZoneConfigs bool,
	zoneConfigValidator ZoneConfigValidator,
) (err error) {
	if tc.skipValidationOnWrite || !tc.validationModeProvider.ValidateDescriptorsOnWrite() {
		return nil
	}
	var descs []catalog.Descriptor
	_ = tc.uncommitted.iterateUncommittedByID(func(desc catalog.Descriptor) error {
		descs = append(descs, desc)
		return nil
	})
	if len(descs) == 0 {
		return nil
	}
	if err := tc.Validate(ctx, txn, catalog.ValidationWriteTelemetry, validate.Write, descs...); err != nil {
		return err
	}
	// Next validate any zone configs that may have been modified
	// in the descriptor set, only if this type of validation is required.
	// We only do this type of validation if region configs are modified.
	if validateZoneConfigs {
		if zoneConfigValidator == nil {
			return errors.AssertionFailedf("zone config validator is required to " +
				"validate zone configs")
		}
		for _, desc := range descs {
			switch t := desc.(type) {
			case catalog.DatabaseDescriptor:
				if err = zoneConfigValidator.ValidateDbZoneConfig(ctx, t); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (tc *Collection) newValidationDereferencer(txn *kv.Txn) validate.ValidationDereferencer {
	crvd := catkv.NewCatalogReaderBackedValidationDereferencer(tc.cr, txn, tc.validationModeProvider)
	return &collectionBackedDereferencer{tc: tc, crvd: crvd}
}

// collectionBackedDereferencer wraps a Collection to implement the
// validate.ValidationDereferencer interface for validation.
type collectionBackedDereferencer struct {
	tc   *Collection
	crvd validate.ValidationDereferencer
}

var _ validate.ValidationDereferencer = &collectionBackedDereferencer{}

// DereferenceDescriptors implements the validate.ValidationDereferencer
// interface by leveraging the collection's uncommitted descriptors as well
// as its storage cache.
func (c collectionBackedDereferencer) DereferenceDescriptors(
	ctx context.Context, version clusterversion.ClusterVersion, reqs []descpb.ID,
) (ret []catalog.Descriptor, _ error) {
	ret = make([]catalog.Descriptor, len(reqs))
	fallbackReqs := make([]descpb.ID, 0, len(reqs))
	fallbackRetIndexes := make([]int, 0, len(reqs))
	for i, id := range reqs {
		if uc := c.tc.uncommitted.getUncommittedByID(id); uc == nil {
			fallbackReqs = append(fallbackReqs, id)
			fallbackRetIndexes = append(fallbackRetIndexes, i)
		} else {
			ret[i] = uc
		}
	}
	if len(fallbackReqs) == 0 {
		return ret, nil
	}
	fallbackRet, err := c.crvd.DereferenceDescriptors(ctx, version, fallbackReqs)
	if err != nil {
		return nil, err
	}
	for j, desc := range fallbackRet {
		ret[fallbackRetIndexes[j]] = desc
		if c.tc.validationModeProvider.ValidateDescriptorsOnRead() {
			c.tc.ensureValidationLevel(desc, catalog.ValidationLevelSelfOnly)
		}
	}
	return ret, nil
}

// DereferenceDescriptorIDs implements the validate.ValidationDereferencer
// interface by leveraging the collection's uncommitted descriptors as well
// as its storage cache.
func (c collectionBackedDereferencer) DereferenceDescriptorIDs(
	ctx context.Context, reqs []descpb.NameInfo,
) (ret []descpb.ID, _ error) {
	ret = make([]descpb.ID, len(reqs))
	fallbackReqs := make([]descpb.NameInfo, 0, len(reqs))
	fallbackRetIndexes := make([]int, 0, len(reqs))
	for i, ni := range reqs {
		if uc := c.tc.uncommitted.getUncommittedByName(ni.ParentID, ni.ParentSchemaID, ni.Name); uc == nil {
			fallbackReqs = append(fallbackReqs, ni)
			fallbackRetIndexes = append(fallbackRetIndexes, i)
		} else {
			ret[i] = uc.GetID()
		}
	}
	if len(fallbackReqs) == 0 {
		return ret, nil
	}
	fallbackRet, err := c.crvd.DereferenceDescriptorIDs(ctx, fallbackReqs)
	if err != nil {
		return nil, err
	}
	for j, id := range fallbackRet {
		ret[fallbackRetIndexes[j]] = id
	}
	return ret, nil
}

func (tc *Collection) ensureValidationLevel(
	desc catalog.Descriptor, newLevel catalog.ValidationLevel,
) {
	if desc == nil {
		return
	}
	if tc.validationLevels == nil {
		tc.validationLevels = make(map[descpb.ID]catalog.ValidationLevel)
	}
	vl, ok := tc.validationLevels[desc.GetID()]
	if ok && vl >= newLevel {
		return
	}
	tc.validationLevels[desc.GetID()] = newLevel
}

// ValidateSelf validates that the descriptor is internally consistent.
// Validation may be skipped depending on mode.
func ValidateSelf(
	desc catalog.Descriptor,
	version clusterversion.ClusterVersion,
	dvmp DescriptorValidationModeProvider,
) error {
	if !dvmp.ValidateDescriptorsOnRead() && !dvmp.ValidateDescriptorsOnWrite() {
		return nil
	}
	return validate.Self(version, desc)
}
