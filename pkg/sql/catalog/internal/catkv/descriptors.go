// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catkv

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// GetCatalogUnvalidated looks up and returns all available descriptors and
// namespace system table entries but does not validate anything.
func GetCatalogUnvalidated(
	ctx context.Context, txn *kv.Txn, codec keys.SQLCodec,
) (nstree.Catalog, error) {
	cq := catalogQuerier{
		isRequired:   true,
		expectedType: catalog.Any,
		codec:        codec,
	}
	log.Eventf(ctx, "fetching all descriptors and namespace entries")
	return cq.query(ctx, txn, func(codec keys.SQLCodec, b *kv.Batch) {
		b.Header.MaxSpanRequestKeys = 0
		descsPrefix := catalogkeys.MakeAllDescsMetadataKey(codec)
		b.Scan(descsPrefix, descsPrefix.PrefixEnd())
		nsPrefix := codec.IndexPrefix(
			uint32(systemschema.NamespaceTable.GetID()),
			uint32(systemschema.NamespaceTable.GetPrimaryIndexID()))
		b.Scan(nsPrefix, nsPrefix.PrefixEnd())
	})
}

func lookupDescriptorsAndValidate(
	ctx context.Context,
	txn *kv.Txn,
	cq catalogQuerier,
	version clusterversion.ClusterVersion,
	ids []descpb.ID,
) ([]catalog.Descriptor, error) {
	descs, err := lookupDescriptorsUnvalidated(ctx, txn, cq, ids)
	if err != nil || len(descs) == 0 {
		return nil, err
	}
	rvd := &readValidationDereferencer{
		catalogQuerier: catalogQuerier{
			expectedType: catalog.Any,
			codec:        cq.codec,
		},
		txn: txn,
	}
	ve := validate.Validate(ctx, version, rvd, catalog.ValidationReadTelemetry, catalog.ValidationLevelCrossReferences, descs...)
	if err := ve.CombinedError(); err != nil {
		return nil, err
	}
	return descs, nil
}

type readValidationDereferencer struct {
	catalogQuerier
	txn *kv.Txn
}

var _ validate.ValidationDereferencer = (*readValidationDereferencer)(nil)

// DereferenceDescriptors implements the validate.ValidationDereferencer
// interface.
func (t *readValidationDereferencer) DereferenceDescriptors(
	ctx context.Context, version clusterversion.ClusterVersion, reqs []descpb.ID,
) ([]catalog.Descriptor, error) {
	return GetCrossReferencedDescriptorsForValidation(ctx, t.txn, t.codec, version, reqs)
}

// DereferenceDescriptorIDs implements the validate.ValidationDereferencer
// interface.
func (t *readValidationDereferencer) DereferenceDescriptorIDs(
	ctx context.Context, requests []descpb.NameInfo,
) ([]descpb.ID, error) {
	return LookupIDs(ctx, t.txn, t.codec, requests)
}

// GetCrossReferencedDescriptorsForValidation looks up the descriptors given
// their IDs on a best-effort basis and validates that they are internally
// consistent.
// These can then be used for cross-reference validation of another descriptor.
func GetCrossReferencedDescriptorsForValidation(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	version clusterversion.ClusterVersion,
	ids []descpb.ID,
) ([]catalog.Descriptor, error) {
	cq := catalogQuerier{
		expectedType: catalog.Any,
		codec:        codec,
	}
	descs, err := lookupDescriptorsUnvalidated(ctx, txn, cq, ids)
	if err != nil || len(descs) == 0 {
		return nil, err
	}
	if err := validate.Self(version, descs...); err != nil {
		return nil, err
	}
	return descs, nil
}

// MaybeGetDescriptorByID looks up the descriptor given its ID,
// returning nil if the descriptor is not found.
func MaybeGetDescriptorByID(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	id descpb.ID,
	expectedType catalog.DescriptorType,
	version clusterversion.ClusterVersion,
) (catalog.Descriptor, error) {
	cq := catalogQuerier{
		expectedType: expectedType,
		codec:        codec,
	}
	descs, err := lookupDescriptorsAndValidate(ctx, txn, cq, version, []descpb.ID{id})
	if err != nil {
		return nil, err
	}
	return descs[0], nil
}

// MustGetDescriptorsByID looks up the descriptors given their IDs,
// returning an error if any descriptor is not found.
func MustGetDescriptorsByID(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	version clusterversion.ClusterVersion,
	ids []descpb.ID,
	expectedType catalog.DescriptorType,
) ([]catalog.Descriptor, error) {
	cq := catalogQuerier{
		codec:        codec,
		isRequired:   true,
		expectedType: expectedType,
	}
	return lookupDescriptorsAndValidate(ctx, txn, cq, version, ids)
}

// MustGetDescriptorByID looks up the descriptor given its ID,
// returning an error if the descriptor is not found.
func MustGetDescriptorByID(
	ctx context.Context,
	txn *kv.Txn,
	codec keys.SQLCodec,
	version clusterversion.ClusterVersion,
	id descpb.ID,
	expectedType catalog.DescriptorType,
) (catalog.Descriptor, error) {
	descs, err := MustGetDescriptorsByID(ctx, txn, codec, version, []descpb.ID{id}, expectedType)
	if err != nil {
		return nil, err
	}
	return descs[0], err
}
