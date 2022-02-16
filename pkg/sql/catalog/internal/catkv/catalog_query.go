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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// lookupDescriptorsUnvalidated is a wrapper around the query method of
// catalogQuerier for descriptor table lookups.
func lookupDescriptorsUnvalidated(
	ctx context.Context, txn *kv.Txn, cq catalogQuerier, ids []descpb.ID,
) ([]catalog.Descriptor, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	cb, err := cq.query(ctx, txn, func(codec keys.SQLCodec, b *kv.Batch) {
		for _, id := range ids {
			key := catalogkeys.MakeDescMetadataKey(codec, id)
			b.Get(key)
		}
		if log.ExpensiveLogEnabled(ctx, 2) {
			log.Infof(ctx, "looking up unvalidated descriptors by id: %v", ids)
		}
	})
	if err != nil {
		return nil, err
	}
	ret := make([]catalog.Descriptor, len(ids))
	for i, id := range ids {
		desc := cb.LookupDescriptorEntry(id)
		if desc == nil {
			if cq.isRequired {
				return nil, wrapError(cq.expectedType, id, requiredError(cq.expectedType, id))
			}
			continue
		}
		ret[i] = desc
	}
	return ret, nil
}

// lookupIDs is a wrapper around the query method of catalogQuerier for
// namespace table lookups
func lookupIDs(
	ctx context.Context, txn *kv.Txn, cq catalogQuerier, nameInfos []descpb.NameInfo,
) ([]descpb.ID, error) {
	if len(nameInfos) == 0 {
		return nil, nil
	}
	cb, err := cq.query(ctx, txn, func(codec keys.SQLCodec, b *kv.Batch) {
		for _, nameInfo := range nameInfos {
			if nameInfo.Name == "" {
				continue
			}
			b.Get(catalogkeys.EncodeNameKey(codec, nameInfo))
		}
	})
	if err != nil {
		return nil, err
	}
	ret := make([]descpb.ID, len(nameInfos))
	for i, nameInfo := range nameInfos {
		id := cb.LookupNamespaceEntry(nameInfo)
		if id == descpb.InvalidID {
			if cq.isRequired {
				return nil, errors.AssertionFailedf("expected namespace entry for %s, none found", nameInfo.String())
			}
			continue
		}
		ret[i] = id
	}
	return ret, nil
}

type catalogQuerier struct {
	isRequired   bool
	expectedType catalog.DescriptorType
	codec        keys.SQLCodec
}

// query the catalog to retrieve data from the descriptor and namespace tables.
func (cq catalogQuerier) query(
	ctx context.Context, txn *kv.Txn, in func(codec keys.SQLCodec, b *kv.Batch),
) (nstree.Catalog, error) {
	b := txn.NewBatch()
	in(cq.codec, b)
	if err := txn.Run(ctx, b); err != nil {
		return nstree.Catalog{}, err
	}
	cb := &nstree.MutableCatalog{}
	for _, result := range b.Results {
		if result.Err != nil {
			return nstree.Catalog{}, result.Err
		}
		for _, row := range result.Rows {
			_, catTableID, err := cq.codec.DecodeTablePrefix(row.Key)
			if err != nil {
				return nstree.Catalog{}, err
			}
			switch catTableID {
			case keys.NamespaceTableID:
				err = cq.processNamespaceResultRow(row, cb)
			case keys.DescriptorTableID:
				err = cq.processDescriptorResultRow(row, cb)
			default:
				err = errors.AssertionFailedf("unexpected catalog key %s", row.Key.String())
			}
			if err != nil {
				return nstree.Catalog{}, err
			}
		}
	}
	return cb.Catalog, nil
}

func (cq catalogQuerier) processNamespaceResultRow(
	row kv.KeyValue, cb *nstree.MutableCatalog,
) error {
	nameInfo, err := catalogkeys.DecodeNameMetadataKey(cq.codec, row.Key)
	if err != nil {
		return err
	}
	if row.Exists() {
		cb.UpsertNamespaceEntry(nameInfo, descpb.ID(row.ValueInt()))
	}
	return nil
}

func (cq catalogQuerier) processDescriptorResultRow(
	row kv.KeyValue, cb *nstree.MutableCatalog,
) error {
	u32ID, err := cq.codec.DecodeDescMetadataID(row.Key)
	if err != nil {
		return err
	}
	id := descpb.ID(u32ID)
	desc, err := build(cq.expectedType, id, row.Value, cq.isRequired)
	if err != nil {
		return wrapError(cq.expectedType, id, err)
	}
	cb.UpsertDescriptorEntry(desc)
	return nil
}

func wrapError(expectedType catalog.DescriptorType, id descpb.ID, err error) error {
	switch expectedType {
	case catalog.Table:
		return catalog.WrapTableDescRefErr(id, err)
	case catalog.Database:
		return catalog.WrapDatabaseDescRefErr(id, err)
	case catalog.Schema:
		return catalog.WrapSchemaDescRefErr(id, err)
	case catalog.Type:
		return catalog.WrapTypeDescRefErr(id, err)
	}
	return errors.Wrapf(err, "referenced descriptor ID %d", id)
}

// build unmarshals and builds a descriptor from its value in the descriptor
// table.
func build(
	expectedType catalog.DescriptorType, id descpb.ID, rowValue *roachpb.Value, isRequired bool,
) (catalog.Descriptor, error) {
	var b catalog.DescriptorBuilder
	if rowValue != nil {
		var descProto descpb.Descriptor
		if err := rowValue.GetProto(&descProto); err != nil {
			return nil, err
		}
		b = descbuilder.NewBuilderWithMVCCTimestamp(&descProto, rowValue.Timestamp)
	}
	if b == nil {
		if isRequired {
			return nil, requiredError(expectedType, id)
		}
		return nil, nil
	}
	if expectedType != catalog.Any && b.DescriptorType() != expectedType {
		return nil, pgerror.Newf(pgcode.WrongObjectType, "descriptor is a %s", b.DescriptorType())
	}
	b.RunPostDeserializationChanges()
	desc := b.BuildImmutable()
	if id != desc.GetID() {
		return nil, errors.AssertionFailedf("unexpected ID %d in descriptor", desc.GetID())
	}
	return desc, nil
}

// requiredError returns an appropriate error when a descriptor which was
// required was not found.
func requiredError(expectedType catalog.DescriptorType, id descpb.ID) (err error) {
	switch expectedType {
	case catalog.Table:
		err = sqlerrors.NewUndefinedRelationError(&tree.TableRef{TableID: int64(id)})
	case catalog.Database:
		err = sqlerrors.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", id))
	case catalog.Schema:
		err = sqlerrors.NewUndefinedSchemaError(fmt.Sprintf("[%d]", id))
	case catalog.Type:
		err = sqlerrors.NewUndefinedTypeError(tree.NewUnqualifiedTypeName(fmt.Sprintf("[%d]", id)))
	default:
		err = errors.Errorf("failed to find descriptor [%d]", id)
	}
	return errors.CombineErrors(catalog.ErrDescriptorNotFound, err)
}
