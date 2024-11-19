// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catkv

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
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
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/errors"
)

// catalogQuery holds the state necessary to perform a catalog query.
//
// Objects of this type should be exclusively created and used by catalogReader
// methods.
type catalogQuery struct {
	codec                keys.SQLCodec
	isDescriptorRequired bool
	expectedType         catalog.DescriptorType
}

// query the catalog to retrieve data from the descriptor and namespace tables.
//
// Any results pertaining to the system database are passed to the system
// database cache to potentially update it with them.
func (cq catalogQuery) query(
	ctx context.Context,
	txn *kv.Txn,
	out *nstree.MutableCatalog,
	in func(codec keys.SQLCodec, b *kv.Batch),
) error {
	if txn == nil {
		return errors.AssertionFailedf("nil txn for catalog query")
	}
	b := txn.NewBatch()
	in(cq.codec, b)
	if err := txn.Run(ctx, b); err != nil {
		return err
	}
	for _, result := range b.Results {
		if result.Err != nil {
			return result.Err
		}
		for _, row := range result.Rows {
			_, catTableID, err := cq.codec.DecodeTablePrefix(row.Key)
			if err != nil {
				return err
			}
			switch catTableID {
			case keys.NamespaceTableID:
				err = cq.processNamespaceResultRow(row, out)
			case keys.DescriptorTableID:
				err = cq.processDescriptorResultRow(row, out)
			case keys.CommentsTableID:
				err = cq.processCommentsResultRow(row, out)
			case keys.ZonesTableID:
				err = cq.processZonesResultRow(row, out)
			default:
				err = errors.AssertionFailedf("unexpected catalog key %s", row.Key.String())
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (cq catalogQuery) processNamespaceResultRow(row kv.KeyValue, cb *nstree.MutableCatalog) error {
	nameInfo, err := catalogkeys.DecodeNameMetadataKey(cq.codec, row.Key)
	if err != nil {
		return err
	}
	if row.Exists() {
		cb.UpsertNamespaceEntry(nameInfo, descpb.ID(row.ValueInt()), row.Value.Timestamp)
	}
	return nil
}

func (cq catalogQuery) processDescriptorResultRow(
	row kv.KeyValue, cb *nstree.MutableCatalog,
) error {
	u32ID, err := cq.codec.DecodeDescMetadataID(row.Key)
	if err != nil {
		return err
	}
	id := descpb.ID(u32ID)
	expectedType := cq.expectedType
	if expectedType == "" {
		expectedType = catalog.Any
	}
	desc, err := build(expectedType, id, row.Value, cq.isDescriptorRequired)
	if err != nil {
		return wrapError(expectedType, id, err)
	}
	cb.UpsertDescriptor(desc)
	return nil
}

func (cq catalogQuery) processCommentsResultRow(row kv.KeyValue, cb *nstree.MutableCatalog) error {
	remaining, cmtKey, err := catalogkeys.DecodeCommentMetadataID(cq.codec, row.Key)
	if err != nil {
		return err
	}
	_, famID, err := encoding.DecodeUvarintAscending(remaining)
	if err != nil {
		return err
	}

	// Skip the primary column family since only the comment string is interested.
	if famID != keys.CommentsTableCommentColFamID {
		return nil
	}
	return cb.UpsertComment(cmtKey, string(row.ValueBytes()))
}

func (cq catalogQuery) processZonesResultRow(row kv.KeyValue, cb *nstree.MutableCatalog) error {
	remaining, id, err := cq.codec.DecodeZoneConfigMetadataID(row.Key)
	if err != nil {
		return err
	}
	_, famID, err := encoding.DecodeUvarintAscending(remaining)
	if err != nil {
		return err
	}

	// Skip not interested column families or non-existing keys.
	if famID != keys.ZonesTableConfigColFamID || len(row.ValueBytes()) == 0 {
		return nil
	}

	var zoneConfig zonepb.ZoneConfig
	if err := row.ValueProto(&zoneConfig); err != nil {
		return errors.Wrapf(err, "decoding zone config for id %d", id)
	}
	cb.UpsertZoneConfig(descpb.ID(id), &zoneConfig, row.ValueBytes())
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
	b, err := descbuilder.FromSerializedValue(rowValue)
	if err != nil {
		return nil, err
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
	b.SetRawBytesInStorage(rowValue.TagAndDataBytes())
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
	return errors.CombineErrors(catalog.NewDescriptorNotFoundError(id), err)
}
