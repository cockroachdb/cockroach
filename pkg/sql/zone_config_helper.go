// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type zoneConfigHelper interface {
	maybeGetTable(id descpb.ID) (catalog.TableDescriptor, error)
	maybeGetZoneConfig(id descpb.ID) (*zonepb.ZoneConfig, error)
}

type systemZoneConfigHelper struct {
	cfg   *config.SystemConfig
	codec keys.SQLCodec
}

// TODO(chengxiong): not sure if system config ever contains any table, can we
// just return nil?
func (h *systemZoneConfigHelper) maybeGetTable(id descpb.ID) (catalog.TableDescriptor, error) {
	val := h.cfg.GetValue(catalogkeys.MakeDescMetadataKey(h.codec, id))
	if val == nil {
		return nil, nil
	}

	b, err := descbuilder.FromSerializedValue(val)
	if err != nil {
		return nil, err
	}
	if b == nil || b.DescriptorType() != catalog.Table {
		return nil, nil
	}
	return b.BuildImmutable().(catalog.TableDescriptor), nil
}

func (h *systemZoneConfigHelper) maybeGetZoneConfig(id descpb.ID) (*zonepb.ZoneConfig, error) {
	val := h.cfg.GetValue(config.MakeZoneKey(h.codec, id))
	if val == nil {
		return nil, nil
	}

	var zone zonepb.ZoneConfig
	if err := val.GetProto(&zone); err != nil {
		return nil, err
	}
	return &zone, nil
}

type collectionZoneConfigHelper struct {
	ctx         context.Context
	txn         *kv.Txn
	descriptors *descs.Collection
}

func newCollectionZoneConfigHelper(
	ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
) *collectionZoneConfigHelper {
	return &collectionZoneConfigHelper{
		ctx:         ctx,
		txn:         txn,
		descriptors: descriptors,
	}
}

func (h *collectionZoneConfigHelper) maybeGetTable(id descpb.ID) (catalog.TableDescriptor, error) {
	if id == keys.RootNamespaceID || keys.IsPseudoTableID(uint32(id)) {
		return nil, nil
	}
	desc, err := h.descriptors.GetImmutableDescriptorByID(
		h.ctx,
		h.txn,
		id,
		tree.CommonLookupFlags{
			Required:       false,
			AvoidLeased:    true,
			IncludeOffline: true,
		},
	)
	if err != nil {
		return nil, err
	}
	if desc.DescriptorType() == catalog.Table {
		return desc.(catalog.TableDescriptor), nil
	}
	return nil, nil
}

func (h *collectionZoneConfigHelper) maybeGetZoneConfig(id descpb.ID) (*zonepb.ZoneConfig, error) {
	zone, err := h.descriptors.GetZoneConfigWithRawByte(h.ctx, h.txn, id)
	if err != nil {
		return nil, err
	}
	if zone != nil {
		return zone.ZoneConfig(), nil
	}
	return nil, nil
}
