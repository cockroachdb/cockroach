// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigreconciler

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigcatalog"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
)

var _ spanconfig.Reconciler = &Reconciler{}

// Reconciler is the cocnrete implementation of the ReconcilerI interface.
type Reconciler struct {
	execCfg *sql.ExecutorConfig
}

// NewReconciler returns a Reconciler.
func NewReconciler(config *sql.ExecutorConfig) *Reconciler {
	return &Reconciler{
		execCfg: config,
	}
}

func (r *Reconciler) FullReconcile(ctx context.Context) (entries []spanconfig.Entry, err error) {
	if err = descs.Txn(
		ctx,
		r.execCfg.Settings,
		r.execCfg.LeaseManager,
		r.execCfg.InternalExecutor,
		r.execCfg.DB,
		func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
			descs, err := descsCol.GetAllDescriptors(ctx, txn)
			ids := make(descpb.IDs, 0, len(descs))
			for _, desc := range descs {
				if desc.DescriptorType() == catalog.Table {
					ids = append(ids, desc.GetID())
				}
			}
			if err != nil {
				return err
			}
			entries, err = r.GenerateSpanConfigurations(ctx, txn, ids)
			return err
		}); err != nil {
		return nil, err
	}
	return entries, nil
}

// GenerateSpanConfigurations implements the Reconciler interface.
func (r *Reconciler) GenerateSpanConfigurations(
	ctx context.Context, txn *kv.Txn, ids descpb.IDs,
) ([]spanconfig.Entry, error) {
	// TODO(arul): Check if there is a utility for this/if this can go in a
	// utility.
	copyKey := func(k roachpb.Key) roachpb.Key {
		k2 := make([]byte, len(k))
		copy(k2, k)
		return k2
	}

	ret := make([]spanconfig.Entry, 0, len(ids))
	for _, id := range ids {
		zoneProto, err := sql.GetHydratedZoneConfigForTable(ctx, txn, r.execCfg.Codec, id)
		if err != nil {
			return nil, err
		}
		zone := spanconfigcatalog.NewZoneConfigFromProto(zoneProto)

		tablePrefix := r.execCfg.Codec.TablePrefix(uint32(id))

		prev := tablePrefix
		for i := range zone.SubzoneSpans {
			// We need to prepend the tablePrefix to the spans stored inside the
			// SubzoneSpans field because we store the stripped version here for
			// historical reasons.
			span := roachpb.Span{
				Key:    append(tablePrefix, zone.SubzoneSpans[i].Key...),
				EndKey: append(tablePrefix, zone.SubzoneSpans[i].EndKey...),
			}

			{
				// TODO(arul): Seems like the zone config code sets the EndKey to be nil
				// if the EndKey is Key.PrefixEnd() -- I'm not exactly sure why and still
				// need to investigate.
				if zone.SubzoneSpans[i].EndKey == nil {
					span.EndKey = span.Key.PrefixEnd()
				}
			}

			// If there is a "hole" in the spans covered by the subzones array we fill
			// it using the parent zone configuration.
			if !prev.Equal(span.Key) {
				ret = append(ret,
					spanconfig.MakeEntry(
						roachpb.Span{Key: copyKey(prev), EndKey: copyKey(span.Key)},
						zone,
					),
				)
			}

			// Add an entry for the subzone.
			subzoneZone := spanconfigcatalog.NewZoneConfigFromProto(&zone.Subzones[zone.SubzoneSpans[i].SubzoneIndex].Config)
			ret = append(ret,
				spanconfig.MakeEntry(
					roachpb.Span{Key: copyKey(span.Key), EndKey: copyKey(span.EndKey)},
					subzoneZone,
				),
			)

			prev = copyKey(span.EndKey)
		}

		if !prev.Equal(tablePrefix.PrefixEnd()) {
			ret = append(ret,
				spanconfig.MakeEntry(
					roachpb.Span{Key: prev, EndKey: tablePrefix.PrefixEnd()},
					zone,
				))
		}
	}

	return ret, nil
}
