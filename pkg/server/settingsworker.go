// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func processSystemConfigKVs(
	ctx context.Context, kvs []roachpb.KeyValue, u settings.Updater, eng storage.Engine,
) error {
	tbl := systemschema.SettingsTable

	a := &rowenc.DatumAlloc{}
	codec := keys.TODOSQLCodec
	settingsTablePrefix := codec.TablePrefix(uint32(tbl.ID))
	colIdxMap := row.ColIDtoRowIndexFromCols(tbl.Columns)

	var settingsKVs []roachpb.KeyValue
	processKV := func(ctx context.Context, kv roachpb.KeyValue, u settings.Updater) error {
		if !bytes.HasPrefix(kv.Key, settingsTablePrefix) {
			return nil
		}

		var k, v, t string
		// First we need to decode the setting name field from the index key.
		{
			types := []*types.T{tbl.Columns[0].Type}
			nameRow := make([]rowenc.EncDatum, 1)
			_, matches, _, err := rowenc.DecodeIndexKey(codec, tbl, tbl.GetPrimaryIndex().IndexDesc(), types, nameRow, nil, kv.Key)
			if err != nil {
				return errors.Wrap(err, "failed to decode key")
			}
			if !matches {
				return errors.Errorf("unexpected non-settings KV with settings prefix: %v", kv.Key)
			}
			if err := nameRow[0].EnsureDecoded(types[0], a); err != nil {
				return err
			}
			k = string(tree.MustBeDString(nameRow[0].Datum))
		}

		// The rest of the columns are stored as a family, packed with diff-encoded
		// column IDs followed by their values.
		{
			// column valueType can be null (missing) so we default it to "s".
			t = "s"
			bytes, err := kv.Value.GetTuple()
			if err != nil {
				return err
			}
			var colIDDiff uint32
			var lastColID descpb.ColumnID
			var res tree.Datum
			for len(bytes) > 0 {
				_, _, colIDDiff, _, err = encoding.DecodeValueTag(bytes)
				if err != nil {
					return err
				}
				colID := lastColID + descpb.ColumnID(colIDDiff)
				lastColID = colID
				if idx, ok := colIdxMap.Get(colID); ok {
					res, bytes, err = rowenc.DecodeTableValue(a, tbl.Columns[idx].Type, bytes)
					if err != nil {
						return err
					}
					switch colID {
					case tbl.Columns[1].ID: // value
						v = string(tree.MustBeDString(res))
					case tbl.Columns[3].ID: // valueType
						t = string(tree.MustBeDString(res))
					case tbl.Columns[2].ID: // lastUpdated
						// TODO(dt): we could decode just the len and then seek `bytes` past
						// it, without allocating/decoding the unused timestamp.
					default:
						return errors.Errorf("unknown column: %v", colID)
					}
				}
			}
		}
		settingsKVs = append(settingsKVs, kv)

		if err := u.Set(k, v, t); err != nil {
			log.Warningf(ctx, "setting %q to %q failed: %+v", k, v, err)
		}
		return nil
	}
	for _, kv := range kvs {
		if err := processKV(ctx, kv, u); err != nil {
			return errors.Wrap(err, `while decoding settings data
this likely indicates the settings table structure or encoding has been altered;
skipping settings updates`)
		}
	}
	u.ResetRemaining()
	return errors.Wrap(storeCachedSettingsKVs(ctx, eng, settingsKVs), "while storing settings kvs")
}

// RefreshSettings starts a settings-changes listener.
func (s *Server) refreshSettings(initialSettingsKVs []roachpb.KeyValue) error {
	ctx := s.AnnotateCtx(context.Background())
	// If we have initial settings KV pairs loaded from the local engines,
	// apply them before starting the gossip listener.
	if len(initialSettingsKVs) != 0 {
		if err := processSystemConfigKVs(ctx, initialSettingsKVs, s.st.MakeUpdater(), s.engines[0]); err != nil {
			return errors.Wrap(err, "during processing initial settings")
		}
	}
	// Setup updater that listens for changes in settings.
	s.stopper.RunWorker(ctx, func(ctx context.Context) {
		gossipUpdateC := s.gossip.RegisterSystemConfigChannel()
		// No new settings can be defined beyond this point.
		for {
			select {
			case <-gossipUpdateC:
				cfg := s.gossip.GetSystemConfig()
				u := s.st.MakeUpdater()
				if err := processSystemConfigKVs(ctx, cfg.Values, u, s.engines[0]); err != nil {
					log.Warningf(ctx, "error processing config KVs: %+v", err)
				}
			case <-s.stopper.ShouldStop():
				return
			}
		}
	})
	return nil
}
