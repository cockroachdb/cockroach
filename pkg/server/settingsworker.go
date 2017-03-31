// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package server

import (
	"bytes"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

// RefreshSettings starts a settings-changes listener.
func (s *Server) refreshSettings() {
	tbl := sqlbase.SettingsTable

	a := &sqlbase.DatumAlloc{}
	settingsTablePrefix := keys.MakeTablePrefix(uint32(tbl.ID))
	colIdxMap := sqlbase.ColIDtoRowIndexFromCols(tbl.Columns)

	processKV := func(kv roachpb.KeyValue, u settings.Updater) error {
		if !bytes.HasPrefix(kv.Key, settingsTablePrefix) {
			return nil
		}

		var k, v, t string
		// First we need to decode the setting name field from the index key.
		{
			nameRow := []sqlbase.EncDatum{{Type: tbl.Columns[0].Type}}
			_, matches, err := sqlbase.DecodeIndexKey(a, &tbl, tbl.PrimaryIndex.ID, nameRow, nil, kv.Key)
			if err != nil {
				panic(errors.Wrap(err, "failed to decode key"))
			}
			if !matches {
				panic(errors.Errorf("unexpected non-settings KV with settings prefix: %v", kv.Key))
			}
			if err := nameRow[0].EnsureDecoded(a); err != nil {
				panic(err)
			}
			k = string(parser.MustBeDString(nameRow[0].Datum))
		}

		// The rest of the columns are stored as a family, packed with diff-encoded
		// column IDs followed by their values.
		{
			// column valueType can be null (missing) so we default it to "s".
			t = "s"
			bytes, err := kv.Value.GetTuple()
			if err != nil {
				panic(err)
			}
			var colIDDiff uint32
			var lastColID sqlbase.ColumnID
			var res parser.Datum
			for len(bytes) > 0 {
				_, _, colIDDiff, _, err = encoding.DecodeValueTag(bytes)
				if err != nil {
					panic(err)
				}
				colID := lastColID + sqlbase.ColumnID(colIDDiff)
				lastColID = colID
				if idx, ok := colIdxMap[colID]; ok {
					res, bytes, err = sqlbase.DecodeTableValue(a, tbl.Columns[idx].Type.ToDatumType(), bytes)
					if err != nil {
						panic(err)
					}
					switch colID {
					case tbl.Columns[1].ID: // value
						v = string(parser.MustBeDString(res))
					case tbl.Columns[3].ID: // valueType
						t = string(parser.MustBeDString(res))
					case tbl.Columns[2].ID: // lastUpdated
						// TODO(dt): we could decode just the len and then seek `bytes` past
						// it, without allocating/decoding the unused timestamp.
					default:
						panic(errors.Errorf("unknown column: %v", colID))
					}
				}
			}
		}

		return u.Add(k, v, t)
	}

	s.stopper.RunWorker(func() {
		gossipUpdateC := s.gossip.RegisterSystemConfigChannel()
		for {
			select {
			case <-gossipUpdateC:
				ctx := s.AnnotateCtx(context.Background())
				cfg, _ := s.gossip.GetSystemConfig()
				u := settings.MakeUpdater()
				for _, kv := range cfg.Values {
					if err := processKV(kv, u); err != nil {
						log.Warning(ctx, err)
					}
				}
				u.Apply()
			case <-s.stopper.ShouldStop():
				return
			}
		}
	})
}
