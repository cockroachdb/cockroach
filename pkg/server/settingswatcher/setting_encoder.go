// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package settingswatcher

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// EncodeSettingKey encodes a key for the system.settings table, which
// can be used for direct KV operations.
func EncodeSettingKey(codec keys.SQLCodec, setting string) []byte {
	indexPrefix := codec.IndexPrefix(keys.SettingsTableID, uint32(1))
	return encoding.EncodeUvarintAscending(encoding.EncodeStringAscending(indexPrefix, setting), uint64(0))
}

// EncodeSettingValue encodes a value for the system.settings table, which
// can be used for direct KV operations.
func EncodeSettingValue(rawValue []byte, valueType string) ([]byte, error) {
	// Encode the setting value to write out the updated version.
	var tuple []byte
	var err error
	if tuple, err = valueside.Encode(tuple,
		valueside.MakeColumnIDDelta(descpb.ColumnID(encoding.NoColumnID),
			systemschema.SettingsTable.PublicColumns()[1].GetID()),
		tree.NewDString(string(rawValue)),
		nil); err != nil {
		return nil, err
	}
	if tuple, err = valueside.Encode(tuple,
		valueside.MakeColumnIDDelta(systemschema.SettingsTable.PublicColumns()[1].GetID(),
			systemschema.SettingsTable.PublicColumns()[2].GetID()),
		tree.MustMakeDTimestamp(timeutil.Now(), time.Microsecond),
		nil); err != nil {
		return nil, err
	}
	if tuple, err = valueside.Encode(tuple,
		valueside.MakeColumnIDDelta(systemschema.SettingsTable.PublicColumns()[2].GetID(),
			systemschema.SettingsTable.PublicColumns()[3].GetID()),
		tree.NewDString(valueType),
		nil); err != nil {
		return nil, err
	}
	return tuple, nil
}
