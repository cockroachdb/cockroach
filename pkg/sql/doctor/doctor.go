// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package doctor provides utilities for checking the consistency of cockroach
// internal persisted metadata.
package doctor

import (
	"context"
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// DescriptorTableRow represents a descriptor from table system.descriptor.
type DescriptorTableRow struct {
	ID        int64
	DescBytes []byte
	ModTime   hlc.Timestamp
}

func key(id int64) roachpb.Key {
	return sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, descpb.ID(id))
}

// NewProtoGetter creates a sqlbase.MapProtoGetter from a descriptor table.
func NewProtoGetter(rows []DescriptorTableRow) (*sqlbase.MapProtoGetter, error) {
	pg := sqlbase.MapProtoGetter{
		Protos: make(map[interface{}]protoutil.Message),
	}
	for _, r := range rows {
		var d descpb.Descriptor
		if err := protoutil.Unmarshal(r.DescBytes, &d); err != nil {
			return nil, errors.Errorf("failed to unmarshal descriptor %d: %v", r.ID, err)
		}
		sqlbase.MaybeSetDescriptorModificationTimeFromMVCCTimestamp(context.Background(), &d, r.ModTime)
		pg.Protos[string(key(r.ID))] = &d
	}
	return &pg, nil
}

// Examine runs a suite of consistency checks over the descriptor table.
func Examine(descTable []DescriptorTableRow, verbose bool, stdout io.Writer) (ok bool, err error) {
	fmt.Fprintf(stdout, "Examining %d descriptors...\n", len(descTable))
	protoGetter, err := NewProtoGetter(descTable)
	if err != nil {
		return false, err
	}
	var problemsFound bool
	for _, row := range descTable {
		desc := protoGetter.Protos[string(key(row.ID))].(*descpb.Descriptor)
		t := sqlbase.TableFromDescriptor(desc, hlc.Timestamp{})
		if t == nil {
			// So far we only examine table descriptors. We may add checks for other
			// descriptors in later versions.
			continue
		}
		ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		sqlbase.MaybeSetDescriptorModificationTimeFromMVCCTimestamp(context.Background(), desc, ts)
		table := sqlbase.NewImmutableTableDescriptor(*sqlbase.TableFromDescriptor(desc, ts))
		if int64(table.ID) != row.ID {
			fmt.Fprintf(stdout, "Table %3d: different id in the descriptor: %d", row.ID, table.ID)
			problemsFound = true
			continue
		}
		if err := table.Validate(context.Background(), protoGetter, keys.SystemSQLCodec); err != nil {
			problemsFound = true
			fmt.Fprintf(stdout, "Table %3d: %s\n", table.ID, err)
		} else if verbose {
			fmt.Fprintf(stdout, "Table %3d: validated\n", table.ID)
		}
	}
	return !problemsFound, nil
}
