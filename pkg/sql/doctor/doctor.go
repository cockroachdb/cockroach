// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package doctor

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type DescriptorTableRow struct {
	ID        int64
	DescBytes []byte
	ModTime   hlc.Timestamp
}

func key(id int64) roachpb.Key {
	return sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, descpb.ID(id))
}

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

func Examine(descTable []DescriptorTableRow, verbose bool) error {
	fmt.Printf("Examing %d descriptors: ", len(descTable))
	protoGetter, err := NewProtoGetter(descTable)
	if err != nil {
		return err
	}
	failed := make(map[descpb.ID]error)
	for _, row := range descTable {
		fmt.Printf("%d, ", row.ID)
		desc := protoGetter.Protos[string(key(row.ID))].(*descpb.Descriptor)
		t := desc.GetTable()
		if t == nil {
			continue
		}
		ts := hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		sqlbase.MaybeSetDescriptorModificationTimeFromMVCCTimestamp(context.Background(), desc, ts)
		table := sqlbase.NewImmutableTableDescriptor(*sqlbase.TableFromDescriptor(desc, ts))
		if int64(table.ID) != row.ID {
			failed[table.ID] =
				errors.Errorf("Table %d has a different id in the descriptor: %d", row.ID, table.ID)
			continue
		}
		if err := table.Validate(context.Background(), protoGetter, keys.SystemSQLCodec); err != nil {
			failed[table.ID] = err
			fmt.Printf("Table %4d: FAIL %v\n", table.ID, err)
			prettyPrint(table)
		} else if verbose {
			fmt.Printf("Table %4d: validated\n", table.ID)
		}
	}
	fmt.Println("\nTables failing validation:")
	for id, e := range failed {
		fmt.Printf("Table %4d: %s\n", id, e.Error())
	}
	return nil
}

func prettyPrint(data interface{}) {
	var p []byte
	//    var err := error
	p, err := json.MarshalIndent(data, "", "\t")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%s \n", p)
}
