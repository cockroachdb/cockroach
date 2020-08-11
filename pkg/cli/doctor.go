// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bufio"
	"context"
	hx "encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/spf13/cobra"
)

var debugDoctorCmd = &cobra.Command{

	Use:   "doctor",
	Short: "check the consistecy of system tables from an unzipped debug.zip",
	Long: `

Validate the consistency of the table descriptors from system.descriptor.txt by
running Validate over all table descriptors.	
`,
	Args: cobra.ExactArgs(0),
	RunE: MaybeDecorateGRPCError(runDebugDoctor),
}

func runDebugDoctor(cmd *cobra.Command, args []string) (retErr error) {
	// To make parsing user functions happy.
	_ = builtins.AllBuiltinNames

	protoGetter := sqlbase.MapProtoGetter{
		Protos: make(map[interface{}]protoutil.Message),
	}
	sc := bufio.NewScanner(os.Stdin)
	var tables []*descpb.Descriptor

	for sc.Scan() {
		fields := strings.Fields(sc.Text())
		if len(fields) != 2 {
			panic(fmt.Errorf("got %q fields, expected 2", fields))
		}
		i, err := strconv.Atoi(fields[0])
		if err != nil {
			panic(fmt.Errorf("failed to parse descriptor id %s: %v", fields[0], err))
		}
		id := descpb.ID(i)

		encoded, err := hx.DecodeString(fields[1])
		if err != nil {
			panic(fmt.Errorf("failed to decode hex descriptor %d: %v", id, err))
		}
		var d descpb.Descriptor
		if err := protoutil.Unmarshal(encoded, &d); err != nil {
			panic(fmt.Errorf("failed to unmarshal descriptor %d: %v", id, err))
		}
		protoGetter.Protos[string(sqlbase.MakeDescMetadataKey(keys.SystemSQLCodec, id))] = &d

		t := d.GetTable()
		if t != nil {
			tables = append(tables, &d)
		}
	}
	for _, desc := range tables {
		sqlbase.MaybeSetDescriptorModificationTimeFromMVCCTimestamp(context.Background(), desc, hlc.Timestamp{})
		table := sqlbase.NewImmutableTableDescriptor(*sqlbase.TableFromDescriptor(desc, hlc.Timestamp{}))
		fmt.Printf("Table %d:", table.ID)
		if err := table.Validate(context.Background(), &protoGetter, keys.SystemSQLCodec); err != nil {
			fmt.Printf("%+v\n", err)
			prettyPrint(table)
		} else {
			fmt.Println(" validated")
		}
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
