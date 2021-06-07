// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package protoreflect_test

import (
	"encoding/hex"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/gogo/protobuf/jsonpb"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

func makeAny(t *testing.T, msg protoutil.Message) *pbtypes.Any {
	any, err := pbtypes.MarshalAny(msg)
	require.NoError(t, err)
	return any
}

func TestMessageToJSONBRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		pbname  string
		message protoutil.Message
	}{
		{ // Just a simple Message
			pbname: "cockroach.sql.sqlbase.Descriptor",
			message: &descpb.Descriptor{
				Union: &descpb.Descriptor_Table{
					Table: &descpb.TableDescriptor{Name: "the table"},
				},
			},
		},
		{ // Message with an array
			pbname: "cockroach.sql.sqlbase.ColumnDescriptor",
			message: &descpb.ColumnDescriptor{
				Name:            "column",
				ID:              123,
				OwnsSequenceIds: []descpb.ID{3, 2, 1},
				Type:            types.MakeTuple([]*types.T{types.Date, types.IntArray}),
			},
		},
		{ // Message with an array and other embedded descriptors
			pbname: "cockroach.sql.sqlbase.IndexDescriptor",
			message: &descpb.IndexDescriptor{
				Name:                "myidx",
				ID:                  500,
				Unique:              true,
				KeyColumnNames:      []string{"foo", "bar", "buz"},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				GeoConfig: geoindex.Config{
					S2Geography: &geoindex.S2GeographyConfig{S2Config: &geoindex.S2Config{
						MinLevel: 123,
						MaxLevel: 321,
					}},
					S2Geometry: &geoindex.S2GeometryConfig{
						MinX: 567,
						MaxX: 765,
					},
				},
			},
		},
		{ // Message with embedded google.protobuf.Any message;
			// nested inside other message; with maps
			pbname: "cockroach.util.tracing.tracingpb.RecordedSpan",
			message: &tracingpb.RecordedSpan{
				TraceID:                      123,
				Tags:                         map[string]string{"one": "1", "two": "2", "three": "3"},
				DeprecatedInternalStructured: []*pbtypes.Any{makeAny(t, &descpb.ColumnDescriptor{Name: "bogus stats"})},
				StructuredRecords: []tracingpb.StructuredRecord{{
					Time:    timeutil.Now(),
					Payload: makeAny(t, &descpb.ColumnDescriptor{Name: "bogus stats"})}},
			},
		},
		{ // Message deeply nested inside other message
			pbname:  "cockroach.sql.sqlbase.TableDescriptor.SequenceOpts.SequenceOwner",
			message: &descpb.TableDescriptor_SequenceOpts_SequenceOwner{OwnerColumnID: 123},
		},
		{
			pbname: "cockroach.sql.sqlbase.Descriptor",
			message: func() protoutil.Message {
				// This is a real descriptor pulled from a demo cluster for system.jobs
				// in a 21.1 alpha.
				encoded, err := hex.DecodeString(`0aa8080a046a6f6273180f200128013a0042310a02696410011a0c08011040180030005014600020002a0e756e697175655f726f7769642829300068007000780080010042250a0673746174757310021a0c08071000180030005019600020003000680070007800800100423a0a076372656174656410031a0d080510001800300050da08600020002a116e6f7728293a3a3a54494d455354414d50300068007000780080010042260a077061796c6f616410041a0c0808100018003000501160002000300068007000780080010042270a0870726f677265737310051a0c08081000180030005011600020013000680070007800800100422e0a0f637265617465645f62795f7479706510061a0c08071000180030005019600020013000680070007800800100422c0a0d637265617465645f62795f696410071a0c08011040180030005014600020013000680070007800800100422f0a10636c61696d5f73657373696f6e5f696410081a0c0808100018003000501160002001300068007000780080010042300a11636c61696d5f696e7374616e63655f696410091a0c08011040180030005014600020013000680070007800800100480a524b0a077072696d6172791001180122026964300140004a10080010001a00200028003000380040005a007a020800800100880100900102980100a20106080012001800a80100b20100ba01005a6e0a176a6f62735f7374617475735f637265617465645f696478100218002206737461747573220763726561746564300230033801400040004a10080010001a00200028003000380040005a007a020800800100880100900102980100a20106080012001800a80100b20100ba01005a96010a266a6f62735f637265617465645f62795f747970655f637265617465645f62795f69645f69647810031800220f637265617465645f62795f74797065220d637265617465645f62795f69642a06737461747573300630073801400040004a10080010001a00200028003000380040005a0070027a020800800100880100900102980100a20106080012001800a80100b20100ba010060046a1f0a0a0a0561646d696e10f0030a090a04726f6f7410f00312046e6f64651801800101880103980100b2016f0a1f66616d5f305f69645f7374617475735f637265617465645f7061796c6f616410001a0269641a067374617475731a07637265617465641a077061796c6f61641a0f637265617465645f62795f747970651a0d637265617465645f62795f69642001200220032004200620072800b2011a0a0870726f677265737310011a0870726f677265737320052805b201340a05636c61696d10021a10636c61696d5f73657373696f6e5f69641a11636c61696d5f696e7374616e63655f6964200820092800b80103c20100e80100f2010408001200f801008002009202009a0200b20200b80200c0021dc80200`)
				require.NoError(t, err)
				var desc descpb.Descriptor
				require.NoError(t, protoutil.Unmarshal(encoded, &desc))
				return &desc
			}(),
		},
	}

	t.Run("pb-to-json-round-trip", func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.pbname, func(t *testing.T) {
				protoData, err := protoutil.Marshal(tc.message)
				require.NoError(t, err)

				// Decode proto bytes to message and compare.
				decoded, err := protoreflect.DecodeMessage(tc.pbname, protoData)
				require.NoError(t, err)
				require.Equal(t, tc.message, decoded)

				// Encode message as json
				jsonb, err := protoreflect.MessageToJSON(decoded, false /* emitDefaults */)
				require.NoError(t, err)

				// Recreate message from json
				fromJSON := reflect.New(reflect.TypeOf(tc.message).Elem()).Interface().(protoutil.Message)

				json := &jsonpb.Unmarshaler{}
				require.NoError(t, json.Unmarshal(strings.NewReader(jsonb.String()), fromJSON))

				require.Equal(t, tc.message, fromJSON)
			})
		}
	})

	t.Run("identity-round-trip", func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.pbname, func(t *testing.T) {
				jsonb, err := protoreflect.MessageToJSON(tc.message, false /* emitDefaults */)
				require.NoError(t, err)

				fromJSON, err := protoreflect.NewMessage(tc.pbname)
				require.NoError(t, err)

				fromJSONBytes, err := protoreflect.JSONBMarshalToMessage(jsonb, fromJSON)
				require.NoError(t, err)

				expectedBytes, err := protoutil.Marshal(tc.message)
				require.NoError(t, err)

				require.Equal(t, expectedBytes, fromJSONBytes)
			})
		}
	})

}

// Ensure we don't blow up when asking to convert invalid
// data.
func TestInvalidConversions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("no such messagge", func(t *testing.T) {
		_, err := protoreflect.DecodeMessage("no.such.message", nil)
		require.Error(t, err)
	})

	t.Run("must be message type", func(t *testing.T) {
		// Valid proto enum, but we require types.
		_, err := protoreflect.DecodeMessage("cockroach.sql.sqlbase.SystemColumnKind", nil)
		require.Error(t, err)
	})
}
