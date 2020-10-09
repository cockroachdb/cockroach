// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package protoreflect

import (
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
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
			},
		},
		{ // Message with an array and other embedded descriptors
			pbname: "cockroach.sql.sqlbase.IndexDescriptor",
			message: &descpb.IndexDescriptor{
				Name:             "myidx",
				ID:               500,
				Unique:           true,
				ColumnNames:      []string{"foo", "bar", "buz"},
				ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
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
				TraceID: 123,
				Tags:    map[string]string{"one": "1", "two": "2", "three": "3"},
				Stats:   makeAny(t, &descpb.ColumnDescriptor{Name: "bogus stats"}),
			},
		},
		{ // Message deeply nested inside other message
			pbname:  "cockroach.sql.sqlbase.TableDescriptor.SequenceOpts.SequenceOwner",
			message: &descpb.TableDescriptor_SequenceOpts_SequenceOwner{OwnerColumnID: 123},
		},
	}

	t.Run("pb-to-json-round-trip", func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.pbname, func(t *testing.T) {
				protoData, err := protoutil.Marshal(tc.message)
				require.NoError(t, err)

				// Decode proto bytes to message and compare.
				decoded, err := DecodeMessage(tc.pbname, protoData)
				require.NoError(t, err)
				require.Equal(t, tc.message, decoded)

				// Encode message as json
				jsonb, err := MessageToJSON(decoded, false /* emitDefaults */)
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
				jsonb, err := MessageToJSON(tc.message, false /* emitDefaults */)
				require.NoError(t, err)

				fromJSON, err := NewMessage(tc.pbname)
				require.NoError(t, err)

				fromJSONBytes, err := JSONBMarshalToMessage(jsonb, fromJSON)
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
		_, err := DecodeMessage("no.such.message", nil)
		require.Error(t, err)
	})

	t.Run("must be message type", func(t *testing.T) {
		// Valid proto enum, but we require types.
		_, err := DecodeMessage("cockroach.sql.sqlbase.SystemColumnKind", nil)
		require.Error(t, err)
	})
}
