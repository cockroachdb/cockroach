// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package protoreflect_test

import (
	"encoding/hex"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	gprotoreflecttest "github.com/cockroachdb/cockroach/pkg/sql/protoreflect/gprototest"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect/test"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	jsonb "github.com/cockroachdb/cockroach/pkg/util/json"
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
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
				GeoConfig: geopb.Config{
					S2Geography: &geopb.S2GeographyConfig{S2Config: &geopb.S2Config{
						MinLevel: 123,
						MaxLevel: 321,
					}},
					S2Geometry: &geopb.S2GeometryConfig{
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
				TagGroups: []tracingpb.TagGroup{
					{
						Name: "",
						Tags: []tracingpb.Tag{
							{
								Key:   "one",
								Value: "1",
							},
							{
								Key:   "two",
								Value: "2",
							},
							{
								Key:   "three",
								Value: "3",
							},
						},
					},
				},
				StructuredRecords: []tracingpb.StructuredRecord{{
					Time:    timeutil.NowNoMono(),
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
				// in a 25.4 alpha.
				encoded, err := hex.DecodeString(`0ae7120a046a6f6273180f200128013a0042390a02696410011a0e0801104018002a0030035014600020002a0e756e697175655f726f77696428293000680070007800800100880100980100422d0a0673746174757310021a0e0807100018002a003000501960002000300068007000780080010088010098010042420a076372656174656410031a0f0805100018002a00300050da08600020002a116e6f7728293a3a3a54494d455354414d50300068007000780080010088010098010042360a0f64726f707065645f7061796c6f616410041a0e0808100018002a003000501160002001300168007000780080010088010098010042370a1064726f707065645f70726f677265737310051a0e0808100018002a003000501160002001300168007000780080010088010098010042360a0f637265617465645f62795f7479706510061a0e0807100018002a003000501960002001300068007000780080010088010098010042340a0d637265617465645f62795f696410071a0e0801104018002a003003501460002001300068007000780080010088010098010042370a10636c61696d5f73657373696f6e5f696410081a0e0808100018002a003000501160002001300068007000780080010088010098010042380a11636c61696d5f696e7374616e63655f696410091a0e0801104018002a0030035014600020013000680070007800800100880100980100422f0a086e756d5f72756e73100a1a0e0801104018002a003003501460002001300068007000780080010088010098010042300a086c6173745f72756e100b1a0f0805100018002a00300050da08600020013000680070007800800100880100980100422f0a086a6f625f74797065100c1a0e0807100018002a0030005019600020013000680070007800800100880100980100422c0a056f776e6572100d1a0e0807100018002a003000501960002001300068007000780080010088010098010042320a0b6465736372697074696f6e100e1a0e0807100018002a003000501960002001300068007000780080010088010098010042300a096572726f725f6d7367100f1a0e0807100018002a003000501960002001300068007000780080010088010098010042300a0866696e697368656410101a0f0809100018002a00300050a009600020013000680070007800800100880100980100481152c4020a077072696d61727910011801220269642a067374617475732a07637265617465642a0f64726f707065645f7061796c6f61642a1064726f707065645f70726f67726573732a0f637265617465645f62795f747970652a0d637265617465645f62795f69642a10636c61696d5f73657373696f6e5f69642a11636c61696d5f696e7374616e63655f69642a086e756d5f72756e732a086c6173745f72756e2a086a6f625f747970652a056f776e65722a0b6465736372697074696f6e2a096572726f725f6d73672a0866696e6973686564300140004a10080010001a00200028003000380040005a0070027003700470057006700770087009700a700b700c700d700e700f70107a0408002000800100880100900104980101a20106080012001800a80100b20100ba0100c00100c80100d00101e00100e9010000000000000000f201005a89010a176a6f62735f7374617475735f637265617465645f696478100218002206737461747573220763726561746564300230033801400040004a10080010001a00200028003000380040005a007a0408002000800100880100900103980100a20106080012001800a80100b20100ba0100c00100c80100d00100e00100e9010000000000000000f201005ab1010a266a6f62735f637265617465645f62795f747970655f637265617465645f62795f69645f69647810031800220f637265617465645f62795f74797065220d637265617465645f62795f69642a06737461747573300630073801400040004a10080010001a00200028003000380040005a0070027a0408002000800100880100900103980100a20106080012001800a80100b20100ba0100c00100c80100d00100e00100e9010000000000000000f201005ac9020a126a6f62735f72756e5f73746174735f696478100418002210636c61696d5f73657373696f6e5f696422067374617475732207637265617465642a086c6173745f72756e2a086e756d5f72756e732a11636c61696d5f696e7374616e63655f696430083002300338014000400040004a10080010001a00200028003000380040005a00700b700a70097a0408002000800100880100900103980100a20106080012001800a80100b20100ba01810173746174757320494e20282772756e6e696e67273a3a3a535452494e472c2027726576657274696e67273a3a3a535452494e472c202770656e64696e67273a3a3a535452494e472c202770617573652d726571756573746564273a3a3a535452494e472c202763616e63656c2d726571756573746564273a3a3a535452494e4729c00100c80100d00100e00100e9010000000000000000f201005a780a116a6f62735f6a6f625f747970655f6964781005180022086a6f625f74797065300c380140004a10080010001a00200028003000380040005a007a0408002000800100880100900103980100a20106080012001800a80100b20100ba0100c00100c80100d00100e00100e9010000000000000000f2010060066a250a0d0a0561646d696e10e00318e0030a0c0a04726f6f7410e00318e00312046e6f64651803800101880103980100b201b4010a1f66616d5f305f69645f7374617475735f637265617465645f7061796c6f616410001a0269641a067374617475731a07637265617465641a0f64726f707065645f7061796c6f61641a0f637265617465645f62795f747970651a0d637265617465645f62795f69641a086a6f625f747970651a056f776e65721a0b6465736372697074696f6e1a096572726f725f6d73671a0866696e6973686564200120022003200420062007200c200d200e200f20102800b201220a0870726f677265737310011a1064726f707065645f70726f677265737320052805b2014c0a05636c61696d10021a10636c61696d5f73657373696f6e5f69641a11636c61696d5f696e7374616e63655f69641a086e756d5f72756e731a086c6173745f72756e20082009200a200b2800b80103c20100e80100f2010408001200f801008002009202009a0200b20200b80200c0021dc80200e00200800300880302a80300b00300d00300d80300e00300f80300880400980400a00400a80400b00400`)
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
				jsonb, err := protoreflect.MessageToJSON(decoded, protoreflect.FmtFlags{EmitDefaults: false})
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
				jsonb, err := protoreflect.MessageToJSON(tc.message, protoreflect.FmtFlags{EmitDefaults: false})
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

	t.Run("redacted-pb-to-json-does-not-round-trip", func(t *testing.T) {
		for _, tc := range testCases {
			t.Run(tc.pbname, func(t *testing.T) {
				jsonb, err := protoreflect.MessageToJSON(tc.message, protoreflect.FmtFlags{EmitRedacted: true})
				require.NoError(t, err)

				fromJSON, err := protoreflect.NewMessage(tc.pbname)
				require.NoError(t, err)

				_, err = protoreflect.JSONBMarshalToMessage(jsonb, fromJSON)
				require.Error(t, err)
			})
		}
	})
}

func fetchPath(t *testing.T, j jsonb.JSON, path ...string) string {
	t.Helper()
	var err error
	for _, p := range path {
		require.NotNil(t, j)
		j, err = j.FetchValKey(p)
		require.NoError(t, err)
	}

	text, err := j.AsText()
	require.NoError(t, err)
	require.NotNil(t, text)
	return *text
}

func TestRedactedMessages(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const redactionMarker = "__redacted__"
	const outerValue = "not redacted"

	m := protoreflecttest.Outer{
		Value: outerValue,
		Inner: &protoreflecttest.Inner{Value: protoreflecttest.SecretMessage},
	}

	unredacted, err := protoreflect.MessageToJSON(&m, protoreflect.FmtFlags{EmitRedacted: false})
	require.NoError(t, err)

	markerSet, err := unredacted.Exists(redactionMarker)
	require.NoError(t, err)
	require.False(t, markerSet)

	// Un-redacted message are round trippable.
	var fromJSON protoreflecttest.Outer
	_, err = protoreflect.JSONBMarshalToMessage(unredacted, &fromJSON)
	require.NoError(t, err)
	require.Equal(t, m, fromJSON)

	// Now, try w/ redaction
	redacted, err := protoreflect.MessageToJSON(&m, protoreflect.FmtFlags{EmitRedacted: true})
	require.NoError(t, err)

	markerSet, err = redacted.Exists(redactionMarker)
	require.NoError(t, err)
	require.True(t, markerSet)

	// Redacted message no longer round trip-able.
	_, err = protoreflect.JSONBMarshalToMessage(redacted, &fromJSON)
	require.Error(t, err)
	require.Equal(t, outerValue, fetchPath(t, redacted, "value"))
	require.Equal(t, protoreflecttest.RedactedMessage, fetchPath(t, redacted, "inner", "value"))
}

// Ensure we don't blow up when asking to convert invalid
// data.
func TestInvalidConversions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	t.Run("no such message", func(t *testing.T) {
		_, err := protoreflect.DecodeMessage("no.such.message", nil)
		require.Error(t, err)
	})

	t.Run("must be message type", func(t *testing.T) {
		// Valid proto enum, but we require types.
		_, err := protoreflect.DecodeMessage("cockroach.sql.sqlbase.SystemColumnKind", nil)
		require.Error(t, err)
	})
}

func TestNewMessageFromFileDescriptor(t *testing.T) {
	msg := "Hello, World"
	in := gprotoreflecttest.Inner{
		Value: msg,
	}
	fd := gprotoreflecttest.File_sql_protoreflect_gprototest_gprototest_proto
	bin, err := protoutil.TODOMarshal(&in)
	require.Nil(t, err)

	t.Run("successfully gets message from FileDescriptor", func(t *testing.T) {
		out, err := protoreflect.NewJSONMessageFromFileDescriptor("Inner", fd, bin, nil)
		require.Nil(t, err)
		require.Equal(t, msg, fetchPath(t, out, "value"))
	})
	t.Run("fails if name is  incorrect", func(t *testing.T) {
		out, err := protoreflect.NewJSONMessageFromFileDescriptor("foo", fd, bin, nil)
		require.Nil(t, out)
		require.Error(t, err)
	})
}
