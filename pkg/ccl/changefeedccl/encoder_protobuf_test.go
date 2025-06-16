package changefeedccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProtoEncoder_EncodeKeyOnly(t *testing.T) {
	// 1) Create a table
	tableDesc, err := parseTableDesc(`CREATE TABLE test (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)
	targets := mkTargets(tableDesc)

	// 2) Row with values [1, 'test-user']
	encRow := rowenc.EncDatumRow{
		rowenc.DatumToEncDatum(types.Int, tree.NewDInt(1)),
		rowenc.DatumToEncDatum(types.String, tree.NewDString("test-user")),
	}
	row := cdcevent.TestingMakeEventRow(tableDesc, 0, encRow, false)

	// 3) Set up protobuf encoder
	pbOpts := protobufEncoderOptions{
		EncodingOptions: changefeedbase.EncodingOptions{
			Envelope: changefeedbase.OptEnvelopeWrapped,
		},
	}
	enc, err := newProtobufEncoder(context.Background(), pbOpts, targets)
	require.NoError(t, err)

	// 4) Encode key and unmarshal it as Key
	keyBytes, err := enc.EncodeKey(context.Background(), row)
	require.NoError(t, err)

	keyMsg := new(changefeedpb.Key)
	require.NoError(t, protoutil.Unmarshal(keyBytes, keyMsg))

	// 5) Assert key content
	require.Len(t, keyMsg.Key, 1)
	require.Equal(t, int64(1), keyMsg.Key[0].GetInt64Value())
}

func TestProtoEncoder_EncodeValueOnly(t *testing.T) {
	// 1) Create a table
	tableDesc, err := parseTableDesc(`CREATE TABLE test (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)
	targets := mkTargets(tableDesc)

	// 2) Row with values [1, 'test-user'].
	encRow := rowenc.EncDatumRow{
		rowenc.DatumToEncDatum(types.Int, tree.NewDInt(1)),
		rowenc.DatumToEncDatum(types.String, tree.NewDString("test-user")),
	}

	row := cdcevent.TestingMakeEventRow(tableDesc, 0, encRow, false)
	prevRow := cdcevent.TestingMakeEventRow(tableDesc, 0, encRow, false)

	// 4) Encode the value
	evCtx := eventContext{
		updated: hlc.Timestamp{WallTime: 123},
		mvcc:    hlc.Timestamp{WallTime: 456},
		topic:   "test-topic",
	}

	// 3) Create a protobuf encoder with default options.
	pbOpts := protobufEncoderOptions{
		EncodingOptions: changefeedbase.EncodingOptions{
			Envelope: changefeedbase.OptEnvelopeWrapped,
		},
	}
	enc, err := newProtobufEncoder(context.Background(), pbOpts, targets)
	require.NoError(t, err)

	// 4) Encode the key and assert it's a BareEnvelope.
	keyBytes, err := enc.EncodeValue(context.Background(), evCtx, row, prevRow)
	require.NoError(t, err)

	Msg := new(changefeedpb.Message)

	require.NoError(t, protoutil.Unmarshal(keyBytes, Msg))
	log.Infof(context.Background(), "Decoded message:\n%s", proto.MarshalTextString(Msg))

	wrapped := Msg.GetWrapped()
	require.NotNil(t, wrapped, "expected vallue to be encoded as WrappedEnvelope")

	// 5) assert values are as expected
	require.Equal(t, int64(1), wrapped.After.Values["id"].GetInt64Value())
	require.Equal(t, "test-user", wrapped.After.Values["name"].GetStringValue())
}

func TestProtoEncoder_WrappedEnvelope(t *testing.T) {
	// 1) Create table
	tableDesc, err := parseTableDesc(`CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	require.NoError(t, err)
	targets := mkTargets(tableDesc)

	// 2) Create an EncDatumRow for values [1, "hello"].
	encRow := rowenc.EncDatumRow{
		rowenc.DatumToEncDatum(types.Int, tree.NewDInt(1)),
		rowenc.DatumToEncDatum(types.String, tree.NewDString("hello")),
	}
	row := cdcevent.TestingMakeEventRow(tableDesc, 0, encRow, false)

	// 3) Construct the encoder with default Envelope=Wrapped.
	opts := changefeedbase.EncodingOptions{
		Envelope:     changefeedbase.OptEnvelopeWrapped,
		Format:       changefeedbase.OptFormatProtobuf,
		Diff:         false,
		KeyInValue:   false,
		TopicInValue: false,
	}

	enc, err := getEncoder(context.Background(), opts, targets, false, nil, nil, nil)
	require.NoError(t, err)

	// 4) EncodeKey should emits a Key
	keyBytes, err := enc.EncodeKey(context.Background(), row)
	require.NoError(t, err)

	keyMsg := new(changefeedpb.Key)
	require.NoError(t, protoutil.Unmarshal(keyBytes, keyMsg))

	// 5) Assert key content
	require.Len(t, keyMsg.Key, 1)
	require.Equal(t, int64(1), keyMsg.Key[0].GetInt64Value())

	// 5) Now test EncodeValue → should emit a WrappedEnvelope.
	evCtx := eventContext{
		updated: hlc.Timestamp{WallTime: 42},
		mvcc:    hlc.Timestamp{WallTime: 84},
		topic:   "test-topic",
	}
	valBytes, err := enc.EncodeValue(context.Background(), evCtx, row, cdcevent.Row{})
	require.NoError(t, err)

	valMsg := new(changefeedpb.Message)
	require.NoError(t, protoutil.Unmarshal(valBytes, valMsg))

	wrap := valMsg.GetWrapped()

	log.Infof(context.Background(), "Decoded wrapped message:\n%s", proto.MarshalTextString(wrap))
	// Inside WrappedEnvelope:
	//  - After should contain both columns a and b.
	after := wrap.After.Values
	require.Equal(t, int64(1), after["a"].GetInt64Value())
	require.Equal(t, "hello", after["b"].GetStringValue())

	//  - Before should be unset (nil) when Diff=false.
	require.Nil(t, wrap.Before)
}

func TestProtoEncoder_BareEnvelope_WithMetadata(t *testing.T) {
	tableDesc, err := parseTableDesc(`CREATE TABLE test (id INT PRIMARY KEY, name STRING)`)
	require.NoError(t, err)

	encRow := rowenc.EncDatumRow{
		rowenc.DatumToEncDatum(types.Int, tree.NewDInt(1)),
		rowenc.DatumToEncDatum(types.String, tree.NewDString("Alice")),
	}
	row := cdcevent.TestingMakeEventRow(tableDesc, 0, encRow, false)

	evCtx := eventContext{
		updated: hlc.Timestamp{WallTime: 123},
		mvcc:    hlc.Timestamp{WallTime: 456},
		topic:   "test-topic",
	}

	encOpts := protobufEncoderOptions{
		EncodingOptions: changefeedbase.EncodingOptions{
			Envelope:          changefeedbase.OptEnvelopeBare,
			UpdatedTimestamps: true,
			MVCCTimestamps:    true,
			KeyInValue:        true,
			TopicInValue:      true,
		},
	}

	encoder, err := newProtobufEncoder(context.Background(), encOpts, mkTargets(tableDesc))
	require.NoError(t, err)

	valueBytes, err := encoder.EncodeValue(context.Background(), evCtx, row, cdcevent.Row{})
	require.NoError(t, err)

	msg := new(changefeedpb.Message)
	require.NoError(t, protoutil.Unmarshal(valueBytes, msg))

	bare := msg.GetBare()
	require.NotNil(t, bare)
	require.NotNil(t, bare.XCrdb__)

	require.Equal(t, evCtx.updated.AsOfSystemTime(), bare.XCrdb__.Updated)
	require.Equal(t, evCtx.mvcc.AsOfSystemTime(), bare.XCrdb__.MvccTimestamp)
	require.NotNil(t, bare.XCrdb__.Key)
	assert.Equal(t, "test-topic", bare.XCrdb__.Topic)
}

func TestProtoEncoder_ResolvedEnvelope(t *testing.T) {
	tableDesc, err := parseTableDesc(`CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	require.NoError(t, err)
	targets := mkTargets(tableDesc)

	ts := hlc.Timestamp{WallTime: 123, Logical: 456}

	tests := []struct {
		name          string
		envelopeType  changefeedbase.EnvelopeType
		expectWrapped bool
	}{
		{
			name:         "wrapped envelope",
			envelopeType: changefeedbase.OptEnvelopeWrapped,
		},
		{
			name:         "bare envelope",
			envelopeType: changefeedbase.OptEnvelopeBare,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := changefeedbase.EncodingOptions{
				Envelope: tc.envelopeType,
				Format:   changefeedbase.OptFormatProtobuf,
			}

			enc, err := getEncoder(context.Background(), opts, targets, false, nil, nil, nil)
			require.NoError(t, err)

			b, err := enc.EncodeResolvedTimestamp(context.Background(), "test-topic", ts)
			require.NoError(t, err)

			var msg changefeedpb.Message
			require.NoError(t, protoutil.Unmarshal(b, &msg))

			switch tc.envelopeType {
			case changefeedbase.OptEnvelopeWrapped:
				res := msg.GetResolved()
				require.NotNil(t, res, "wrapped envelope should populate Resolved field")
				require.Equal(t, ts.AsOfSystemTime(), res.Resolved)
			case changefeedbase.OptEnvelopeBare:
				res := msg.GetBareResolved()
				require.NotNil(t, res, "bare envelope should populate BareResolved field")
				require.Equal(t, ts.AsOfSystemTime(), res.XCrdb__.Resolved)
			default:
				t.Fatalf("unexpected envelope type: %v", tc.envelopeType)
			}
		})
	}
}
