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

	// 4) Encode key and unmarshal it as Key (not Message!)
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

	// 2) new row with values [1, 'test-user'].
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

	keyMsg := new(changefeedpb.Message)

	require.NoError(t, protoutil.Unmarshal(keyBytes, keyMsg))
	log.Infof(context.Background(), "Decoded key message:\n%s", proto.MarshalTextString(keyMsg))

	wrapped := keyMsg.GetWrapped()
	require.NotNil(t, wrapped, "expected vallue to be encoded as WrappedEnvelope")

	// 5) assert values are as expected
	require.Equal(t, int64(1), wrapped.After.Values["id"].GetInt64Value())
	require.Equal(t, "test-user", wrapped.After.Values["name"].GetStringValue())
}

func TestProtoEncoder_WrappedEnvelope(t *testing.T) {
	// 1) Build a real table descriptor.
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
	pbOpts := protobufEncoderOptions{
		EncodingOptions: changefeedbase.EncodingOptions{
			Envelope:     changefeedbase.OptEnvelopeWrapped,
			Diff:         false,
			KeyInValue:   false,
			TopicInValue: false,
		},
	}
	enc, err := newProtobufEncoder(context.Background(), pbOpts, targets)
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
	require.NotNil(t, wrap, "EncodeValue must emit a WrappedEnvelope when Envelope=Wrapped")

	// Inside WrappedEnvelope:
	//  - After should contain both columns a and b.
	after := wrap.After.Values
	require.Equal(t, "1", after["a"].GetStringValue())
	require.Equal(t, "hello", after["b"].GetStringValue())

	//  - Before should be unset (nil) when Diff=false.
	require.Nil(t, wrap.Before)

	//  - Updated and MvccTimestamp fields should be empty unless configured.
	require.Empty(t, wrap.Updated)
	require.Empty(t, wrap.MvccTimestamp)
	require.Empty(t, wrap.Key)
	require.Empty(t, wrap.Topic)
}
