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
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
