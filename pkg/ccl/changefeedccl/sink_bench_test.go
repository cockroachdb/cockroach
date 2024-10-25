// Copyright 2024 The Cockroach Authors.

// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func setUpWebHookSinks(b *testing.B) (*cdctest.MockWebhookSink, string) {
	cert, certEncoded, err := cdctest.NewCACertBase64Encoded()
	require.NoError(b, err)
	sinkDest, err := cdctest.StartMockWebhookSink(cert)
	require.NoError(b, err)

	sinkDestHost, err := url.Parse(sinkDest.URL())
	require.NoError(b, err)

	params := sinkDestHost.Query()
	params.Set(changefeedbase.SinkParamCACert, certEncoded)
	sinkDestHost.RawQuery = params.Encode()
	return sinkDest, sinkDestHost.String()
}

func generateRandomizedBytes(rand *rand.Rand) []byte {
	const tableID = 42
	dataTypes := []*types.T{types.String, types.Int, types.Decimal, types.Bytes, types.Bool, types.Date, types.Timestamp, types.Float}
	randType := dataTypes[rand.Intn(len(dataTypes))]

	key, err := keyside.Encode(
		keys.SystemSQLCodec.TablePrefix(tableID),
		randgen.RandDatumSimple(rand, randType),
		encoding.Ascending,
	)
	if err != nil {
		panic(err)
	}
	return key
}

func runWebhookSink(b *testing.B, sinkSrc Sink, rng *rand.Rand, enc Encoder) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bytes := generateRandomizedBytes(rng)
		ts := hlc.Timestamp{WallTime: rand.Int63() + 1}
		require.NoError(b, sinkSrc.EmitRow(context.Background(), noTopic{}, bytes, bytes, zeroTS, zeroTS, zeroAlloc))
		require.NoError(b, sinkSrc.Flush(context.Background()))
		require.NoError(b, sinkSrc.EmitResolvedTimestamp(context.Background(), enc, ts))
	}
}

func BenchmarkWebhookSink(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	b.ReportAllocs()

	webhookSinkTestfn := func(parallelism int, opts map[string]string) {
		sinkDest, sinkDestHost := setUpWebHookSinks(b)
		defer sinkDest.Close()
		details := jobspb.ChangefeedDetails{
			SinkURI: fmt.Sprintf("webhook-%s", sinkDestHost),
			Opts:    opts,
		}
		sinkSrc, err := setupWebhookSinkWithDetails(context.Background(), details, parallelism, timeutil.DefaultTimeSource{})
		require.NoError(b, err)
		defer require.NoError(b, sinkSrc.Close())

		rng, _ := randutil.NewTestRand()
		ctx := context.Background()
		require.NoError(b, err)
		encodingOpts, err := changefeedbase.MakeStatementOptions(opts).GetEncodingOptions()
		require.NoError(b, err)
		enc, err := makeJSONEncoder(ctx, jsonEncoderOptions{EncodingOptions: encodingOpts})
		require.NoError(b, err)
		runWebhookSink(b, sinkSrc, rng, enc)
	}

	opts := getGenericWebhookSinkOptions()
	optsZeroValueConfig := getGenericWebhookSinkOptions(
		struct {
			key   string
			value string
		}{changefeedbase.OptWebhookSinkConfig,
			`{"Retry":{"Backoff": "5ms"},"Flush":{"Bytes": 0, "Frequency": "0s", "Messages": 0}}`})
	for i := 1; i <= 4; i++ {
		opts.AsMap()
		for optName, opt := range map[string]changefeedbase.StatementOptions{"generic": opts, "opts_zero_value": optsZeroValueConfig} {
			b.Run(fmt.Sprintf("parallelism=%d/opt=%s", i, optName), func(b *testing.B) {
				webhookSinkTestfn(i, opt.AsMap())
			})
		}
	}
}
