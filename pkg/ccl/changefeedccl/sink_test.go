// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

var zeroTS hlc.Timestamp

type asyncProducerMock struct {
	inputCh     chan *sarama.ProducerMessage
	successesCh chan *sarama.ProducerMessage
	errorsCh    chan *sarama.ProducerError
	mu          struct {
		syncutil.Mutex
		outstanding []*sarama.ProducerMessage
	}
}

var _ sarama.AsyncProducer = (*asyncProducerMock)(nil)

const unbuffered = 0

func newAsyncProducerMock(bufSize int) *asyncProducerMock {
	return &asyncProducerMock{
		inputCh:     make(chan *sarama.ProducerMessage, bufSize),
		successesCh: make(chan *sarama.ProducerMessage, bufSize),
		errorsCh:    make(chan *sarama.ProducerError, bufSize),
	}
}

func (p *asyncProducerMock) Input() chan<- *sarama.ProducerMessage     { return p.inputCh }
func (p *asyncProducerMock) Successes() <-chan *sarama.ProducerMessage { return p.successesCh }
func (p *asyncProducerMock) Errors() <-chan *sarama.ProducerError      { return p.errorsCh }
func (p *asyncProducerMock) AsyncClose()                               { panic(`unimplemented`) }
func (p *asyncProducerMock) Close() error {
	close(p.inputCh)
	close(p.successesCh)
	close(p.errorsCh)
	return nil
}
func (p *asyncProducerMock) IsTransactional() bool                   { panic(`unimplemented`) }
func (p *asyncProducerMock) BeginTxn() error                         { panic(`unimplemented`) }
func (p *asyncProducerMock) CommitTxn() error                        { panic(`unimplemented`) }
func (p *asyncProducerMock) AbortTxn() error                         { panic(`unimplemented`) }
func (p *asyncProducerMock) TxnStatus() sarama.ProducerTxnStatusFlag { panic(`unimplemented`) }
func (p *asyncProducerMock) AddOffsetsToTxn(
	_ map[string][]*sarama.PartitionOffsetMetadata, _ string,
) error {
	panic(`unimplemented`)
}
func (p *asyncProducerMock) AddMessageToTxn(_ *sarama.ConsumerMessage, _ string, _ *string) error {
	panic(`unimplemented`)
}

type syncProducerMock struct {
	overrideSend func(*sarama.ProducerMessage) error
}

var _ sarama.SyncProducer = (*syncProducerMock)(nil)

func (p *syncProducerMock) SendMessage(
	msg *sarama.ProducerMessage,
) (partition int32, offset int64, err error) {
	if p.overrideSend != nil {
		if err := p.overrideSend(msg); err != nil {
			return 0, 0, sarama.ProducerError{Msg: msg, Err: err}
		}
	}
	return 0, 0, nil
}
func (p *syncProducerMock) SendMessages(msgs []*sarama.ProducerMessage) error {
	var errs sarama.ProducerErrors = nil
	for _, msg := range msgs {
		_, _, err := p.SendMessage(msg)
		if err != nil {
			// nolint:errcmp
			if producerErr, ok := err.(sarama.ProducerError); ok {
				errs = append(errs, &producerErr)
			} else {
				return err
			}
		}
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}
func (p *syncProducerMock) IsTransactional() bool                   { panic(`unimplemented`) }
func (p *syncProducerMock) BeginTxn() error                         { panic(`unimplemented`) }
func (p *syncProducerMock) CommitTxn() error                        { panic(`unimplemented`) }
func (p *syncProducerMock) AbortTxn() error                         { panic(`unimplemented`) }
func (p *syncProducerMock) TxnStatus() sarama.ProducerTxnStatusFlag { panic(`unimplemented`) }
func (p *syncProducerMock) AddOffsetsToTxn(
	_ map[string][]*sarama.PartitionOffsetMetadata, _ string,
) error {
	panic(`unimplemented`)
}
func (p *syncProducerMock) AddMessageToTxn(_ *sarama.ConsumerMessage, _ string, _ *string) error {
	panic(`unimplemented`)
}
func (p *syncProducerMock) Close() error { panic(`unimplemented`) }

// consumeAndSucceed consumes input messages and sends them to successes channel.
// Returns function that must be called to stop this consumer
// to clean up. The cleanup function must be called before closing asyncProducerMock.
func (p *asyncProducerMock) consumeAndSucceed() (cleanup func()) {
	var wg sync.WaitGroup
	wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case m := <-p.inputCh:
				p.successesCh <- m
			}
		}
	}()
	return func() {
		close(done)
		wg.Wait()
	}
}

// consume consumes input messages but does not acknowledge neither successes, nor errors.
// In essence, this simulates an unreachable kafka sink.
// Use acknowledge methods to acknowledge successes or errors.
// Returns a function that must be called to stop this consumer
// to clean up. The cleanup function must be called before closing asyncProducerMock.
func (p *asyncProducerMock) consume() (cleanup func()) {
	var wg sync.WaitGroup
	wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case m := <-p.inputCh:
				p.mu.Lock()
				p.mu.outstanding = append(p.mu.outstanding, m)
				p.mu.Unlock()
			}
		}
	}()
	return func() {
		close(done)
		wg.Wait()
	}
}

// acknowledge sends acknowledgements on the specified channel
// for each of the outstanding messages.
func (p *asyncProducerMock) acknowledge(n int, ch chan *sarama.ProducerMessage) {
	for n > 0 {
		var outstanding []*sarama.ProducerMessage
		p.mu.Lock()
		outstanding = append(outstanding, p.mu.outstanding...)
		p.mu.outstanding = p.mu.outstanding[:0]
		p.mu.Unlock()

		for _, m := range outstanding {
			ch <- m
		}
		n -= len(outstanding)
	}
}

// outstanding returns the number of un-acknowledged messages.
func (p *asyncProducerMock) outstanding() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.mu.outstanding)
}

func topic(name string) *tableDescriptorTopic {
	tableDesc := tabledesc.NewBuilder(&descpb.TableDescriptor{Name: name}).BuildImmutableTable()
	spec := changefeedbase.Target{
		Type:              jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY,
		TableID:           tableDesc.GetID(),
		StatementTimeName: changefeedbase.StatementTimeName(name),
	}
	return &tableDescriptorTopic{Metadata: makeMetadata(tableDesc), spec: spec}
}

const noTopicPrefix = ""
const defaultTopicName = ""

type testAllocPool struct {
	syncutil.Mutex
	n int64
}

// Release implements kvevent.pool interface.
func (ap *testAllocPool) Release(ctx context.Context, bytes, entries int64) {
	ap.Lock()
	defer ap.Unlock()
	if ap.n == 0 {
		panic("can't release zero resources")
	}
	ap.n -= bytes
}

func (ap *testAllocPool) alloc() kvevent.Alloc {
	ap.Lock()
	defer ap.Unlock()
	ap.n++
	return kvevent.TestingMakeAlloc(1, ap)
}

func (ap *testAllocPool) used() int64 {
	ap.Lock()
	defer ap.Unlock()
	return ap.n
}

var zeroAlloc kvevent.Alloc

func makeTestKafkaSink(
	t testing.TB,
	topicPrefix string,
	topicNameOverride string,
	p sarama.AsyncProducer,
	targetNames ...string,
) (s *kafkaSink, cleanup func()) {
	targets := makeChangefeedTargets(targetNames...)
	topics, err := MakeTopicNamer(targets,
		WithPrefix(topicPrefix), WithSingleName(topicNameOverride), WithSanitizeFn(changefeedbase.SQLNameToKafkaName))
	require.NoError(t, err)

	s = &kafkaSink{
		ctx:      context.Background(),
		topics:   topics,
		kafkaCfg: &sarama.Config{},
		metrics:  (*sliMetrics)(nil),
		knobs: kafkaSinkKnobs{
			OverrideAsyncProducerFromClient: func(client kafkaClient) (sarama.AsyncProducer, error) {
				return p, nil
			},
			OverrideClientInit: func(config *sarama.Config) (kafkaClient, error) {
				client := &fakeKafkaClient{config}
				return client, nil
			},
		},
	}
	err = s.Dial()
	require.NoError(t, err)

	return s, func() {
		require.NoError(t, s.Close())
	}
}

func makeChangefeedTargets(targetNames ...string) changefeedbase.Targets {
	targets := changefeedbase.Targets{}
	for i, name := range targetNames {
		targets.Add(changefeedbase.Target{
			TableID:           descpb.ID(i),
			StatementTimeName: changefeedbase.StatementTimeName(name),
		})
	}
	return targets
}

func TestKafkaSink(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	p := newAsyncProducerMock(1)
	sink, cleanup := makeTestKafkaSink(
		t, noTopicPrefix, defaultTopicName, p, "t")
	defer cleanup()

	// No inflight
	require.NoError(t, sink.Flush(ctx))

	// Timeout
	require.NoError(t,
		sink.EmitRow(ctx, topic(`t`), []byte(`1`), nil, zeroTS, zeroTS, zeroAlloc))

	m1 := <-p.inputCh
	for i := 0; i < 2; i++ {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Millisecond)
		defer cancel()
		require.True(t, errors.Is(context.DeadlineExceeded, sink.Flush(timeoutCtx)))
	}
	go func() { p.successesCh <- m1 }()
	require.NoError(t, sink.Flush(ctx))

	// Check no inflight again now that we've sent something
	require.NoError(t, sink.Flush(ctx))

	// Mixed success and error.
	var pool testAllocPool
	require.NoError(t, sink.EmitRow(ctx,
		topic(`t`), []byte(`2`), nil, zeroTS, zeroTS, pool.alloc()))
	m2 := <-p.inputCh
	require.NoError(t, sink.EmitRow(
		ctx, topic(`t`), []byte(`3`), nil, zeroTS, zeroTS, pool.alloc()))

	m3 := <-p.inputCh
	require.NoError(t, sink.EmitRow(
		ctx, topic(`t`), []byte(`4`), nil, zeroTS, zeroTS, pool.alloc()))

	m4 := <-p.inputCh
	go func() { p.successesCh <- m2 }()
	go func() {
		p.errorsCh <- &sarama.ProducerError{
			Msg: m3,
			Err: errors.New("m3"),
		}
	}()
	go func() { p.successesCh <- m4 }()
	require.Regexp(t, "m3", sink.Flush(ctx))

	// Check simple success again after error
	require.NoError(t, sink.EmitRow(
		ctx, topic(`t`), []byte(`5`), nil, zeroTS, zeroTS, pool.alloc()))

	m5 := <-p.inputCh
	go func() { p.successesCh <- m5 }()
	require.NoError(t, sink.Flush(ctx))
	// At the end, all of the resources has been released
	require.EqualValues(t, 0, pool.used())
}

func TestKafkaSinkEscaping(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	p := newAsyncProducerMock(1)
	sink, cleanup := makeTestKafkaSink(t, noTopicPrefix, defaultTopicName, p, `☃`)
	defer cleanup()

	if err := sink.EmitRow(ctx, topic(`☃`), []byte(`k☃`), []byte(`v☃`), zeroTS, zeroTS, zeroAlloc); err != nil {
		t.Fatal(err)
	}
	m := <-p.inputCh
	require.Equal(t, `_u2603_`, m.Topic)
	require.Equal(t, sarama.ByteEncoder(`k☃`), m.Key)
	require.Equal(t, sarama.ByteEncoder(`v☃`), m.Value)
}

func TestKafkaTopicNameProvided(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	const topicOverride = "general"
	p := newAsyncProducerMock(1)
	sink, cleanup := makeTestKafkaSink(
		t, noTopicPrefix, topicOverride, p, "particular0", "particular1")
	defer cleanup()

	//all messages go to the general topic
	require.NoError(t, sink.EmitRow(ctx, topic("particular0"), []byte(`k☃`), []byte(`v☃`), zeroTS, zeroTS, zeroAlloc))
	m := <-p.inputCh
	require.Equal(t, topicOverride, m.Topic)
}

func TestKafkaTopicNameWithPrefix(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	p := newAsyncProducerMock(1)
	const topicPrefix = "prefix-"
	const topicOverride = "☃"
	sink, clenaup := makeTestKafkaSink(
		t, topicPrefix, topicOverride, p, "particular0", "particular1")
	defer clenaup()

	//the prefix is applied and the name is escaped
	require.NoError(t, sink.EmitRow(ctx, topic("particular0"), []byte(`k☃`), []byte(`v☃`), zeroTS, zeroTS, zeroAlloc))
	m := <-p.inputCh
	require.Equal(t, `prefix-_u2603_`, m.Topic)
}

// goos: darwin
// goarch: amd64
// pkg: github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl
// cpu: Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz
// BenchmarkEmitRow-16    	  573620	      1779 ns/op	     235 B/op	       6 allocs/op
func BenchmarkEmitRow(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	ctx := context.Background()
	p := newAsyncProducerMock(unbuffered)
	const tableName = `defaultdb.public.funky_table☃`
	topic := topic(tableName)
	sink, cleanup := makeTestKafkaSink(b, noTopicPrefix, defaultTopicName, p, tableName)
	stopConsume := p.consumeAndSucceed()
	defer func() {
		stopConsume()
		cleanup()
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		require.NoError(b, sink.EmitRow(ctx, topic, []byte(`k☃`), []byte(`v☃`), hlc.Timestamp{}, zeroTS, zeroAlloc))
	}

	b.ReportAllocs()
}

type testEncoder struct{}

func (testEncoder) EncodeKey(context.Context, cdcevent.Row) ([]byte, error) {
	panic(`unimplemented`)
}
func (testEncoder) EncodeValue(
	context.Context, eventContext, cdcevent.Row, cdcevent.Row,
) ([]byte, error) {
	panic(`unimplemented`)
}
func (testEncoder) EncodeResolvedTimestamp(
	_ context.Context, _ string, ts hlc.Timestamp,
) ([]byte, error) {
	return []byte(ts.String()), nil
}

func TestSQLSink(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	overrideTopic := func(name string) *tableDescriptorTopic {
		id, _ := strconv.ParseUint(name, 36, 64)
		td := tabledesc.NewBuilder(&descpb.TableDescriptor{Name: name, ID: descpb.ID(id)}).BuildImmutableTable()
		spec := changefeedbase.Target{
			Type:              jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY,
			TableID:           td.GetID(),
			StatementTimeName: changefeedbase.StatementTimeName(name),
		}
		return &tableDescriptorTopic{Metadata: makeMetadata(td), spec: spec}
	}

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSharedProcessModeButDoesntYet(
			base.TestTenantProbabilistic, 112863,
		),
		UseDatabase: "d",
	})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	sqlDB.Exec(t, `CREATE DATABASE d`)

	// TODO(herko): When the issue relating to this test is fixed, update this
	// to use PGUrl on the server interface instead.
	// See: https://github.com/cockroachdb/cockroach/issues/112863
	pgURL, cleanup := pgurlutils.PGUrl(t, s.ApplicationLayer().AdvSQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanup()
	pgURL.Path = `d`

	fooTopic := overrideTopic(`foo`)
	barTopic := overrideTopic(`bar`)
	targets := changefeedbase.Targets{}
	targets.Add(fooTopic.GetTargetSpecification())
	targets.Add(barTopic.GetTargetSpecification())

	const testTableName = `sink`
	// TODO(herko): `makeSQLSink` does not expect "options" to be present in the URL, find out if
	// this is a bug, or if we should add it to to `consumeParam` in the `makeSQLSink` function.
	// See: https://github.com/cockroachdb/cockroach/issues/112863.
	sink, err := makeSQLSink(&changefeedbase.SinkURL{URL: &pgURL}, testTableName, targets, nilMetricsRecorderBuilder)
	require.NoError(t, err)
	require.NoError(t, sink.(*sqlSink).Dial())
	defer func() { require.NoError(t, sink.Close()) }()

	// Empty
	require.NoError(t, sink.Flush(ctx))

	// With one row, nothing flushes until Flush is called.
	require.NoError(t, sink.EmitRow(ctx, fooTopic, []byte(`k1`), []byte(`v0`), zeroTS, zeroTS, zeroAlloc))
	sqlDB.CheckQueryResults(t, `SELECT key, value FROM sink ORDER BY PRIMARY KEY sink`,
		[][]string{},
	)
	require.NoError(t, sink.Flush(ctx))
	sqlDB.CheckQueryResults(t, `SELECT key, value FROM sink ORDER BY PRIMARY KEY sink`,
		[][]string{{`k1`, `v0`}},
	)
	sqlDB.Exec(t, `TRUNCATE sink`)

	// Verify the implicit flushing
	sqlDB.CheckQueryResults(t, `SELECT count(*) FROM sink`, [][]string{{`0`}})
	for i := 0; i < sqlSinkRowBatchSize+1; i++ {
		require.NoError(t,
			sink.EmitRow(ctx, fooTopic, []byte(`k1`), []byte(`v`+strconv.Itoa(i)), zeroTS, zeroTS, zeroAlloc))
	}
	// Should have auto flushed after sqlSinkRowBatchSize
	sqlDB.CheckQueryResults(t, `SELECT count(*) FROM sink`, [][]string{{`3`}})
	require.NoError(t, sink.Flush(ctx))
	sqlDB.CheckQueryResults(t, `SELECT count(*) FROM sink`, [][]string{{`4`}})
	sqlDB.Exec(t, `TRUNCATE sink`)

	// Two tables interleaved in time
	var pool testAllocPool
	require.NoError(t, sink.EmitRow(ctx, fooTopic, []byte(`kfoo`), []byte(`v0`), zeroTS, zeroTS, pool.alloc()))
	require.NoError(t, sink.EmitRow(ctx, barTopic, []byte(`kbar`), []byte(`v0`), zeroTS, zeroTS, pool.alloc()))
	require.NoError(t, sink.EmitRow(ctx, fooTopic, []byte(`kfoo`), []byte(`v1`), zeroTS, zeroTS, pool.alloc()))
	require.NoError(t, sink.Flush(ctx))
	require.EqualValues(t, 0, pool.used())
	sqlDB.CheckQueryResults(t, `SELECT topic, key, value FROM sink ORDER BY PRIMARY KEY sink`,
		[][]string{{`bar`, `kbar`, `v0`}, {`foo`, `kfoo`, `v0`}, {`foo`, `kfoo`, `v1`}},
	)
	sqlDB.Exec(t, `TRUNCATE sink`)

	// Multiple keys interleaved in time. Use sqlSinkNumPartitions+1 keys to
	// guarantee that at lease two of them end up in the same partition.
	for i := 0; i < sqlSinkNumPartitions+1; i++ {
		require.NoError(t,
			sink.EmitRow(ctx, fooTopic, []byte(`v`+strconv.Itoa(i)), []byte(`v0`), zeroTS, zeroTS, zeroAlloc))
	}
	for i := 0; i < sqlSinkNumPartitions+1; i++ {
		require.NoError(t,
			sink.EmitRow(ctx, fooTopic, []byte(`v`+strconv.Itoa(i)), []byte(`v1`), zeroTS, zeroTS, zeroAlloc))
	}
	require.NoError(t, sink.Flush(ctx))
	sqlDB.CheckQueryResults(t, `SELECT partition, key, value FROM sink ORDER BY PRIMARY KEY sink`,
		[][]string{
			{`0`, `v3`, `v0`},
			{`0`, `v3`, `v1`},
			{`1`, `v1`, `v0`},
			{`1`, `v2`, `v0`},
			{`1`, `v1`, `v1`},
			{`1`, `v2`, `v1`},
			{`2`, `v0`, `v0`},
			{`2`, `v0`, `v1`},
		},
	)
	sqlDB.Exec(t, `TRUNCATE sink`)

	// Emit resolved
	var e testEncoder
	require.NoError(t, sink.EmitResolvedTimestamp(ctx, e, zeroTS))
	require.NoError(t, sink.EmitRow(ctx, fooTopic, []byte(`foo0`), []byte(`v0`), zeroTS, zeroTS, zeroAlloc))
	require.NoError(t, sink.EmitResolvedTimestamp(ctx, e, hlc.Timestamp{WallTime: 1}))
	require.NoError(t, sink.Flush(ctx))
	sqlDB.CheckQueryResults(t,
		`SELECT topic, partition, key, value, resolved FROM sink ORDER BY PRIMARY KEY sink`,
		[][]string{
			{`bar`, `0`, ``, ``, `0,0`},
			{`bar`, `0`, ``, ``, `0.000000001,0`},
			{`bar`, `1`, ``, ``, `0,0`},
			{`bar`, `1`, ``, ``, `0.000000001,0`},
			{`bar`, `2`, ``, ``, `0,0`},
			{`bar`, `2`, ``, ``, `0.000000001,0`},
			{`foo`, `0`, ``, ``, `0,0`},
			{`foo`, `0`, `foo0`, `v0`, ``},
			{`foo`, `0`, ``, ``, `0.000000001,0`},
			{`foo`, `1`, ``, ``, `0,0`},
			{`foo`, `1`, ``, ``, `0.000000001,0`},
			{`foo`, `2`, ``, ``, `0,0`},
			{`foo`, `2`, ``, ``, `0.000000001,0`},
		},
	)
}

func TestSaramaConfigOptionParsing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("defaults returned if not option set", func(t *testing.T) {
		opts := changefeedbase.SinkSpecificJSONConfig("")

		expected := defaultSaramaConfig()

		cfg, err := getSaramaConfig(opts)
		require.NoError(t, err)
		require.Equal(t, expected, cfg)
	})
	t.Run("options overlay defaults", func(t *testing.T) {
		opts := changefeedbase.SinkSpecificJSONConfig(`{"Flush": {"MaxMessages": 1000, "Frequency": "1s"}}`)

		expected := defaultSaramaConfig()
		expected.Flush.MaxMessages = 1000
		expected.Flush.Frequency = jsonDuration(time.Second)

		cfg, err := getSaramaConfig(opts)
		require.NoError(t, err)
		require.Equal(t, expected, cfg)
	})
	t.Run("validate returns nil for valid flush configuration", func(t *testing.T) {
		opts := changefeedbase.SinkSpecificJSONConfig(`{"Flush": {"Messages": 1000, "Frequency": "1s"}}`)

		cfg, err := getSaramaConfig(opts)
		require.NoError(t, err)
		require.NoError(t, cfg.Validate())

		opts = `{"Flush": {"Messages": 1}}`
		cfg, err = getSaramaConfig(opts)
		require.NoError(t, err)
		require.NoError(t, cfg.Validate())

		saramaCfg := sarama.NewConfig()
		opts = `{"ClientID": "clientID1"}`
		cfg, _ = getSaramaConfig(opts)
		err = cfg.Apply(saramaCfg)
		require.NoError(t, err)
		require.NoError(t, cfg.Validate())
		require.NoError(t, saramaCfg.Validate())

		opts = `{"Flush": {"Messages": 1000, "Frequency": "1s"}, "ClientID": "clientID1"}`
		cfg, _ = getSaramaConfig(opts)
		err = cfg.Apply(saramaCfg)
		require.NoError(t, err)
		require.NoError(t, cfg.Validate())
		require.NoError(t, saramaCfg.Validate())
		require.Equal(t, "clientID1", cfg.ClientID)
	})
	t.Run("validate returns error for bad flush configuration", func(t *testing.T) {
		opts := changefeedbase.SinkSpecificJSONConfig(`{"Flush": {"Messages": 1000}}`)

		cfg, err := getSaramaConfig(opts)
		require.NoError(t, err)
		require.Error(t, cfg.Validate())

		opts = `{"Flush": {"Bytes": 10}}`
		cfg, err = getSaramaConfig(opts)
		require.NoError(t, err)
		require.Error(t, cfg.Validate())

		opts = `{"Version": "0.8.2.0", "ClientID": "bad_client_id*"}`
		saramaCfg := sarama.NewConfig()
		cfg, _ = getSaramaConfig(opts)
		err = cfg.Apply(saramaCfg)
		require.NoError(t, err)
		require.NoError(t, cfg.Validate())
		require.Error(t, saramaCfg.Validate())
	})
	t.Run("apply parses valid version", func(t *testing.T) {
		opts := changefeedbase.SinkSpecificJSONConfig(`{"version": "0.8.2.0"}`)

		cfg, err := getSaramaConfig(opts)
		require.NoError(t, err)

		saramaCfg := &sarama.Config{}
		err = cfg.Apply(saramaCfg)
		require.NoError(t, err)
		require.Equal(t, sarama.V0_8_2_0, saramaCfg.Version)
	})
	t.Run("apply parses valid version with capitalized key", func(t *testing.T) {
		opts := changefeedbase.SinkSpecificJSONConfig(`{"Version": "0.8.2.0"}`)

		cfg, err := getSaramaConfig(opts)
		require.NoError(t, err)

		saramaCfg := &sarama.Config{}
		err = cfg.Apply(saramaCfg)
		require.NoError(t, err)
		require.Equal(t, sarama.V0_8_2_0, saramaCfg.Version)
	})
	t.Run("apply allows for unset version", func(t *testing.T) {
		opts := changefeedbase.SinkSpecificJSONConfig(`{}`)

		cfg, err := getSaramaConfig(opts)
		require.NoError(t, err)

		saramaCfg := &sarama.Config{}
		err = cfg.Apply(saramaCfg)
		require.NoError(t, err)
		require.Equal(t, sarama.KafkaVersion{}, saramaCfg.Version)
	})
	t.Run("apply errors if version is invalid", func(t *testing.T) {
		opts := changefeedbase.SinkSpecificJSONConfig(`{"version": "invalid"}`)

		cfg, err := getSaramaConfig(opts)
		require.NoError(t, err)

		saramaCfg := &sarama.Config{}
		err = cfg.Apply(saramaCfg)
		require.Error(t, err)
	})
	t.Run("apply parses RequiredAcks", func(t *testing.T) {
		opts := changefeedbase.SinkSpecificJSONConfig(`{"RequiredAcks": "ALL"}`)

		cfg, err := getSaramaConfig(opts)
		require.NoError(t, err)

		saramaCfg := &sarama.Config{}
		err = cfg.Apply(saramaCfg)
		require.NoError(t, err)
		require.Equal(t, sarama.WaitForAll, saramaCfg.Producer.RequiredAcks)

		opts = `{"RequiredAcks": "-1"}`

		cfg, err = getSaramaConfig(opts)
		require.NoError(t, err)

		saramaCfg = &sarama.Config{}
		err = cfg.Apply(saramaCfg)
		require.NoError(t, err)
		require.Equal(t, sarama.WaitForAll, saramaCfg.Producer.RequiredAcks)

	})
	t.Run("apply errors if RequiredAcks is invalid", func(t *testing.T) {
		opts := changefeedbase.SinkSpecificJSONConfig(`{"RequiredAcks": "LocalQuorum"}`)

		cfg, err := getSaramaConfig(opts)
		require.NoError(t, err)

		saramaCfg := &sarama.Config{}
		err = cfg.Apply(saramaCfg)
		require.Error(t, err)

	})
	t.Run("compression options validation", func(t *testing.T) {
		testCases := make([]string, 0, len(saramaCompressionCodecOptions)*2)
		for option := range saramaCompressionCodecOptions {
			testCases = append(testCases, option, strings.ToLower(option))
		}
		for _, option := range testCases {
			opts := changefeedbase.SinkSpecificJSONConfig(fmt.Sprintf(`{"Compression": "%s"}`, option))
			cfg, err := getSaramaConfig(opts)
			require.NoError(t, err)

			saramaCfg := &sarama.Config{}
			err = cfg.Apply(saramaCfg)
			require.NoError(t, err)
		}

		forEachSupportedCodec := func(fn func(name string, c sarama.CompressionCodec)) {
			defer func() { _ = recover() }()

			for c := sarama.CompressionCodec(0); c.String() != ""; c++ {
				fn(c.String(), c)
			}
		}

		forEachSupportedCodec(func(option string, c sarama.CompressionCodec) {
			opts := changefeedbase.SinkSpecificJSONConfig(fmt.Sprintf(`{"Compression": "%s"}`, option))
			cfg, err := getSaramaConfig(opts)
			require.NoError(t, err)

			saramaCfg := &sarama.Config{}
			err = cfg.Apply(saramaCfg)
			require.NoError(t, err)
			require.Equal(t, c, saramaCfg.Producer.Compression)
		})

		opts := changefeedbase.SinkSpecificJSONConfig(`{"Compression": "invalid"}`)
		_, err := getSaramaConfig(opts)
		require.Error(t, err)
	})
	t.Run("validate returns nil for valid compression codec and level", func(t *testing.T) {
		tests := []struct {
			give      changefeedbase.SinkSpecificJSONConfig
			wantCodec sarama.CompressionCodec
			wantLevel int
		}{
			{
				give:      `{"Compression": "GZIP"}`,
				wantCodec: sarama.CompressionGZIP,
				wantLevel: sarama.CompressionLevelDefault,
			},
			{
				give:      `{"CompressionLevel": 1}`,
				wantCodec: sarama.CompressionNone,
				wantLevel: 1,
			},
			{
				give:      `{"Compression": "GZIP", "CompressionLevel": 1}`,
				wantCodec: sarama.CompressionGZIP,
				wantLevel: 1,
			},
			{
				give:      `{"Compression": "ZSTD", "CompressionLevel": 1}`,
				wantCodec: sarama.CompressionZSTD,
				wantLevel: 1,
			},
			{
				// The maximum supported zstd compression level is 10,
				// so all higher values are valid and limited by it.
				give:      `{"Compression": "ZSTD", "CompressionLevel": 11}`,
				wantCodec: sarama.CompressionZSTD,
				wantLevel: 11,
			},
		}

		for _, tt := range tests {
			t.Run(string(tt.give), func(t *testing.T) {
				cfg, err := getSaramaConfig(tt.give)
				require.NoError(t, err)

				saramaCfg := sarama.NewConfig()
				require.NoError(t, cfg.Apply(saramaCfg))
				require.NoError(t, saramaCfg.Validate())
				require.Equal(t, tt.wantCodec, saramaCfg.Producer.Compression)
				require.Equal(t, tt.wantLevel, saramaCfg.Producer.CompressionLevel)
			})
		}
	})
	t.Run("validate returns err for bad compression level", func(t *testing.T) {
		opts := changefeedbase.SinkSpecificJSONConfig(`{"Compression": "GZIP", "CompressionLevel": "invalid"}`)
		_, err := getSaramaConfig(opts)
		require.ErrorContains(t, err, "field saramaConfig.CompressionLevel of type int")

		// The maximum gzip compression level is gzip.BestCompression,
		// so use gzip.BestCompression + 1.
		opts = `{"Compression": "GZIP", "CompressionLevel": 10}`
		cfg, err := getSaramaConfig(opts)
		require.NoError(t, err)

		saramaCfg := sarama.NewConfig()
		require.NoError(t, cfg.Apply(saramaCfg))
		require.ErrorContains(t, saramaCfg.Validate(), "gzip: invalid compression level: 10")
	})
}

func TestKafkaSinkTracksMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Use fake kafka sink which "consumes" all messages on its input channel,
	// but does not acknowledge them automatically (i.e. slow sink)
	p := newAsyncProducerMock(unbuffered)
	stopConsume := p.consume()

	sink, cleanup := makeTestKafkaSink(
		t, noTopicPrefix, defaultTopicName, p, "t")
	defer func() {
		stopConsume()
		cleanup()
	}()

	// No inflight
	require.NoError(t, sink.Flush(ctx))

	// Emit few messages
	rnd, _ := randutil.NewTestRand()
	key := randutil.RandBytes(rnd, 1+rnd.Intn(64))
	val := randutil.RandBytes(rnd, 1+rnd.Intn(512))

	testTopic := topic(`t`)
	var pool testAllocPool
	for i := 0; i < 10; i++ {
		require.NoError(t, sink.EmitRow(ctx, testTopic, key, val, zeroTS, zeroTS, pool.alloc()))
	}
	require.EqualValues(t, 10, pool.used())

	// Acknowledge outstanding messages, and flush.
	p.acknowledge(10, p.successesCh)
	require.NoError(t, sink.Flush(ctx))
	require.EqualValues(t, 0, p.outstanding())
	require.EqualValues(t, 0, pool.used())
}

func TestSinkConfigParsing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("handles valid types", func(t *testing.T) {
		opts := changefeedbase.SinkSpecificJSONConfig(`{"Flush": {"Messages": 1234, "Frequency": "3s", "Bytes":30}, "Retry": {"Max": 5, "Backoff": "3h"}}`)
		batch, retry, err := getSinkConfigFromJson(opts, sinkJSONConfig{})
		require.NoError(t, err)
		require.Equal(t, batch, sinkBatchConfig{
			Bytes:     30,
			Messages:  1234,
			Frequency: jsonDuration(3 * time.Second),
		})
		require.Equal(t, retry.MaxRetries, 5)
		require.Equal(t, retry.InitialBackoff, 3*time.Hour)

		// Max accepts both values and specifically the string "inf"
		opts = changefeedbase.SinkSpecificJSONConfig(`{"Retry": {"Max": "inf"}}`)
		_, retry, err = getSinkConfigFromJson(opts, sinkJSONConfig{})
		require.NoError(t, err)
		require.Equal(t, retry.MaxRetries, 0)
	})

	t.Run("provides retry defaults", func(t *testing.T) {
		defaultRetry := defaultRetryConfig()

		opts := changefeedbase.SinkSpecificJSONConfig(`{"Flush": {"Messages": 1234, "Frequency": "3s"}}`)
		_, retry, err := getSinkConfigFromJson(opts, sinkJSONConfig{})
		require.NoError(t, err)

		require.Equal(t, retry.MaxRetries, defaultRetry.MaxRetries)
		require.Equal(t, retry.InitialBackoff, defaultRetry.InitialBackoff)

		opts = changefeedbase.SinkSpecificJSONConfig(`{"Retry": {"Max": "inf"}}`)
		_, retry, err = getSinkConfigFromJson(opts, sinkJSONConfig{})
		require.NoError(t, err)
		require.Equal(t, retry.MaxRetries, 0)
		require.Equal(t, retry.InitialBackoff, defaultRetry.InitialBackoff)
	})

	t.Run("errors on invalid configuration", func(t *testing.T) {
		opts := changefeedbase.SinkSpecificJSONConfig(`{"Flush": {"Messages": -1234, "Frequency": "3s"}}`)
		_, _, err := getSinkConfigFromJson(opts, sinkJSONConfig{})
		require.ErrorContains(t, err, "invalid sink config, all values must be non-negative")

		opts = changefeedbase.SinkSpecificJSONConfig(`{"Flush": {"Messages": 1234, "Frequency": "-3s"}}`)
		_, _, err = getSinkConfigFromJson(opts, sinkJSONConfig{})
		require.ErrorContains(t, err, "invalid sink config, all values must be non-negative")

		opts = changefeedbase.SinkSpecificJSONConfig(`{"Flush": {"Messages": 10}}`)
		_, _, err = getSinkConfigFromJson(opts, sinkJSONConfig{})
		require.ErrorContains(t, err, "invalid sink config, Flush.Frequency is not set, messages may never be sent")
	})

	t.Run("errors on invalid type", func(t *testing.T) {
		opts := changefeedbase.SinkSpecificJSONConfig(`{"Flush": {"Messages": "1234", "Frequency": "3s", "Bytes":30}}`)
		_, _, err := getSinkConfigFromJson(opts, sinkJSONConfig{})
		require.ErrorContains(t, err, "Flush.Messages of type int")

		opts = changefeedbase.SinkSpecificJSONConfig(`{"Flush": {"Messages": 1234, "Frequency": "3s", "Bytes":"30"}}`)
		_, _, err = getSinkConfigFromJson(opts, sinkJSONConfig{})
		require.ErrorContains(t, err, "Flush.Bytes of type int")

		opts = changefeedbase.SinkSpecificJSONConfig(`{"Retry": {"Max": true}}`)
		_, _, err = getSinkConfigFromJson(opts, sinkJSONConfig{})
		require.ErrorContains(t, err, "Retry.Max must be either a positive int or 'inf'")

		opts = changefeedbase.SinkSpecificJSONConfig(`{"Retry": {"Max": "not-inf"}}`)
		_, _, err = getSinkConfigFromJson(opts, sinkJSONConfig{})
		require.ErrorContains(t, err, "Retry.Max must be either a positive int or 'inf'")
	})

	t.Run("errors on malformed json", func(t *testing.T) {
		opts := changefeedbase.SinkSpecificJSONConfig(`{"Flush": {"Messages": 1234 "Frequency": "3s"}}`)
		_, _, err := getSinkConfigFromJson(opts, sinkJSONConfig{})
		require.ErrorContains(t, err, "invalid character '\"'")

		opts = changefeedbase.SinkSpecificJSONConfig(`string`)
		_, _, err = getSinkConfigFromJson(opts, sinkJSONConfig{})
		require.ErrorContains(t, err, "invalid character 's' looking for beginning of value")
	})
}

func TestChangefeedConsistentPartitioning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// We test that these arbitrary strings get mapped to these
	// arbitrary partitions to ensure that if an upgrade occurs
	// while a changefeed is running, partitioning remains the same
	// and therefore ordering guarantees are preserved. Changing
	// these values is a breaking change.
	referencePartitions := map[string]int32{
		"0":         1003,
		"01":        351,
		"10":        940,
		"a":         292,
		"\x00":      732,
		"\xff \xff": 164,
	}
	longString1 := strings.Repeat("a", 2048)
	referencePartitions[longString1] = 755
	longString2 := strings.Repeat("a", 2047) + "A"
	referencePartitions[longString2] = 592

	partitioner := newChangefeedPartitioner("topic1")
	kgoPartitioner := newKgoChangefeedPartitioner().ForTopic("topic1")

	for key, expected := range referencePartitions {
		actual, err := partitioner.Partition(&sarama.ProducerMessage{Key: sarama.ByteEncoder(key)}, 1031)
		require.NoError(t, err)
		require.Equal(t, expected, actual)

		actual = int32(kgoPartitioner.Partition(&kgo.Record{Key: []byte(key)}, 1031))
		require.Equal(t, expected, actual)
	}

}
