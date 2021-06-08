// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"math"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
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

func topic(name string) tableDescriptorTopic {
	return tableDescriptorTopic{tabledesc.NewBuilder(&descpb.TableDescriptor{Name: name}).BuildImmutableTable()}
}

const memoryUnlimited int64 = math.MaxInt64
const noTopicPrefix = ""
const defaultTopicName = ""

func getBoundAccountWithBudget(budget int64) (account mon.BoundAccount, cleanup func()) {
	mm := mon.NewMonitorWithLimit(
		"test-mm", mon.MemoryResource, budget,
		nil, nil, mon.DefaultPoolAllocationSize, 100,
		cluster.MakeTestingClusterSettings())
	mm.Start(context.Background(), nil, mon.MakeStandaloneBudget(budget))
	return mm.MakeBoundAccount(), func() { mm.Stop(context.Background()) }
}

func makeTestKafkaSink(
	t testing.TB,
	topicPrefix string,
	topicNameOverride string,
	p sarama.AsyncProducer,
	budget int64,
	targetNames ...string,
) (s *kafkaSink, cleanup func()) {
	mem, release := getBoundAccountWithBudget(budget)

	targets := makeChangefeedTargets(targetNames...)

	s = &kafkaSink{
		ctx:      context.Background(),
		topics:   makeTopicsMap(topicPrefix, topicNameOverride, targets),
		producer: p,
	}
	s.mu.mem = mem
	s.start()

	return s, func() {
		require.NoError(t, s.Close())
		release()
	}
}

func makeChangefeedTargets(targetNames ...string) jobspb.ChangefeedTargets {
	targets := make(jobspb.ChangefeedTargets, len(targetNames))
	for i, name := range targetNames {
		targets[descpb.ID(i)] = jobspb.ChangefeedTarget{StatementTimeName: name}
	}
	return targets
}

func TestKafkaSink(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	p := newAsyncProducerMock(1)
	sink, cleanup := makeTestKafkaSink(
		t, noTopicPrefix, defaultTopicName, p, memoryUnlimited, "t")
	defer cleanup()

	// No inflight
	if err := sink.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Timeout
	if err := sink.EmitRow(ctx, topic(`t`), []byte(`1`), nil, zeroTS); err != nil {
		t.Fatal(err)
	}
	m1 := <-p.inputCh
	for i := 0; i < 2; i++ {
		timeoutCtx, cancel := context.WithTimeout(ctx, time.Millisecond)
		defer cancel()
		if err := sink.Flush(timeoutCtx); !testutils.IsError(
			err, `context deadline exceeded`,
		) {
			t.Fatalf(`expected "context deadline exceeded" error got: %+v`, err)
		}
	}
	go func() { p.successesCh <- m1 }()
	if err := sink.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Check no inflight again now that we've sent something
	if err := sink.Flush(ctx); err != nil {
		t.Fatal(err)
	}

	// Mixed success and error.
	if err := sink.EmitRow(ctx, topic(`t`), []byte(`2`), nil, zeroTS); err != nil {
		t.Fatal(err)
	}
	m2 := <-p.inputCh
	if err := sink.EmitRow(ctx, topic(`t`), []byte(`3`), nil, zeroTS); err != nil {
		t.Fatal(err)
	}
	m3 := <-p.inputCh
	if err := sink.EmitRow(ctx, topic(`t`), []byte(`4`), nil, zeroTS); err != nil {
		t.Fatal(err)
	}
	m4 := <-p.inputCh
	go func() { p.successesCh <- m2 }()
	go func() {
		p.errorsCh <- &sarama.ProducerError{
			Msg: m3,
			Err: errors.New("m3"),
		}
	}()
	go func() { p.successesCh <- m4 }()
	if err := sink.Flush(ctx); !testutils.IsError(err, `m3`) {
		t.Fatalf(`expected "m3" error got: %+v`, err)
	}

	// Check simple success again after error
	if err := sink.EmitRow(ctx, topic(`t`), []byte(`5`), nil, zeroTS); err != nil {
		t.Fatal(err)
	}
	m5 := <-p.inputCh
	go func() { p.successesCh <- m5 }()
	if err := sink.Flush(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestKafkaSinkEscaping(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	p := newAsyncProducerMock(1)
	sink, cleanup := makeTestKafkaSink(
		t, noTopicPrefix, defaultTopicName, p, memoryUnlimited, `☃`)
	defer cleanup()

	if err := sink.EmitRow(ctx, topic(`☃`), []byte(`k☃`), []byte(`v☃`), zeroTS); err != nil {
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
		t, noTopicPrefix, topicOverride, p, memoryUnlimited, "particular0", "particular1")
	defer cleanup()

	//all messages go to the general topic
	require.NoError(t, sink.EmitRow(ctx, topic("particular0"), []byte(`k☃`), []byte(`v☃`), zeroTS))
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
		t, topicPrefix, topicOverride, p, memoryUnlimited, "particular0", "particular1")
	defer clenaup()

	//the prefix is applied and the name is escaped
	require.NoError(t, sink.EmitRow(ctx, topic("particular0"), []byte(`k☃`), []byte(`v☃`), zeroTS))
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
	sink, cleanup := makeTestKafkaSink(b, noTopicPrefix, defaultTopicName, p, memoryUnlimited, tableName)
	stopConsume := p.consumeAndSucceed()
	defer func() {
		stopConsume()
		cleanup()
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		require.NoError(b, sink.EmitRow(ctx, topic, []byte(`k☃`), []byte(`v☃`), hlc.Timestamp{}))
	}

	b.ReportAllocs()
}

type testEncoder struct{}

func (testEncoder) EncodeKey(context.Context, encodeRow) ([]byte, error)   { panic(`unimplemented`) }
func (testEncoder) EncodeValue(context.Context, encodeRow) ([]byte, error) { panic(`unimplemented`) }
func (testEncoder) EncodeResolvedTimestamp(
	_ context.Context, _ string, ts hlc.Timestamp,
) ([]byte, error) {
	return []byte(ts.String()), nil
}

func TestSQLSink(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	overrideTopic := func(name string) tableDescriptorTopic {
		id, _ := strconv.ParseUint(name, 36, 64)
		return tableDescriptorTopic{
			tabledesc.NewBuilder(&descpb.TableDescriptor{Name: name, ID: descpb.ID(id)}).BuildImmutableTable()}
	}

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "d"})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)
	sqlDB.Exec(t, `CREATE DATABASE d`)

	pgURL, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
	defer cleanup()
	pgURL.Path = `d`

	fooTopic := overrideTopic(`foo`)
	barTopic := overrideTopic(`bar`)
	targets := jobspb.ChangefeedTargets{
		fooTopic.GetID(): jobspb.ChangefeedTarget{StatementTimeName: `foo`},
		barTopic.GetID(): jobspb.ChangefeedTarget{StatementTimeName: `bar`},
	}
	const testTableName = `sink`
	sink, err := makeSQLSink(sinkURL{URL: &pgURL}, testTableName, targets)
	require.NoError(t, err)
	require.NoError(t, sink.(*sqlSink).Dial())
	defer func() { require.NoError(t, sink.Close()) }()

	// Empty
	require.NoError(t, sink.Flush(ctx))

	// Undeclared topic
	require.EqualError(t,
		sink.EmitRow(ctx, overrideTopic(`nope`), nil, nil, zeroTS), `cannot emit to undeclared topic: `)

	// With one row, nothing flushes until Flush is called.
	require.NoError(t, sink.EmitRow(ctx, fooTopic, []byte(`k1`), []byte(`v0`), zeroTS))
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
			sink.EmitRow(ctx, fooTopic, []byte(`k1`), []byte(`v`+strconv.Itoa(i)), zeroTS))
	}
	// Should have auto flushed after sqlSinkRowBatchSize
	sqlDB.CheckQueryResults(t, `SELECT count(*) FROM sink`, [][]string{{`3`}})
	require.NoError(t, sink.Flush(ctx))
	sqlDB.CheckQueryResults(t, `SELECT count(*) FROM sink`, [][]string{{`4`}})
	sqlDB.Exec(t, `TRUNCATE sink`)

	// Two tables interleaved in time
	require.NoError(t, sink.EmitRow(ctx, fooTopic, []byte(`kfoo`), []byte(`v0`), zeroTS))
	require.NoError(t, sink.EmitRow(ctx, barTopic, []byte(`kbar`), []byte(`v0`), zeroTS))
	require.NoError(t, sink.EmitRow(ctx, fooTopic, []byte(`kfoo`), []byte(`v1`), zeroTS))
	require.NoError(t, sink.Flush(ctx))
	sqlDB.CheckQueryResults(t, `SELECT topic, key, value FROM sink ORDER BY PRIMARY KEY sink`,
		[][]string{{`bar`, `kbar`, `v0`}, {`foo`, `kfoo`, `v0`}, {`foo`, `kfoo`, `v1`}},
	)
	sqlDB.Exec(t, `TRUNCATE sink`)

	// Multiple keys interleaved in time. Use sqlSinkNumPartitions+1 keys to
	// guarantee that at lease two of them end up in the same partition.
	for i := 0; i < sqlSinkNumPartitions+1; i++ {
		require.NoError(t,
			sink.EmitRow(ctx, fooTopic, []byte(`v`+strconv.Itoa(i)), []byte(`v0`), zeroTS))
	}
	for i := 0; i < sqlSinkNumPartitions+1; i++ {
		require.NoError(t,
			sink.EmitRow(ctx, fooTopic, []byte(`v`+strconv.Itoa(i)), []byte(`v1`), zeroTS))
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
	require.NoError(t, sink.EmitRow(ctx, fooTopic, []byte(`foo0`), []byte(`v0`), zeroTS))
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
		opts := make(map[string]string)

		expected := defaultSaramaConfig()

		cfg, err := getSaramaConfig(opts)
		require.NoError(t, err)
		require.Equal(t, expected, cfg)
	})
	t.Run("options overlay defaults", func(t *testing.T) {
		opts := make(map[string]string)
		opts[changefeedbase.OptKafkaSinkConfig] = `{"Flush": {"MaxMessages": 1000, "Frequency": "1s"}}`

		expected := defaultSaramaConfig()
		expected.Flush.MaxMessages = 1000
		expected.Flush.Frequency = jsonDuration(time.Second)

		cfg, err := getSaramaConfig(opts)
		require.NoError(t, err)
		require.Equal(t, expected, cfg)
	})
	t.Run("validate returns nil for valid flush configuration", func(t *testing.T) {
		opts := make(map[string]string)

		opts[changefeedbase.OptKafkaSinkConfig] = `{"Flush": {"Messages": 1000, "Frequency": "1s"}}`
		cfg, err := getSaramaConfig(opts)
		require.NoError(t, err)
		require.NoError(t, cfg.Validate())

		opts[changefeedbase.OptKafkaSinkConfig] = `{"Flush": {"Messages": 1}}`
		cfg, err = getSaramaConfig(opts)
		require.NoError(t, err)
		require.NoError(t, cfg.Validate())
	})
	t.Run("validate returns error for bad flush configurationg", func(t *testing.T) {
		opts := make(map[string]string)
		opts[changefeedbase.OptKafkaSinkConfig] = `{"Flush": {"Messages": 1000}}`

		cfg, err := getSaramaConfig(opts)
		require.NoError(t, err)
		require.Error(t, cfg.Validate())

		opts[changefeedbase.OptKafkaSinkConfig] = `{"Flush": {"Bytes": 10}}`
		cfg, err = getSaramaConfig(opts)
		require.NoError(t, err)
		require.Error(t, cfg.Validate())
	})
	t.Run("apply parses valid version", func(t *testing.T) {
		opts := make(map[string]string)
		opts[changefeedbase.OptKafkaSinkConfig] = `{"version": "0.8.2.0"}`

		cfg, err := getSaramaConfig(opts)
		require.NoError(t, err)

		saramaCfg := &sarama.Config{}
		err = cfg.Apply(saramaCfg)
		require.NoError(t, err)
		require.Equal(t, sarama.V0_8_2_0, saramaCfg.Version)
	})
	t.Run("apply allows for unset version", func(t *testing.T) {
		opts := make(map[string]string)
		opts[changefeedbase.OptKafkaSinkConfig] = `{}`

		cfg, err := getSaramaConfig(opts)
		require.NoError(t, err)

		saramaCfg := &sarama.Config{}
		err = cfg.Apply(saramaCfg)
		require.NoError(t, err)
		require.Equal(t, sarama.KafkaVersion{}, saramaCfg.Version)
	})
	t.Run("apply errors if version is invalid", func(t *testing.T) {
		opts := make(map[string]string)
		opts[changefeedbase.OptKafkaSinkConfig] = `{"version": "invalid"}`

		cfg, err := getSaramaConfig(opts)
		require.NoError(t, err)

		saramaCfg := &sarama.Config{}
		err = cfg.Apply(saramaCfg)
		require.Error(t, err)
	})
}

func TestKafkaSinkTracksMemory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	memCapacity := mon.DefaultPoolAllocationSize

	// Use fake kafka sink which "consumes" all messages on its input channel,
	// but does not acknowledge them automatically (i.e. slow sink)
	p := newAsyncProducerMock(unbuffered)
	stopConsume := p.consume()

	sink, cleanup := makeTestKafkaSink(
		t, noTopicPrefix, defaultTopicName, p, memCapacity, "t")
	defer func() {
		stopConsume()
		cleanup()
	}()

	// No inflight
	require.NoError(t, sink.Flush(ctx))

	// Emit few messages
	rnd, _ := randutil.NewTestPseudoRand()
	key := randutil.RandBytes(rnd, 1+rnd.Intn(64))
	val := randutil.RandBytes(rnd, 1+rnd.Intn(512))
	kvLen := int64(len(key)) + int64(len(val))

	testTopic := topic(`t`)
	for i := 0; i < 10; i++ {
		require.NoError(t, sink.EmitRow(ctx, testTopic, key, val, zeroTS))
	}

	memUsed := func() int64 {
		sink.mu.Lock()
		defer sink.mu.Unlock()
		return sink.mu.mem.Used()
	}
	require.Equal(t, 10*kvLen, memUsed())

	// Acknowledge outstanding messages, and flush.
	p.acknowledge(10, p.successesCh)
	require.NoError(t, sink.Flush(ctx))
	require.EqualValues(t, 0, p.outstanding())

	// Try emitting resolved timestamp.  This message type is different from the
	// regular messages since it doesn't have Key set.
	// We bypass majority of EmitResolvedTimestamp logic since we don't have
	// a real kafka client instantiated.  Instead, we call emitMessage directly.
	reg := cdctest.StartTestSchemaRegistry()
	defer reg.Close()
	opts := map[string]string{
		changefeedbase.OptEnvelope:                string(changefeedbase.OptEnvelopeWrapped),
		changefeedbase.OptConfluentSchemaRegistry: reg.URL(),
	}
	encoder, err := newConfluentAvroEncoder(ctx, opts, makeChangefeedTargets("t"))
	require.NoError(t, err)
	payload, err := encoder.EncodeResolvedTimestamp(ctx, "t", hlc.Timestamp{})
	require.NoError(t, err)
	msg := &sarama.ProducerMessage{
		Topic: "t",
		Key:   nil,
		Value: sarama.ByteEncoder(payload),
	}
	require.NoError(t, sink.emitMessage(ctx, msg))
	p.acknowledge(1, p.successesCh)
	require.NoError(t, sink.Flush(ctx))
	require.EqualValues(t, 0, p.outstanding())

	// Try to emit more than we can handle.
	expectOverflow := memCapacity / kvLen
	for err == nil {
		err = sink.EmitRow(ctx, testTopic, key, val, zeroTS)
	}

	require.Regexp(t, `memory budget exceeded`, err)
	// We failed to allocate more memory, but we should have used
	// memory for the expectOverflow key/values.
	require.EqualValues(t, expectOverflow*kvLen, memUsed())
}
