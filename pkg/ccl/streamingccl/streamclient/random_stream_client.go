// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streamclient

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const (
	// RandomStreamSchemaPlaceholder is the schema of the KVs emitted by the
	// random stream client.
	RandomStreamSchemaPlaceholder = "CREATE TABLE %s (k INT PRIMARY KEY, v INT)"

	// RandomGenScheme is the URI scheme used to create a test load.
	RandomGenScheme = "randomgen"
	// ValueRangeKey controls the range of the randomly generated values produced
	// by this workload. The workload will generate between 0 and this value.
	ValueRangeKey = "VALUE_RANGE"
	// EventFrequency is the frequency in nanoseconds that the stream will emit
	// randomly generated KV events.
	EventFrequency = "EVENT_FREQUENCY"
	// EventsPerCheckpoint controls approximately how many data events (KV/SST/DelRange)
	// should be emitted between checkpoint events.
	EventsPerCheckpoint = "EVENTS_PER_CHECKPOINT"
	// NumPartitions controls the number of partitions the client will stream data
	// back on. Each partition will encompass a single table span.
	NumPartitions = "NUM_PARTITIONS"
	// DupProbability controls the probability with which we emit duplicate data
	// events.
	DupProbability = "DUP_PROBABILITY"
	// SSTProbability controls the probability with which we emit SST event.
	SSTProbability = "SST_PROBABILITY"
	// TenantID specifies the ID of the tenant we are ingesting data into. This
	// allows the client to prefix the generated KVs with the appropriate tenant
	// prefix.
	// TODO(casper): ensure this should be consistent across the usage of APIs
	TenantID = "TENANT_ID"
	// TenantSpanStart and TenantSpanEnd are bool params to extend checkpoints to
	// the tenant span start or end key.
	TenantSpanStart = "TENANT_START"
	TenantSpanEnd   = "TENANT_END"
	// IngestionDatabaseID is the ID used in the generated table descriptor.
	IngestionDatabaseID = 50 /* defaultDB */
	// IngestionTablePrefix is the prefix of the table name used in the generated
	// table descriptor.
	IngestionTablePrefix = "foo"
)

// TODO(dt): just make interceptors a singleton, not the whole client.
var randomStreamClientSingleton = func() *RandomStreamClient {
	c := RandomStreamClient{}
	// Make the base tableID really large to prevent colliding with system table IDs.
	c.mu.tableID = 5000
	return &c
}()

// GetRandomStreamClientSingletonForTesting returns the singleton instance of
// the client. This is to be used in testing, when interceptors can be
// registered on the client to observe events.
func GetRandomStreamClientSingletonForTesting() *RandomStreamClient {
	return randomStreamClientSingleton
}

// InterceptFn is a function that will intercept events emitted by
// an InterceptableStreamClient
type InterceptFn func(event streamingccl.Event, spec SubscriptionToken)

// DialInterceptFn is a function that will intercept Dial calls made to an
// InterceptableStreamClient
type DialInterceptFn func(streamURL *url.URL) error

// HeartbeatInterceptFn is a function that will intercept calls to a client's
// Heartbeat.
type HeartbeatInterceptFn func(timestamp hlc.Timestamp)

// SSTableMakerFn is a function that generates RangeFeedSSTable event
// with a given list of roachpb.KeyValue.
type SSTableMakerFn func(keyValues []roachpb.KeyValue) kvpb.RangeFeedSSTable

// randomStreamConfig specifies the variables that controls the rate and type of
// events that the generated stream emits.
type randomStreamConfig struct {
	valueRange          int
	eventFrequency      time.Duration
	eventsPerCheckpoint int
	numPartitions       int
	dupProbability      float64
	sstProbability      float64

	tenantID roachpb.TenantID
	// startTenant and endTenant extend checkpoint spans to the tenant span.
	startTenant, endTenant bool
}

func parseRandomStreamConfig(streamURL *url.URL) (randomStreamConfig, error) {
	c := randomStreamConfig{
		valueRange:          100,
		eventFrequency:      10 * time.Microsecond,
		eventsPerCheckpoint: 30,
		numPartitions:       1, // TODO(casper): increases this
		dupProbability:      0.3,
		sstProbability:      0.2,
		tenantID:            roachpb.SystemTenantID,
	}

	var err error
	if valueRangeStr := streamURL.Query().Get(ValueRangeKey); valueRangeStr != "" {
		c.valueRange, err = strconv.Atoi(valueRangeStr)
		if err != nil {
			return c, err
		}
	}

	if eventFreqStr := streamURL.Query().Get(EventFrequency); eventFreqStr != "" {
		eventFreq, err := strconv.Atoi(eventFreqStr)
		c.eventFrequency = time.Duration(eventFreq)
		if err != nil {
			return c, err
		}
	}

	if eventsPerCheckpointStr := streamURL.Query().Get(EventsPerCheckpoint); eventsPerCheckpointStr != "" {
		c.eventsPerCheckpoint, err = strconv.Atoi(eventsPerCheckpointStr)
		if err != nil {
			return c, err
		}
	}

	if sstProbabilityStr := streamURL.Query().Get(SSTProbability); sstProbabilityStr != "" {
		c.sstProbability, err = strconv.ParseFloat(sstProbabilityStr, 32)
		if err != nil {
			return c, err
		}
	}

	if numPartitionsStr := streamURL.Query().Get(NumPartitions); numPartitionsStr != "" {
		c.numPartitions, err = strconv.Atoi(numPartitionsStr)
		if err != nil {
			return c, err
		}
	}

	if dupProbStr := streamURL.Query().Get(DupProbability); dupProbStr != "" {
		c.dupProbability, err = strconv.ParseFloat(dupProbStr, 32)
		if err != nil {
			return c, err
		}
	}

	if tenantIDStr := streamURL.Query().Get(TenantID); tenantIDStr != "" {
		id, err := strconv.Atoi(tenantIDStr)
		if err != nil {
			return c, err
		}
		c.tenantID = roachpb.MustMakeTenantID(uint64(id))
	}

	if s := streamURL.Query().Get(TenantSpanStart); s != "" {
		b, err := strconv.ParseBool(s)
		if err != nil {
			return c, err
		}
		c.startTenant = b
	}
	if s := streamURL.Query().Get(TenantSpanEnd); s != "" {
		b, err := strconv.ParseBool(s)
		if err != nil {
			return c, err
		}
		c.endTenant = b
	}

	return c, nil
}

func (c randomStreamConfig) URL(table int, startsTenant, finishesTenant bool) string {
	u := &url.URL{
		Scheme: RandomGenScheme,
		Host:   strconv.Itoa(table),
	}
	q := u.Query()
	q.Add(ValueRangeKey, strconv.Itoa(c.valueRange))
	q.Add(EventFrequency, strconv.Itoa(int(c.eventFrequency)))
	q.Add(EventsPerCheckpoint, strconv.Itoa(c.eventsPerCheckpoint))
	q.Add(NumPartitions, strconv.Itoa(c.numPartitions))
	q.Add(DupProbability, fmt.Sprintf("%f", c.dupProbability))
	q.Add(SSTProbability, fmt.Sprintf("%f", c.sstProbability))
	q.Add(TenantSpanStart, strconv.FormatBool(startsTenant))
	q.Add(TenantSpanEnd, strconv.FormatBool(finishesTenant))
	q.Add(TenantID, strconv.Itoa(int(c.tenantID.ToUint64())))
	u.RawQuery = q.Encode()
	return u.String()
}

type randomEventGenerator struct {
	rng                        *rand.Rand
	config                     randomStreamConfig
	numEventsSinceLastResolved int
	sstMaker                   SSTableMakerFn
	codec                      keys.SQLCodec
	tableDesc                  *tabledesc.Mutable
	systemKVs                  []roachpb.KeyValue
}

func newRandomEventGenerator(
	rng *rand.Rand, partitionURL *url.URL, config randomStreamConfig, fn SSTableMakerFn,
) (*randomEventGenerator, error) {
	var partitionTableID int
	partitionTableID, err := strconv.Atoi(partitionURL.Host)
	if err != nil {
		return nil, err
	}
	tableDesc, systemKVs, err := getDescriptorAndNamespaceKVForTableID(config, descpb.ID(partitionTableID))
	if err != nil {
		return nil, err
	}
	return &randomEventGenerator{
		rng:                        rng,
		config:                     config,
		numEventsSinceLastResolved: 0,
		sstMaker:                   fn,
		tableDesc:                  tableDesc,
		codec:                      keys.MakeSQLCodec(config.tenantID),
		systemKVs:                  systemKVs,
	}, nil
}

func (r *randomEventGenerator) generateNewEvent() streamingccl.Event {
	var event streamingccl.Event
	if r.numEventsSinceLastResolved == r.config.eventsPerCheckpoint {
		sp := r.tableDesc.TableSpan(r.codec)
		if r.config.startTenant {
			sp.Key = keys.MakeTenantSpan(r.config.tenantID).Key
		}
		if r.config.endTenant {
			sp.EndKey = keys.MakeTenantSpan(r.config.tenantID).EndKey
		}
		// Emit a CheckpointEvent.
		resolvedTime := timeutil.Now()
		hlcResolvedTime := hlc.Timestamp{WallTime: resolvedTime.UnixNano()}
		resolvedSpan := jobspb.ResolvedSpan{Span: sp, Timestamp: hlcResolvedTime}
		event = streamingccl.MakeCheckpointEvent([]jobspb.ResolvedSpan{resolvedSpan})
		r.numEventsSinceLastResolved = 0
	} else {
		// If there are system KVs to emit, prioritize those.
		if len(r.systemKVs) > 0 {
			systemKV := r.systemKVs[0]
			systemKV.Value.Timestamp = hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
			event = streamingccl.MakeKVEvent([]roachpb.KeyValue{systemKV})
			r.systemKVs = r.systemKVs[1:]
			return event
		}

		// Emit SST with given probability.
		// TODO(casper): add support for DelRange.
		if prob := r.rng.Float64(); prob < r.config.sstProbability {
			size := 10 + r.rng.Intn(30)
			keyVals := make([]roachpb.KeyValue, 0, size)
			for i := 0; i < size; i++ {
				keyVals = append(keyVals, makeRandomKey(r.rng, r.config, r.codec, r.tableDesc))
			}
			event = streamingccl.MakeSSTableEvent(r.sstMaker(keyVals))
		} else {
			event = streamingccl.MakeKVEvent([]roachpb.KeyValue{makeRandomKey(r.rng, r.config, r.codec, r.tableDesc)})
		}
		r.numEventsSinceLastResolved++
	}
	return event
}

// RandomStreamClient is a temporary stream client implementation that generates
// random events.
//
// The client can be configured to return more than one partition via the stream
// URL. Each partition covers a single table span.
type RandomStreamClient struct {
	config    randomStreamConfig
	streamURL *url.URL

	// mu is used to provide a threadsafe interface to interceptors.
	mu struct {
		syncutil.Mutex

		// interceptors can be registered to peek at every event generated by this
		// client and which partition spec it was sent to.
		interceptors          []InterceptFn
		dialInterceptors      []DialInterceptFn
		heartbeatInterceptors []HeartbeatInterceptFn
		sstMaker              SSTableMakerFn
		tableID               int
	}
}

var _ Client = &RandomStreamClient{}

// newRandomStreamClient returns a stream client that generates a random set of
// events on a table with an integer key and integer value for the table with
// the given ID.
func newRandomStreamClient(streamURL *url.URL) (Client, error) {
	c := randomStreamClientSingleton

	streamConfig, err := parseRandomStreamConfig(streamURL)
	if err != nil {
		return nil, err
	}
	c.config = streamConfig
	c.streamURL = streamURL
	return c, nil
}

func (m *RandomStreamClient) getNextTableID() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	ret := m.mu.tableID
	m.mu.tableID++
	return ret
}

func (m *RandomStreamClient) tableDescForID(tableID int) (*tabledesc.Mutable, error) {
	partitionURI := m.config.URL(tableID, false, false)
	partitionURL, err := url.Parse(partitionURI)
	if err != nil {
		return nil, err
	}
	config, err := parseRandomStreamConfig(partitionURL)
	if err != nil {
		return nil, err
	}
	var partitionTableID int
	partitionTableID, err = strconv.Atoi(partitionURL.Host)
	if err != nil {
		return nil, err
	}
	tableDesc, _, err := getDescriptorAndNamespaceKVForTableID(config, descpb.ID(partitionTableID))
	return tableDesc, err
}

// Dial implements Client interface.
func (m *RandomStreamClient) Dial(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, interceptor := range m.mu.dialInterceptors {
		if interceptor == nil {
			continue
		}
		if err := interceptor(m.streamURL); err != nil {
			return err
		}
	}

	return nil
}

// Plan implements the Client interface.
func (m *RandomStreamClient) Plan(ctx context.Context, _ streampb.StreamID) (Topology, error) {
	topology := Topology{
		Partitions:     make([]PartitionInfo, 0, m.config.numPartitions),
		SourceTenantID: m.config.tenantID,
	}
	tenantSpan := keys.MakeTenantSpan(m.config.tenantID)
	log.Infof(ctx, "planning random stream for tenant %d", m.config.tenantID)

	// Allocate table IDs and return one per partition address in the topology.
	srcCodec := keys.MakeSQLCodec(m.config.tenantID)
	for i := 0; i < m.config.numPartitions; i++ {
		tableID := m.getNextTableID()
		tableDesc, err := m.tableDescForID(tableID)
		if err != nil {
			return Topology{}, err
		}

		partitionURI := m.config.URL(tableID, i == 0, i == m.config.numPartitions-1)
		log.Infof(ctx, "planning random stream partition %d for tenant %d: %q", i, m.config.tenantID, partitionURI)

		topology.Partitions = append(topology.Partitions,
			PartitionInfo{
				ID:                strconv.Itoa(i),
				SrcAddr:           streamingccl.PartitionAddress(partitionURI),
				SubscriptionToken: []byte(partitionURI),
				Spans:             []roachpb.Span{tableDesc.TableSpan(srcCodec)},
			})
	}
	topology.Partitions[0].Spans[0].Key = tenantSpan.Key
	topology.Partitions[m.config.numPartitions-1].Spans[0].EndKey = tenantSpan.EndKey

	return topology, nil
}

// Create implements the Client interface.
func (m *RandomStreamClient) Create(
	ctx context.Context, tenantName roachpb.TenantName, _ streampb.ReplicationProducerRequest,
) (streampb.ReplicationProducerSpec, error) {
	log.Infof(ctx, "creating random stream for tenant %s", tenantName)
	return streampb.ReplicationProducerSpec{
		StreamID:             streampb.StreamID(1),
		ReplicationStartTime: hlc.Timestamp{WallTime: timeutil.Now().UnixNano()},
	}, nil
}

// Heartbeat implements the Client interface.
func (m *RandomStreamClient) Heartbeat(
	ctx context.Context, _ streampb.StreamID, ts hlc.Timestamp,
) (streampb.StreamReplicationStatus, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, interceptor := range m.mu.heartbeatInterceptors {
		if interceptor != nil {
			interceptor(ts)
		}
	}

	return streampb.StreamReplicationStatus{}, nil
}

// getDescriptorAndNamespaceKVForTableID returns the namespace and descriptor
// KVs for the table with tableID.
func getDescriptorAndNamespaceKVForTableID(
	config randomStreamConfig, tableID descpb.ID,
) (*tabledesc.Mutable, []roachpb.KeyValue, error) {
	tableName := fmt.Sprintf("%s%d", IngestionTablePrefix, tableID)
	testTable, err := sql.CreateTestTableDescriptor(
		context.Background(),
		IngestionDatabaseID,
		tableID,
		fmt.Sprintf(RandomStreamSchemaPlaceholder, tableName),
		catpb.NewBasePrivilegeDescriptor(username.RootUserName()),
		nil, /* txn */
		nil, /* collection */
	)
	if err != nil {
		return nil, nil, err
	}

	// Generate namespace entry.
	codec := keys.MakeSQLCodec(config.tenantID)
	k := rekey(config.tenantID, catalogkeys.EncodeNameKey(codec, testTable))
	var value roachpb.Value
	value.SetInt(int64(testTable.GetID()))
	value.InitChecksum(k)
	namespaceKV := roachpb.KeyValue{
		Key:   k,
		Value: value,
	}

	// Generate descriptor entry.
	descKey := catalogkeys.MakeDescMetadataKey(codec, testTable.GetID())
	descKey = rekey(config.tenantID, descKey)
	descDesc := testTable.DescriptorProto()
	var descValue roachpb.Value
	if err := descValue.SetProto(descDesc); err != nil {
		panic(err)
	}
	descValue.InitChecksum(descKey)
	descKV := roachpb.KeyValue{
		Key:   descKey,
		Value: descValue,
	}

	return testTable, []roachpb.KeyValue{namespaceKV, descKV}, nil
}

// Close implements the Client interface.
func (m *RandomStreamClient) Close(_ context.Context) error {
	return nil
}

// Subscribe implements the Client interface.
func (m *RandomStreamClient) Subscribe(
	_ context.Context,
	_ streampb.StreamID,
	_ int32,
	spec SubscriptionToken,
	initialScanTime hlc.Timestamp,
	_ span.Frontier,
) (Subscription, error) {
	partitionURL, err := url.Parse(string(spec))
	if err != nil {
		return nil, err
	}
	// add option for sst probability
	config, err := parseRandomStreamConfig(partitionURL)
	if err != nil {
		return nil, err
	}

	eventCh := make(chan streamingccl.Event)
	now := timeutil.Now()
	startWalltime := timeutil.Unix(0 /* sec */, initialScanTime.WallTime)
	if startWalltime.After(now) {
		panic("cannot start random stream client event stream in the future")
	}

	// rand is not thread safe, so create a random source for each partition.
	rng, _ := randutil.NewPseudoRand()
	reg, err := func() (*randomEventGenerator, error) {
		m.mu.Lock()
		defer m.mu.Unlock()
		return newRandomEventGenerator(rng, partitionURL, config, m.mu.sstMaker)
	}()
	if err != nil {
		return nil, err
	}

	receiveFn := func(ctx context.Context) error {
		defer close(eventCh)

		dataEventInterval := config.eventFrequency
		var lastEventCopy streamingccl.Event
		for {
			var event streamingccl.Event
			if lastEventCopy != nil && rng.Float64() < config.dupProbability {
				event = duplicateEvent(lastEventCopy)
			} else {
				event = reg.generateNewEvent()
			}
			lastEventCopy = duplicateEvent(event)

			select {
			// The event may get modified after sent to the channel.
			case eventCh <- event:
			case <-ctx.Done():
				return ctx.Err()
			}

			func() {
				m.mu.Lock()
				defer m.mu.Unlock()

				if len(m.mu.interceptors) > 0 {
					for _, interceptor := range m.mu.interceptors {
						if interceptor != nil {
							interceptor(duplicateEvent(lastEventCopy), spec)
						}
					}
				}
			}()

			time.Sleep(dataEventInterval)
		}
	}

	return &randomStreamSubscription{
		receiveFn: receiveFn,
		eventCh:   eventCh,
	}, nil
}

// Complete implements the streamclient.Client interface.
func (m *RandomStreamClient) Complete(_ context.Context, _ streampb.StreamID, _ bool) error {
	return nil
}

// PriorReplicationDetails implements the streamclient.Client interface.
func (p *RandomStreamClient) PriorReplicationDetails(
	ctx context.Context, tenant roachpb.TenantName,
) (string, string, hlc.Timestamp, error) {
	return "", "", hlc.Timestamp{}, nil

}

type randomStreamSubscription struct {
	receiveFn func(ctx context.Context) error
	eventCh   chan streamingccl.Event
}

// Subscribe implements the Subscription interface.
func (r *randomStreamSubscription) Subscribe(ctx context.Context) error {
	return r.receiveFn(ctx)
}

// Events implements the Subscription interface.
func (r *randomStreamSubscription) Events() <-chan streamingccl.Event {
	return r.eventCh
}

// Err implements the Subscription interface.
func (r *randomStreamSubscription) Err() error {
	return nil
}

func rekey(tenantID roachpb.TenantID, k roachpb.Key) roachpb.Key {
	// Strip old prefix.
	tenantPrefix := keys.MakeTenantPrefix(tenantID)
	noTenantPrefix, _, err := keys.DecodeTenantPrefix(k)
	if err != nil {
		panic(err)
	}

	// Prepend tenant prefix.
	rekeyedKey := append(tenantPrefix, noTenantPrefix...)
	return rekeyedKey
}

func makeRandomKey(
	r *rand.Rand, config randomStreamConfig, codec keys.SQLCodec, tableDesc *tabledesc.Mutable,
) roachpb.KeyValue {
	// Create a key holding a random integer.
	keyDatum := tree.NewDInt(tree.DInt(r.Intn(config.valueRange)))

	index := tableDesc.GetPrimaryIndex()
	// Create the ColumnID to index in datums slice map needed by
	// MakeIndexKeyPrefix.
	var colIDToRowIndex catalog.TableColMap
	colIDToRowIndex.Set(index.GetKeyColumnID(0), 0)

	keyPrefix := rowenc.MakeIndexKeyPrefix(codec, tableDesc.GetID(), index.GetID())
	k, _, err := rowenc.EncodeIndexKey(tableDesc, index, colIDToRowIndex, tree.Datums{keyDatum}, keyPrefix)
	if err != nil {
		panic(err)
	}
	k = keys.MakeFamilyKey(k, uint32(tableDesc.Families[0].ID))

	k = rekey(config.tenantID, k)

	// Create a value holding a random integer.
	valueDatum := tree.NewDInt(tree.DInt(r.Intn(config.valueRange)))
	valueBuf, err := valueside.Encode(
		[]byte(nil), valueside.MakeColumnIDDelta(0, tableDesc.Columns[1].ID), valueDatum, []byte(nil))
	if err != nil {
		panic(err)
	}
	var v roachpb.Value
	v.SetTuple(valueBuf)
	v.ClearChecksum()
	v.InitChecksum(k)

	v.Timestamp = hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}

	return roachpb.KeyValue{
		Key:   k,
		Value: v,
	}
}

func duplicateEvent(event streamingccl.Event) streamingccl.Event {
	var dup streamingccl.Event
	switch event.Type() {
	case streamingccl.CheckpointEvent:
		resolvedSpans := make([]jobspb.ResolvedSpan, len(event.GetResolvedSpans()))
		copy(resolvedSpans, event.GetResolvedSpans())
		dup = streamingccl.MakeCheckpointEvent(resolvedSpans)
	case streamingccl.KVEvent:
		kvs := event.GetKVs()
		res := make([]roachpb.KeyValue, len(kvs))
		var a bufalloc.ByteAllocator
		for i := range kvs {
			res[i].Key = kvs[i].Key.Clone()
			res[i].Value.Timestamp = kvs[i].Value.Timestamp
			a, res[i].Value.RawBytes = a.Copy(kvs[i].Value.RawBytes, 0)
		}
		dup = streamingccl.MakeKVEvent(res)
	case streamingccl.SSTableEvent:
		sst := event.GetSSTable()
		dataCopy := make([]byte, len(sst.Data))
		copy(dataCopy, sst.Data)
		dup = streamingccl.MakeSSTableEvent(kvpb.RangeFeedSSTable{
			Data:    dataCopy,
			Span:    sst.Span.Clone(),
			WriteTS: sst.WriteTS,
		})
	default:
		panic("unsopported event type")
	}
	return dup
}

// RegisterInterception registers a interceptor to be called after
// an event is emitted from the client.
func (m *RandomStreamClient) RegisterInterception(fn InterceptFn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.interceptors = append(m.mu.interceptors, fn)
}

// RegisterDialInterception registers a interceptor to be called
// whenever Dial is called on the client.
func (m *RandomStreamClient) RegisterDialInterception(fn DialInterceptFn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.dialInterceptors = append(m.mu.dialInterceptors, fn)
}

// RegisterHeartbeatInterception registers an interceptor to be called
// whenever Heartbeat is called on the client.
func (m *RandomStreamClient) RegisterHeartbeatInterception(fn HeartbeatInterceptFn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.heartbeatInterceptors = append(m.mu.heartbeatInterceptors, fn)
}

// RegisterSSTableGenerator registers a functor to be called
// whenever an SSTable event is to be generated.
func (m *RandomStreamClient) RegisterSSTableGenerator(fn SSTableMakerFn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.sstMaker = fn
}

// ClearInterceptors clears all registered interceptors on the client.
func (m *RandomStreamClient) ClearInterceptors() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.interceptors = m.mu.interceptors[:0]
	m.mu.heartbeatInterceptors = m.mu.heartbeatInterceptors[:0]
	m.mu.dialInterceptors = m.mu.dialInterceptors[:0]
	m.mu.sstMaker = nil
}
