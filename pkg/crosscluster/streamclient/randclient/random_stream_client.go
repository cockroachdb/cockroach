// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package randclient

import (
	"context"
	"fmt"
	"math/rand"
	"net/url"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/externalcatalog/externalpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	// RandomStreamSchemaPlaceholder is the schema of the KVs emitted by the
	// random stream client.
	RandomStreamSchemaPlaceholder = "CREATE TABLE %s (k INT PRIMARY KEY, v INT)"

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
	SSTProbability  = "SST_PROBABILITY"
	ErrProbability  = "ERROR_PROBABILITY"
	ExistingTableID = "TABLE_ID"
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

func init() {
	streamclient.RandomGenClientBuilder = newRandomStreamClient
	streamclient.GetRandomStreamClientSingletonForTesting = func() streamclient.RandomClient {
		return randomStreamClientSingleton
	}
}

// TODO(dt): just make interceptors a singleton, not the whole client.
var randomStreamClientSingleton = func() *RandomStreamClient {
	c := RandomStreamClient{}
	// Make the base tableID really large to prevent colliding with system table IDs.
	c.mu.tableID = 5000
	return &c
}()

// randomStreamConfig specifies the variables that controls the rate and type of
// events that the generated stream emits.
type randomStreamConfig struct {
	eventFrequency      time.Duration
	eventsPerCheckpoint int
	numPartitions       int
	dupProbability      float64
	errorProbability    float64
	sstProbability      float64

	existingTableID catid.DescID
	tenantID        roachpb.TenantID
	// startTenant and endTenant extend checkpoint spans to the tenant span.
	startTenant, endTenant bool
}

type randomEvent int

const (
	randomKV randomEvent = iota
	randomSST
	randomError
)

func (c randomStreamConfig) randomEvent(rng *rand.Rand) (randomEvent, error) {
	if c.sstProbability+c.errorProbability > 1 {
		return 0, errors.AssertionFailedf("misconfigured random stream")
	}

	prob := rng.Float64()
	if c.sstProbability > 0 && prob < c.sstProbability {
		return randomSST, nil
	} else if c.errorProbability > 0 && prob < (c.sstProbability+c.errorProbability) {
		return randomError, nil
	} else {
		return randomKV, nil
	}
}

func parseRandomStreamConfig(streamURL url.URL) (randomStreamConfig, error) {
	c := randomStreamConfig{
		eventFrequency:      10 * time.Microsecond,
		eventsPerCheckpoint: 30,
		numPartitions:       1, // TODO(casper): increases this
		dupProbability:      0.3,
		sstProbability:      0.2,
		tenantID:            roachpb.SystemTenantID,
	}

	var err error
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

	if errProbabilityStr := streamURL.Query().Get(ErrProbability); errProbabilityStr != "" {
		c.errorProbability, err = strconv.ParseFloat(errProbabilityStr, 32)
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

	if tableIDStr := streamURL.Query().Get(ExistingTableID); tableIDStr != "" {
		id, err := strconv.Atoi(tableIDStr)
		if err != nil {
			return c, err
		}
		c.existingTableID = catid.DescID(id)
	}

	return c, nil
}

func (c randomStreamConfig) URL(
	table int, startsTenant, finishesTenant bool,
) streamclient.ClusterUri {
	u := url.URL{
		Scheme: streamclient.RandomGenScheme,
		Host:   strconv.Itoa(table),
	}
	q := u.Query()
	q.Add(EventFrequency, strconv.Itoa(int(c.eventFrequency)))
	q.Add(EventsPerCheckpoint, strconv.Itoa(c.eventsPerCheckpoint))
	q.Add(NumPartitions, strconv.Itoa(c.numPartitions))
	q.Add(ExistingTableID, strconv.Itoa(int(c.existingTableID)))
	q.Add(DupProbability, fmt.Sprintf("%f", c.dupProbability))
	q.Add(SSTProbability, fmt.Sprintf("%f", c.sstProbability))
	q.Add(ErrProbability, fmt.Sprintf("%f", c.errorProbability))
	q.Add(TenantSpanStart, strconv.FormatBool(startsTenant))
	q.Add(TenantSpanEnd, strconv.FormatBool(finishesTenant))
	q.Add(TenantID, strconv.Itoa(int(c.tenantID.ToUint64())))
	u.RawQuery = q.Encode()
	return streamclient.MakeTestClusterUri(u)
}

type randomEventGenerator struct {
	rng                        *rand.Rand
	config                     randomStreamConfig
	numEventsSinceLastResolved int
	sstMaker                   streamclient.SSTableMakerFn
	codec                      keys.SQLCodec
	tableDesc                  catalog.TableDescriptor
	systemKVs                  []roachpb.KeyValue
}

func newRandomEventGenerator(
	rng *rand.Rand,
	partitionURL *url.URL,
	config randomStreamConfig,
	fn streamclient.SSTableMakerFn,
	tableDesc catalog.TableDescriptor,
) (*randomEventGenerator, error) {
	var partitionTableID int
	partitionTableID, err := strconv.Atoi(partitionURL.Host)
	if err != nil {
		return nil, err
	}

	var systemKVs []roachpb.KeyValue
	if tableDesc == nil {
		tableDesc, systemKVs, err = getDescriptorAndNamespaceKVForTableID(config, descpb.ID(partitionTableID))
		if err != nil {
			return nil, err
		}
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

func (r *randomEventGenerator) generateNewEvent() (crosscluster.Event, error) {
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
		r.numEventsSinceLastResolved = 0
		checkpoint := &streampb.StreamEvent_StreamCheckpoint{
			ResolvedSpans: []jobspb.ResolvedSpan{resolvedSpan},
		}
		return crosscluster.MakeCheckpointEvent(checkpoint), nil
	}

	// If there are system KVs to emit, prioritize those.
	if len(r.systemKVs) > 0 {
		systemKV := r.systemKVs[0]
		systemKV.Value.Timestamp = hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
		event := crosscluster.MakeKVEventFromKVs([]roachpb.KeyValue{systemKV})
		r.systemKVs = r.systemKVs[1:]
		return event, nil
	}

	eventType, err := r.config.randomEvent(r.rng)
	if err != nil {
		return nil, err
	}
	switch eventType {
	case randomKV:
		r.numEventsSinceLastResolved++
		return crosscluster.MakeKVEventFromKVs([]roachpb.KeyValue{makeRandomKey(r.rng, r.config, r.codec, r.tableDesc)}), nil
	case randomSST:
		size := 10 + r.rng.Intn(30)
		keyVals := make([]roachpb.KeyValue, 0, size)
		for i := 0; i < size; i++ {
			keyVals = append(keyVals, makeRandomKey(r.rng, r.config, r.codec, r.tableDesc))
		}
		return crosscluster.MakeSSTableEvent(r.sstMaker(keyVals)), nil
	case randomError:
		return nil, errors.Errorf("randomly ingested event")
	default:
		return nil, errors.AssertionFailedf("unknown event type: %d", eventType)
	}
}

// RandomStreamClient is a temporary stream client implementation that generates
// random events.
//
// The client can be configured to return more than one partition via the stream
// URL. Each partition covers a single table span.
type RandomStreamClient struct {
	config    randomStreamConfig
	tableDesc catalog.TableDescriptor
	streamURL streamclient.ClusterUri

	// mu is used to provide a threadsafe interface to interceptors.
	mu struct {
		syncutil.Mutex

		// interceptors can be registered to peek at every event generated by this
		// client and which partition spec it was sent to.
		interceptors          []streamclient.InterceptFn
		dialInterceptors      []streamclient.DialInterceptFn
		heartbeatInterceptors []streamclient.HeartbeatInterceptFn
		sstMaker              streamclient.SSTableMakerFn
		tableID               int
	}
}

var _ streamclient.Client = &RandomStreamClient{}

// newRandomStreamClient returns a stream client that generates a random set of
// events on a table with an integer key and integer value for the table with
// the given ID.
func newRandomStreamClient(
	streamURL streamclient.ClusterUri, db descs.DB,
) (streamclient.Client, error) {
	c := randomStreamClientSingleton
	streamConfig, err := parseRandomStreamConfig(streamURL.URL())
	if err != nil {
		return nil, err
	}
	c.config = streamConfig
	c.streamURL = streamURL
	if c.config.existingTableID > 0 {
		if err := db.DescsTxn(context.Background(), func(ctx context.Context, txn descs.Txn) error {
			c.tableDesc, err = txn.Descriptors().ByIDWithoutLeased(txn.KV()).Get().Table(ctx, c.config.existingTableID)
			return err
		}); err != nil {
			return nil, err
		}
	}
	if err := c.dial(); err != nil {
		return nil, err
	}
	return c, nil
}

func (m *RandomStreamClient) getNextTableID() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	ret := m.mu.tableID
	m.mu.tableID++
	return ret
}

func (m *RandomStreamClient) tableDescForID(tableID int) (catalog.TableDescriptor, error) {
	if m.config.existingTableID != 0 && (m.config.existingTableID != catid.DescID(tableID)) {
		return nil, errors.AssertionFailedf("expected table ID %d requested", tableID)
	} else if m.config.existingTableID != 0 {
		return m.tableDesc, nil
	} else {
		tableDesc, _, err := getDescriptorAndNamespaceKVForTableID(m.config, descpb.ID(tableID))
		return tableDesc, err
	}
}

func (m *RandomStreamClient) dial() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, interceptor := range m.mu.dialInterceptors {
		if interceptor == nil {
			continue
		}
		if err := interceptor(m.streamURL.URL()); err != nil {
			return err
		}
	}

	return nil
}

// Plan implements the Client interface.
func (m *RandomStreamClient) PlanPhysicalReplication(
	ctx context.Context, _ streampb.StreamID,
) (streamclient.Topology, error) {
	topology := streamclient.Topology{
		Partitions:     make([]streamclient.PartitionInfo, 0, m.config.numPartitions),
		SourceTenantID: m.config.tenantID,
	}
	tenantSpan := keys.MakeTenantSpan(m.config.tenantID)
	log.Infof(ctx, "planning random stream for tenant %d", m.config.tenantID)

	// Allocate table IDs and return one per partition uri in the topology.
	srcCodec := keys.MakeSQLCodec(m.config.tenantID)
	for i := 0; i < m.config.numPartitions; i++ {
		tableID := m.getNextTableID()
		tableDesc, err := m.tableDescForID(tableID)
		if err != nil {
			return streamclient.Topology{}, err
		}

		partitionURI := m.config.URL(tableID, i == 0, i == m.config.numPartitions-1)
		log.Infof(ctx, "planning random stream partition %d for tenant %d: %q", i, m.config.tenantID, partitionURI.Serialize())

		topology.Partitions = append(topology.Partitions,
			streamclient.PartitionInfo{
				ID:                strconv.Itoa(i),
				ConnUri:           partitionURI,
				SubscriptionToken: []byte(partitionURI.Serialize()),
				Spans:             []roachpb.Span{tableDesc.TableSpan(srcCodec)},
			})
	}
	topology.Partitions[0].Spans[0].Key = tenantSpan.Key
	topology.Partitions[m.config.numPartitions-1].Spans[0].EndKey = tenantSpan.EndKey

	return topology, nil
}

// Create implements the Client interface.
func (m *RandomStreamClient) CreateForTenant(
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
	k := catalogkeys.EncodeNameKey(codec, testTable)
	var value roachpb.Value
	value.SetInt(int64(testTable.GetID()))
	value.InitChecksum(k)
	namespaceKV := roachpb.KeyValue{
		Key:   k,
		Value: value,
	}

	// Generate descriptor entry.
	descKey := catalogkeys.MakeDescMetadataKey(codec, testTable.GetID())
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
	_, _ int32,
	spec streamclient.SubscriptionToken,
	initialScanTime hlc.Timestamp,
	_ span.Frontier,
	_ ...streamclient.SubscribeOption,
) (streamclient.Subscription, error) {
	partitionURL, err := url.Parse(string(spec))
	if err != nil {
		return nil, err
	}
	// add option for sst probability
	config, err := parseRandomStreamConfig(*partitionURL)
	if err != nil {
		return nil, err
	}

	eventCh := make(chan crosscluster.Event)
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
		return newRandomEventGenerator(rng, partitionURL, config, m.mu.sstMaker, m.tableDesc)
	}()
	if err != nil {
		return nil, err
	}

	receiveFn := func(ctx context.Context) error {
		dataEventInterval := config.eventFrequency
		var lastEventCopy crosscluster.Event
		for {
			var event crosscluster.Event
			if lastEventCopy != nil && rng.Float64() < config.dupProbability {
				log.VInfof(ctx, 2, "sending duplicate event")
				event = duplicateEvent(lastEventCopy)
			} else {
				var err error
				event, err = reg.generateNewEvent()
				if err != nil {
					log.VInfof(ctx, 2, "sending error: %v", err)
					return err
				}
				log.VInfof(ctx, 2, "sending event: %v", event.Type())
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

func (p *RandomStreamClient) ExecStatement(
	ctx context.Context, cmd string, opname string, args ...interface{},
) error {
	return errors.AssertionFailedf("unimplemented")
}

type randomStreamSubscription struct {
	receiveFn func(ctx context.Context) error
	eventCh   chan crosscluster.Event
	err       error
}

// Subscribe implements the Subscription interface.
func (r *randomStreamSubscription) Subscribe(ctx context.Context) error {
	defer close(r.eventCh)
	r.err = r.receiveFn(ctx)
	return r.err
}

// Events implements the Subscription interface.
func (r *randomStreamSubscription) Events() <-chan crosscluster.Event {
	return r.eventCh
}

// Err implements the Subscription interface.
func (r *randomStreamSubscription) Err() error {
	return r.err
}

func makeRandomKey(
	r *rand.Rand, config randomStreamConfig, codec keys.SQLCodec, tableDesc catalog.TableDescriptor,
) roachpb.KeyValue {
	datums, err := randDatumsForTable(r, tableDesc)
	if err != nil {
		panic(err)
	}
	kv, err := encodeKV(codec, tableDesc, datums)
	if err != nil {
		panic(err)
	}

	kv.Value.Timestamp = hlc.Timestamp{WallTime: timeutil.Now().UnixNano()}
	return kv
}

func encodeKV(
	codec keys.SQLCodec, descr catalog.TableDescriptor, datums []tree.Datum,
) (roachpb.KeyValue, error) {
	primary := descr.GetPrimaryIndex()
	var colMap catalog.TableColMap
	for i := range datums {
		col, err := catalog.MustFindColumnByID(descr, descpb.ColumnID(i+1))
		if err != nil {
			return roachpb.KeyValue{}, err
		}
		colMap.Set(col.GetID(), col.Ordinal())
	}

	const includeEmpty = true
	indexEntries, err := rowenc.EncodePrimaryIndex(codec, descr, primary,
		colMap, datums, includeEmpty)
	if err != nil {
		return roachpb.KeyValue{}, err
	}
	for i := range indexEntries {
		indexEntries[i].Value.InitChecksum(indexEntries[i].Key)
	}
	return roachpb.KeyValue{Key: indexEntries[0].Key, Value: indexEntries[0].Value}, nil
}

func randDatumsForTable(rng *rand.Rand, td catalog.TableDescriptor) ([]tree.Datum, error) {
	if td.NumFamilies() != 1 {
		return nil, errors.AssertionFailedf("random client does not support multi-column-family tables")
	}

	columns := td.PublicColumns()
	datums := make([]tree.Datum, 0, len(columns))
	for _, c := range columns {
		if c.IsComputed() {
			return nil, errors.Errorf("unable to generate random datums for table with computed column %q", c.GetName())
		}
		datums = append(datums,
			randgen.RandDatum(rng, c.GetType(), c.IsNullable()))
	}
	return datums, nil
}

func duplicateEvent(event crosscluster.Event) crosscluster.Event {
	var dup crosscluster.Event
	switch event.Type() {
	case crosscluster.CheckpointEvent:
		checkpointClone := protoutil.Clone(event.GetCheckpoint()).(*streampb.StreamEvent_StreamCheckpoint)
		dup = crosscluster.MakeCheckpointEvent(checkpointClone)
	case crosscluster.KVEvent:
		kvs := event.GetKVs()
		res := make([]roachpb.KeyValue, len(kvs))
		var a bufalloc.ByteAllocator
		for i := range kvs {
			res[i].Key = kvs[i].KeyValue.Key.Clone()
			res[i].Value.Timestamp = kvs[i].KeyValue.Value.Timestamp
			a, res[i].Value.RawBytes = a.Copy(kvs[i].KeyValue.Value.RawBytes, 0)
		}
		dup = crosscluster.MakeKVEventFromKVs(res)
	case crosscluster.SSTableEvent:
		sst := event.GetSSTable()
		dataCopy := make([]byte, len(sst.Data))
		copy(dataCopy, sst.Data)
		dup = crosscluster.MakeSSTableEvent(kvpb.RangeFeedSSTable{
			Data:    dataCopy,
			Span:    sst.Span.Clone(),
			WriteTS: sst.WriteTS,
		})
	default:
		panic("unsupported event type")
	}
	return dup
}

// RegisterInterception registers a interceptor to be called after
// an event is emitted from the client.
func (m *RandomStreamClient) RegisterInterception(fn streamclient.InterceptFn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.interceptors = append(m.mu.interceptors, fn)
}

// RegisterDialInterception registers a interceptor to be called
// whenever Dial is called on the client.
func (m *RandomStreamClient) RegisterDialInterception(fn streamclient.DialInterceptFn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.dialInterceptors = append(m.mu.dialInterceptors, fn)
}

// RegisterHeartbeatInterception registers an interceptor to be called
// whenever Heartbeat is called on the client.
func (m *RandomStreamClient) RegisterHeartbeatInterception(fn streamclient.HeartbeatInterceptFn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.heartbeatInterceptors = append(m.mu.heartbeatInterceptors, fn)
}

// RegisterSSTableGenerator registers a functor to be called
// whenever an SSTable event is to be generated.
func (m *RandomStreamClient) RegisterSSTableGenerator(fn streamclient.SSTableMakerFn) {
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

func (m *RandomStreamClient) PlanLogicalReplication(
	ctx context.Context, req streampb.LogicalReplicationPlanRequest,
) (streamclient.LogicalReplicationPlan, error) {
	if m.config.numPartitions != 1 {
		return streamclient.LogicalReplicationPlan{}, errors.AssertionFailedf("random client only supports logical replication with 1 partition")
	}

	topology := streamclient.Topology{
		Partitions:     make([]streamclient.PartitionInfo, 0, m.config.numPartitions),
		SourceTenantID: m.config.tenantID,
	}

	srcCodec := keys.MakeSQLCodec(m.config.tenantID)
	tableSpan := m.tableDesc.TableSpan(srcCodec)
	partitionUri := m.config.URL(int(m.tableDesc.GetID()), false, false)
	topology.Partitions = append(topology.Partitions,
		streamclient.PartitionInfo{
			ID:                strconv.Itoa(1),
			ConnUri:           partitionUri,
			SubscriptionToken: []byte(partitionUri.Serialize()),
			Spans:             []roachpb.Span{m.tableDesc.TableSpan(srcCodec)},
		})
	return streamclient.LogicalReplicationPlan{
		Topology:    topology,
		SourceSpans: []roachpb.Span{tableSpan},
		DescriptorMap: map[int32]descpb.TableDescriptor{
			int32(m.tableDesc.GetID()): *m.tableDesc.TableDesc(),
		},
	}, nil
}

func (m *RandomStreamClient) CreateForTables(
	ctx context.Context, req *streampb.ReplicationProducerRequest,
) (*streampb.ReplicationProducerSpec, error) {
	if len(req.TableNames) > 1 {
		return nil, errors.AssertionFailedf("random stream only supports 1 table")
	}
	if m.tableDesc == nil {
		return nil, errors.AssertionFailedf("random stream not configured for logical streaming")
	}
	return &streampb.ReplicationProducerSpec{
		StreamID:             streampb.StreamID(1),
		ReplicationStartTime: hlc.Timestamp{WallTime: timeutil.Now().UnixNano()},
		ExternalCatalog: externalpb.ExternalCatalog{
			Tables: []descpb.TableDescriptor{*m.tableDesc.TableDesc()},
		},
	}, nil
}

func (m *RandomStreamClient) URL() streamclient.ClusterUri { return m.streamURL }
