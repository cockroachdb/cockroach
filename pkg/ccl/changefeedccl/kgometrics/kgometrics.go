// WIP!
// TODO: something like this, if we want. would have to integrate with existing stuff though.

package kgometrics

import (
	"net"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/metric/aggmetric"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	metaConnects = metric.Metadata{
		Name:        "changefeed.kafka.connects",
		Help:        "Number of connections made to Kafka brokers",
		Measurement: "Connections",
		Unit:        metric.Unit_COUNT,
	}
	metaConnectErrors = metric.Metadata{
		Name:        "changefeed.kafka.connect_errors",
		Help:        "Number of errors encountered while connecting to Kafka brokers",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}
	metaWriteErrors = metric.Metadata{
		Name:        "changefeed.kafka.write_errors",
		Help:        "Number of errors encountered while writing to Kafka brokers",
		Measurement: "Errors",
		Unit:        metric.Unit_COUNT,
	}
	metaWriteBytes = metric.Metadata{
		Name:        "changefeed.kafka.write_bytes",
		Help:        "Number of bytes written to Kafka brokers",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaProduceBytes = metric.Metadata{
		Name:        "changefeed.kafka.produce_bytes",
		Help:        "Number of bytes produced to Kafka brokers",
		Measurement: "Bytes",
		Unit:        metric.Unit_BYTES,
	}
	metaBufferedProduceRecords = metric.Metadata{
		Name:        "changefeed.kafka.buffered_produce_records",
		Help:        "Number of records buffered for producing to Kafka brokers",
		Measurement: "Records",
		Unit:        metric.Unit_COUNT,
	}
)

type KgoMetrics struct {
	Connects               *aggmetric.AggCounter
	ConnectErrors          *aggmetric.AggCounter
	WriteErrors            *aggmetric.AggCounter
	WriteBytes             *aggmetric.AggCounter
	ProduceBytes           *aggmetric.AggCounter
	BufferedProduceRecords *aggmetric.AggCounter

	// There is always at least 1 scopedMetrics created for default scope.
	mu struct {
		syncutil.Mutex
		scopedMetrics map[string]*ScopedKgoMetrics
	}
}

func (k *KgoMetrics) MetricStruct() {}

var _ metric.Struct = &KgoMetrics{}

func NewKgoMetrics() *KgoMetrics {
	b := aggmetric.MakeBuilder("scope")
	return &KgoMetrics{
		Connects:               b.Counter(metaConnects),
		ConnectErrors:          b.Counter(metaConnectErrors),
		WriteErrors:            b.Counter(metaWriteErrors),
		WriteBytes:             b.Counter(metaWriteBytes),
		ProduceBytes:           b.Counter(metaProduceBytes),
		BufferedProduceRecords: b.Counter(metaBufferedProduceRecords),
	}
}

// constants copied from ../metrics.go. TODO: move them to a shared location or something
// max length for the scope name.
const maxSLIScopeNameLen = 128

// defaultSLIScope is the name of the default SLI scope -- i.e. the set of metrics
// keeping track of all changefeeds which did not have explicit sli scope specified.
const defaultSLIScope = "default"

func (k *KgoMetrics) GetOrCreateScope(scope string) (*ScopedKgoMetrics, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	scope = strings.TrimSpace(strings.ToLower(scope))

	if scope == "" {
		scope = defaultSLIScope
	}
	if len(scope) > maxSLIScopeNameLen {
		return nil, pgerror.Newf(pgcode.ConfigurationLimitExceeded,
			"scope name length must be less than %d bytes", maxSLIScopeNameLen)
	}

	if k.mu.scopedMetrics == nil {
		k.mu.scopedMetrics = make(map[string]*ScopedKgoMetrics)
	}
	if m, ok := k.mu.scopedMetrics[scope]; ok {
		return m, nil
	}

	if scope != defaultSLIScope {
		const failSafeMax = 1024
		if len(k.mu.scopedMetrics) == failSafeMax {
			return nil, pgerror.Newf(pgcode.ConfigurationLimitExceeded,
				"too many metrics labels; max %d", failSafeMax)
		}
	}

	sm := &ScopedKgoMetrics{
		Connects:               k.Connects.AddChild(scope),
		ConnectErrors:          k.ConnectErrors.AddChild(scope),
		WriteErrors:            k.WriteErrors.AddChild(scope),
		WriteBytes:             k.WriteBytes.AddChild(scope),
		ProduceBytes:           k.ProduceBytes.AddChild(scope),
		BufferedProduceRecords: k.BufferedProduceRecords.AddChild(scope),
	}
	k.mu.scopedMetrics[scope] = sm
	return sm, nil
}

type ScopedKgoMetrics struct {
	Connects               *aggmetric.Counter
	ConnectErrors          *aggmetric.Counter
	WriteErrors            *aggmetric.Counter
	WriteBytes             *aggmetric.Counter
	ProduceBytes           *aggmetric.Counter
	BufferedProduceRecords *aggmetric.Counter
}

// TODO: finish this and prune the hooks we dont need

func (s *ScopedKgoMetrics) OnProduceRecordPartitioned(*kgo.Record, int32)      {}
func (s *ScopedKgoMetrics) OnProduceRecordBuffered(*kgo.Record)                {}
func (s *ScopedKgoMetrics) OnGroupManageError(error)                           {}
func (s *ScopedKgoMetrics) OnFetchRecordUnbuffered(r *kgo.Record, polled bool) {}
func (s *ScopedKgoMetrics) OnFetchRecordBuffered(*kgo.Record)                  {}
func (s *ScopedKgoMetrics) OnProduceBatchWritten(meta kgo.BrokerMetadata, topic string, partition int32, metrics kgo.ProduceBatchMetrics) {
}
func (s *ScopedKgoMetrics) OnNewClient(*kgo.Client) {}
func (s *ScopedKgoMetrics) OnFetchBatchRead(meta kgo.BrokerMetadata, topic string, partition int32, metrics kgo.FetchBatchMetrics) {
}
func (s *ScopedKgoMetrics) OnClientClosed(*kgo.Client) {}
func (s *ScopedKgoMetrics) OnBrokerWrite(meta kgo.BrokerMetadata, key int16, bytesWritten int, writeWait time.Duration, timeToWrite time.Duration, err error) {
	s.WriteBytes.Inc(int64(bytesWritten))
	if err != nil {
		s.WriteErrors.Inc(1)
	}
}
func (s *ScopedKgoMetrics) OnBrokerThrottle(meta kgo.BrokerMetadata, throttleInterval time.Duration, throttledAfterResponse bool) {
}
func (s *ScopedKgoMetrics) OnBrokerRead(meta kgo.BrokerMetadata, key int16, bytesRead int, readWait time.Duration, timeToRead time.Duration, err error) {
}
func (s *ScopedKgoMetrics) OnBrokerDisconnect(meta kgo.BrokerMetadata, conn net.Conn)         {}
func (s *ScopedKgoMetrics) OnBrokerE2E(meta kgo.BrokerMetadata, key int16, e2e kgo.BrokerE2E) {}
func (s *ScopedKgoMetrics) OnBrokerConnect(meta kgo.BrokerMetadata, dialDur time.Duration, conn net.Conn, err error) {
	s.Connects.Inc(1)
	if err != nil {
		s.ConnectErrors.Inc(1)
	}
}

var (
	_ kgo.HookBrokerConnect            = (*ScopedKgoMetrics)(nil)
	_ kgo.HookBrokerDisconnect         = (*ScopedKgoMetrics)(nil)
	_ kgo.HookBrokerE2E                = (*ScopedKgoMetrics)(nil)
	_ kgo.HookBrokerRead               = (*ScopedKgoMetrics)(nil)
	_ kgo.HookBrokerThrottle           = (*ScopedKgoMetrics)(nil)
	_ kgo.HookBrokerWrite              = (*ScopedKgoMetrics)(nil)
	_ kgo.HookClientClosed             = (*ScopedKgoMetrics)(nil)
	_ kgo.HookFetchBatchRead           = (*ScopedKgoMetrics)(nil)
	_ kgo.HookFetchRecordBuffered      = (*ScopedKgoMetrics)(nil)
	_ kgo.HookFetchRecordUnbuffered    = (*ScopedKgoMetrics)(nil)
	_ kgo.HookGroupManageError         = (*ScopedKgoMetrics)(nil)
	_ kgo.HookNewClient                = (*ScopedKgoMetrics)(nil)
	_ kgo.HookProduceBatchWritten      = (*ScopedKgoMetrics)(nil)
	_ kgo.HookProduceRecordBuffered    = (*ScopedKgoMetrics)(nil)
	_ kgo.HookProduceRecordPartitioned = (*ScopedKgoMetrics)(nil)
)
