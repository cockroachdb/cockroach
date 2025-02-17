// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"hash"
	"hash/fnv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/unique"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
)

const (
	sqlSinkCreateTableStmt = `CREATE TABLE IF NOT EXISTS "%s" (
		topic STRING,
		partition INT,
		message_id INT,
		key BYTES, value BYTES,
		resolved BYTES,
		PRIMARY KEY (topic, partition, message_id)
	)`
	sqlSinkEmitStmt = `INSERT INTO "%s" (topic, partition, message_id, key, value, resolved)`
	sqlSinkEmitCols = 6
	// Some amount of batching to mirror a bit how kafkaSink works.
	sqlSinkRowBatchSize = 3
	// While sqlSink is only used for testing, hardcode the number of
	// partitions to something small but greater than 1.
	sqlSinkNumPartitions = 3
)

// sqlSink mirrors the semantics offered by kafkaSink as closely as possible,
// but writes to a SQL table (presumably in CockroachDB). Currently only for
// testing.
//
// Each emitted row or resolved timestamp is stored as a row in the table. Each
// table gets 3 partitions. Similar to kafkaSink, the order between two emits is
// only preserved if they are emitted to by the same node and to the same
// partition.
type sqlSink struct {
	db *gosql.DB

	uri        string
	tableName  string
	topicNamer *TopicNamer
	hasher     hash.Hash32

	rowBuf  []interface{}
	scratch bufalloc.ByteAllocator

	metrics metricsRecorder
}

func (s *sqlSink) getConcreteType() sinkType {
	return sinkTypeSQL
}

// TODO(dan): Make tableName configurable or based on the job ID or
// something.
const sqlSinkTableName = `sqlsink`

func makeSQLSink(
	u *changefeedbase.SinkURL,
	tableName string,
	targets changefeedbase.Targets,
	mb metricsRecorderBuilder,
) (Sink, error) {
	// Swap the changefeed prefix for the sql connection one that sqlSink
	// expects.
	u.Scheme = `postgres`

	if u.Path == `` {
		return nil, errors.Errorf(`must specify database`)
	}

	topicNamer, err := MakeTopicNamer(targets)
	if err != nil {
		return nil, err
	}

	uri := u.String()
	u.ConsumeParam(`sslcert`)
	u.ConsumeParam(`sslkey`)
	u.ConsumeParam(`sslmode`)
	u.ConsumeParam(`sslrootcert`)

	if unknownParams := u.RemainingQueryParams(); len(unknownParams) > 0 {
		return nil, errors.Errorf(
			`unknown SQL sink query parameters: %s`, strings.Join(unknownParams, ", "))
	}

	return &sqlSink{
		uri:        uri,
		tableName:  tableName,
		topicNamer: topicNamer,
		hasher:     fnv.New32a(),
		metrics:    mb(noResourceAccounting),
	}, nil
}

func (s *sqlSink) Dial() error {
	connector, err := pq.NewConnector(s.uri)
	if err != nil {
		return err
	}

	s.metrics.netMetrics().WrapPqDialer(connector, "sql")
	db := gosql.OpenDB(connector)
	if _, err := db.Exec(fmt.Sprintf(sqlSinkCreateTableStmt, s.tableName)); err != nil {
		db.Close()
		return err
	}
	s.db = db
	return nil
}

// EmitRow implements the Sink interface.
func (s *sqlSink) EmitRow(
	ctx context.Context,
	topicDescr TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	defer alloc.Release(ctx)
	defer s.metrics.recordOneMessage()(mvcc, len(key)+len(value), sinkDoesNotCompress)

	topic, err := s.topicNamer.Name(topicDescr)
	if err != nil {
		return err
	}

	// Hashing logic copied from sarama.HashPartitioner.
	s.hasher.Reset()
	if _, err := s.hasher.Write(key); err != nil {
		return err
	}
	partition := int32(s.hasher.Sum32()) % sqlSinkNumPartitions
	if partition < 0 {
		partition = -partition
	}

	var noResolved []byte
	return s.emit(ctx, topic, partition, key, value, noResolved)
}

// EmitResolvedTimestamp implements the Sink interface.
func (s *sqlSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	defer s.metrics.recordResolvedCallback()()

	var noKey, noValue []byte
	return s.topicNamer.Each(func(topic string) error {
		payload, err := encoder.EncodeResolvedTimestamp(ctx, topic, resolved)
		if err != nil {
			return err
		}
		s.scratch, payload = s.scratch.Copy(payload, 0 /* extraCap */)
		for partition := int32(0); partition < sqlSinkNumPartitions; partition++ {
			if err := s.emit(ctx, topic, partition, noKey, noValue, payload); err != nil {
				return err
			}
		}
		return nil
	})
}

// Topics gives the names of all topics that have been initialized
// and will receive resolved timestamps.
func (s *sqlSink) Topics() []string {
	return s.topicNamer.DisplayNamesSlice()
}

func (s *sqlSink) emit(
	ctx context.Context, topic string, partition int32, key, value, resolved []byte,
) error {
	// Generate the message id on the client to match the guaranttees of kafka
	// (two messages are only guaranteed to keep their order if emitted from the
	// same producer to the same partition).
	messageID := unique.GenerateUniqueInt(unique.ProcessUniqueID(partition))
	s.rowBuf = append(s.rowBuf, topic, partition, messageID, key, value, resolved)
	if len(s.rowBuf)/sqlSinkEmitCols >= sqlSinkRowBatchSize {
		return s.Flush(ctx)
	}
	return nil
}

// Flush implements the Sink interface.
func (s *sqlSink) Flush(ctx context.Context) error {
	defer s.metrics.recordFlushRequestCallback()()

	if len(s.rowBuf) == 0 {
		return nil
	}

	var stmt strings.Builder
	fmt.Fprintf(&stmt, sqlSinkEmitStmt, s.tableName)
	for i := 0; i < len(s.rowBuf); i++ {
		if i == 0 {
			stmt.WriteString(` VALUES (`)
		} else if i%sqlSinkEmitCols == 0 {
			stmt.WriteString(`),(`)
		} else {
			stmt.WriteString(`,`)
		}
		fmt.Fprintf(&stmt, `$%d`, i+1)
	}
	stmt.WriteString(`)`)
	_, err := s.db.Exec(stmt.String(), s.rowBuf...)
	if err != nil {
		return err
	}
	s.rowBuf = s.rowBuf[:0]
	return nil
}

// Close implements the Sink interface.
func (s *sqlSink) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}
