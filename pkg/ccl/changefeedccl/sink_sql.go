// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	gosql "database/sql"
	"fmt"
	"hash"
	"hash/fnv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
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

	uri       string
	tableName string
	topics    map[string]struct{}
	hasher    hash.Hash32

	rowBuf  []interface{}
	scratch bufalloc.ByteAllocator

	targetNames map[descpb.ID]string
}

// TODO(dan): Make tableName configurable or based on the job ID or
// something.
const sqlSinkTableName = `sqlsink`

func makeSQLSink(u sinkURL, tableName string, targets jobspb.ChangefeedTargets) (Sink, error) {
	// Swap the changefeed prefix for the sql connection one that sqlSink
	// expects.
	u.Scheme = `postgres`

	if u.Path == `` {
		return nil, errors.Errorf(`must specify database`)
	}

	topics := make(map[string]struct{})
	targetNames := make(map[descpb.ID]string)
	for id, t := range targets {
		topics[t.StatementTimeName] = struct{}{}
		targetNames[id] = t.StatementTimeName
	}

	uri := u.String()
	u.consumeParam(`sslcert`)
	u.consumeParam(`sslkey`)
	u.consumeParam(`sslmode`)
	u.consumeParam(`sslrootcert`)

	if unknownParams := u.remainingQueryParams(); len(unknownParams) > 0 {
		return nil, errors.Errorf(
			`unknown SQL sink query parameters: %s`, strings.Join(unknownParams, ", "))
	}

	return &sqlSink{
		uri:         uri,
		tableName:   tableName,
		topics:      topics,
		hasher:      fnv.New32a(),
		targetNames: targetNames,
	}, nil
}

func (s *sqlSink) Dial() error {
	db, err := gosql.Open(`postgres`, s.uri)
	if err != nil {
		return err
	}
	if _, err := db.Exec(fmt.Sprintf(sqlSinkCreateTableStmt, s.tableName)); err != nil {
		db.Close()
		return err
	}
	s.db = db
	return nil
}

// EmitRow implements the Sink interface.
func (s *sqlSink) EmitRow(
	ctx context.Context, topicDescr TopicDescriptor, key, value []byte, updated hlc.Timestamp,
) error {
	topic := s.targetNames[topicDescr.GetID()]
	if _, ok := s.topics[topic]; !ok {
		return errors.Errorf(`cannot emit to undeclared topic: %s`, topic)
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
	var noKey, noValue []byte
	for topic := range s.topics {
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
	}
	return nil
}

func (s *sqlSink) emit(
	ctx context.Context, topic string, partition int32, key, value, resolved []byte,
) error {
	// Generate the message id on the client to match the guaranttees of kafka
	// (two messages are only guaranteed to keep their order if emitted from the
	// same producer to the same partition).
	messageID := builtins.GenerateUniqueInt(base.SQLInstanceID(partition))
	s.rowBuf = append(s.rowBuf, topic, partition, messageID, key, value, resolved)
	if len(s.rowBuf)/sqlSinkEmitCols >= sqlSinkRowBatchSize {
		return s.Flush(ctx)
	}
	return nil
}

// Flush implements the Sink interface.
func (s *sqlSink) Flush(ctx context.Context) error {
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
	return s.db.Close()
}
