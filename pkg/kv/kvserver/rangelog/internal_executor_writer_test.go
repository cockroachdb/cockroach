// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rangelog

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

// InternalExecutorWriter implements kvserver.RangeLogWriter
// using the Executor.
type InternalExecutorWriter struct {
	generateUniqueID func() int64
	ie               isql.Executor
	insertQuery      string
}

// NewInternalExecutorWriter returns a new InternalExecutorWriter which
// implements kvserver.RangeLogWriter using the Executor.
func NewInternalExecutorWriter(
	generateUniqueID func() int64, ie isql.Executor, tableName string,
) *InternalExecutorWriter {
	return &InternalExecutorWriter{
		generateUniqueID: generateUniqueID,
		ie:               ie,
		insertQuery: fmt.Sprintf(`
	INSERT INTO %s (
		timestamp, "rangeID", "storeID", "eventType", "otherRangeID", info, "uniqueID"
	)
	VALUES(
		$1, $2, $3, $4, $5, $6, $7
	)
	`, tableName),
	}
}

func (s *InternalExecutorWriter) WriteRangeLogEvent(
	ctx context.Context, runner kvserver.DBOrTxn, event kvserverpb.RangeLogEvent,
) error {
	args := []interface{}{
		event.Timestamp,
		event.RangeID,
		event.StoreID,
		event.EventType.String(),
		nil, // otherRangeID
		nil, // info
		s.generateUniqueID(),
	}
	if event.OtherRangeID != 0 {
		args[4] = event.OtherRangeID
	}
	if event.Info != nil {
		infoBytes, err := json.Marshal(*event.Info)
		if err != nil {
			return err
		}
		args[5] = string(infoBytes)
	}

	txn, ok := runner.(*kv.Txn)
	if !ok {
		return errors.Errorf("no transaction provided")
	}
	rows, err := s.ie.ExecEx(ctx, "log-range-event", txn,
		sessiondata.NodeUserSessionDataOverride,
		s.insertQuery, args...)
	if err != nil {
		return err
	}
	if rows != 1 {
		return errors.Errorf("%d rows affected by log insertion; expected exactly one row affected.", rows)
	}
	return nil
}
