// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemachange

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type schemaChangeWatchDog struct {
	conn        *pgxpool.Pool
	sessionID   string
	cmdChannel  chan chan struct{}
	activeQuery string
	txnID       string
	numRetries  int
}

// newSchemaChangeWatchDog constructs a new watch dog for monitoring
// a single connection.
func newSchemaChangeWatchDog(conn *pgxpool.Pool) *schemaChangeWatchDog {
	return &schemaChangeWatchDog{
		conn:       conn,
		cmdChannel: make(chan chan struct{}),
	}
}

// isConnectionActive checks if a connection is alive and making progress,
// towards completing its current operations. This includes making sure
// data is being read or transactions are being retried. Returns true
// when progress is detected.
func (w *schemaChangeWatchDog) isConnectionActive(ctx context.Context) bool {
	// Scan the session to make sure progress is being made first.
	sessionInfo := w.conn.QueryRow(ctx,
		"SELECT active_queries, kv_txn FROM crdb_internal.cluster_sessions WHERE session_id = $1",
		w.sessionID)
	lastTxnID := w.txnID
	lastActiveQuery := w.activeQuery
	if err := sessionInfo.Scan(&w.activeQuery, &w.txnID); err != nil {
		fmt.Printf("failed to get session information: %v\n", err)
		return false
	}
	if w.activeQuery != lastActiveQuery {
		return true
	}
	lastNumRetries := w.numRetries
	txnInfo := w.conn.QueryRow(ctx,
		"SELECT SUM(num_retries) + SUM(num_auto_retries) FROM crdb_internal.cluster_transactions WHERE id=$1",
		&w.txnID)
	if err := txnInfo.Scan(&w.numRetries); err != nil {
		fmt.Printf("failed to get transaction information: %v\n", err)
		return false
	}
	if lastTxnID != w.txnID ||
		lastNumRetries != w.numRetries {
		return true
	}
	return false
}

// watchLoop monitors the connection until either observed work is finished,
// or a timeout is hit.
func (w *schemaChangeWatchDog) watchLoop(ctx context.Context) {
	const maxTimeOutForDump = 300
	totalTimeWaited := 0
	for {
		select {
		case responseChannel := <-w.cmdChannel:
			// Only command is to stop.
			close(responseChannel)
			return
			// FIXME: Sense the deadline here?
		case <-time.After(time.Second):
			if deadline, ok := ctx.Deadline(); ok {
				if deadline.Before(time.Now()) {
					panic("dumping stacks, we failed to terminate threads on time.")
				}
			}
			// If the connection is making progress, the watch dog timer can be reset
			// again.
			if w.isConnectionActive(ctx) {
				totalTimeWaited = 0
			}
			totalTimeWaited += 1
			if totalTimeWaited > maxTimeOutForDump {
				fmt.Printf("connection time out detected\n")
				panic(fmt.Sprintf("connection has timed out %v", w))
			}
		}
	}
}

// Start starts monitoring the given transaction, as a part of this process,
// any required session information will be collected.
func (w *schemaChangeWatchDog) Start(ctx context.Context, tx pgx.Tx) error {
	sessionInfo := tx.QueryRow(ctx, "SELECT session_id FROM [SHOW session_id]")
	if sessionInfo == nil {
		return errors.AssertionFailedf("unable to retrieve session id on connection")
	}
	err := sessionInfo.Scan(&w.sessionID)
	if err != nil {
		return err
	}
	// Start up the session watch loop.
	go w.watchLoop(ctx)
	return nil
}

// reset prepares the watch dog for re-use.
func (w *schemaChangeWatchDog) reset() {
	w.sessionID = ""
	w.activeQuery = ""
	w.txnID = ""
}

// Stop stops monitoring the connection and waits for the watch dog thread to
// return.
func (w *schemaChangeWatchDog) Stop() {
	replyChan := make(chan struct{})
	w.cmdChannel <- replyChan
	<-replyChan
	w.reset()
}
