// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemachange

import (
	"context"
	gosql "database/sql"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaChangeWatchDog connection watch dog object.
type schemaChangeWatchDog struct {
	// conn used to monitor the target session.
	conn *pgxpool.Pool
	// logger used for logging for connections.
	logger *logger
	// sessionID the session we should be monitoring.
	sessionID string
	// cmdChannel used to communicate from thread executing commands on the session.
	cmdChannel chan chan struct{}
	// activeQuery last active query observed on the monitored connection.
	activeQuery gosql.NullString
	// txnID last active transaction ID observed on the target connection.
	txnID gosql.NullString
	// numRetries number of transaction retries observed.
	numRetries int
}

// newSchemaChangeWatchDog constructs a new watch dog for monitoring
// a single connection.
func newSchemaChangeWatchDog(conn *pgxpool.Pool, logger *logger) *schemaChangeWatchDog {
	return &schemaChangeWatchDog{
		conn:       conn,
		cmdChannel: make(chan chan struct{}),
		logger:     logger,
	}
}

// isConnectionActive checks if a connection is alive and making progress,
// towards completing its current operations. This includes making sure
// data is being read or transactions are being retried. Returns true
// when progress is detected.
func (w *schemaChangeWatchDog) isConnectionActive(ctx context.Context) bool {
	// Scan the session to make sure progress is being made first.
	var status string
	sessionInfo := w.conn.QueryRow(ctx,
		"SELECT active_queries, kv_txn, status FROM crdb_internal.cluster_sessions WHERE session_id = $1",
		w.sessionID)
	lastTxnID := w.txnID
	lastActiveQuery := w.activeQuery
	if err := sessionInfo.Scan(&w.activeQuery, &w.txnID, &status); err != nil {
		w.logger.logWatchDog(fmt.Sprintf("failed to get session information: %v\n", err))
		return false
	}
	if w.activeQuery != lastActiveQuery || status == "IDLE" {
		// If the query has changed or the session is idle, it means the initial
		// query has made progress.
		return true
	}
	lastNumRetries := w.numRetries
	txnInfo := w.conn.QueryRow(ctx,
		"SELECT coalesce(sum(num_retries),0) + coalesce(sum(num_auto_retries),0) FROM crdb_internal.cluster_transactions WHERE id=$1",
		&w.txnID)
	if err := txnInfo.Scan(&w.numRetries); err != nil {
		w.logger.logWatchDog(fmt.Sprintf("failed to get transaction information: %v\n", err))
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
	const maxTimeOutForDump = time.Second * 300
	var totalTimeWaited time.Duration = 0
	for {
		select {
		case responseChannel := <-w.cmdChannel:
			// Only command is to stop.
			close(responseChannel)
			return
		case <-ctx.Done():
			// Give the connections a small amount of time to clean up, if they fail
			// to do so, we will dump stacks.
			select {
			case responseChannel := <-w.cmdChannel:
				close(responseChannel)
				return
			case <-time.After(time.Second * 4):
				panic("dumping stacks, we failed to terminate threads on time.")

			}
		case <-time.After(time.Second):
			// If the connection is making progress, the watch dog timer can be reset
			// again.
			if w.isConnectionActive(ctx) {
				totalTimeWaited = 0
			}
			totalTimeWaited += time.Second
			if totalTimeWaited > maxTimeOutForDump {
				panic(fmt.Sprintf("connection has timed out; sessionID=%s activeQuery=%+v", w.sessionID, w.activeQuery))
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
	w.activeQuery = gosql.NullString{}
	w.txnID = gosql.NullString{}
}

// Stop stops monitoring the connection and waits for the watch dog thread to
// return.
func (w *schemaChangeWatchDog) Stop() {
	replyChan := make(chan struct{})
	w.cmdChannel <- replyChan
	<-replyChan
	w.reset()
}
