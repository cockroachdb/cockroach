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

func newSchemaChangeWatchDog(conn *pgxpool.Pool) *schemaChangeWatchDog {
	return &schemaChangeWatchDog{
		conn:       conn,
		cmdChannel: make(chan chan struct{}),
	}
}

func (w *schemaChangeWatchDog) isConnectionActive(ctx context.Context) bool {
	// Scan the session to make sure progress is being made first.
	sessionInfo := w.conn.QueryRow(ctx,
		"SELECT active_queries, kv_txn FROM crdb_internal.cluster_sessions WHERE session_id = $1",
		w.sessionID)
	lastTxnID := w.txnID
	lastActiveQuery := w.activeQuery
	if err := sessionInfo.Scan(&w.activeQuery, &w.txnID); err != nil {
		fmt.Printf("failed to get session information: %v", err)
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
		fmt.Printf("failed to get transaction information: %v", err)
		return false
	}
	if lastTxnID != w.txnID ||
		lastNumRetries != w.numRetries {
		return true
	}
	// FIXME: Next we can check the transaction to see if retries are happening..
	return false
}

func (w *schemaChangeWatchDog) watchLoop() {
	ctx := context.Background()
	const maxTimeOutForDump = 300
	totalTimeWaited := 0
	for {
		select {
		case responseChannel := <-w.cmdChannel:
			// Only command is to stop.
			close(responseChannel)
			return
		case <-time.After(time.Second):
			// If the connection is making progress, the watch dog timer can be reset
			// again.
			if w.isConnectionActive(ctx) {
				totalTimeWaited = 0
			}
			totalTimeWaited += 1
			if totalTimeWaited > maxTimeOutForDump {
				panic(fmt.Sprintf("connection has timed out %v", w))
				// FIXME: Dump stacks..
			}
		}
	}
}

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
	go w.watchLoop()
	return nil
}

func (w *schemaChangeWatchDog) reset() {
	w.sessionID = ""
	w.activeQuery = ""
	w.txnID = ""
}

func (w *schemaChangeWatchDog) Stop() {
	replyChan := make(chan struct{})
	w.cmdChannel <- replyChan
	<-replyChan
	w.reset()
}
