package schemachange

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/errwrap/testdata/src/github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

type schemaChangeWatchDog struct {
	conn            *pgxpool.Pool
	sessionID       string
	cmdChannel      chan chan struct{}
	lastActiveQuery string
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
	var currentActiveQuery, txnID string
	sessionInfo.Scan(&currentActiveQuery, &txnID)
	if w.lastActiveQuery != currentActiveQuery {
		w.lastActiveQuery = currentActiveQuery
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
				panic("connection has timed out")
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
	w.lastActiveQuery = ""
}

func (w *schemaChangeWatchDog) Stop() {
	replyChan := make(chan struct{})
	w.cmdChannel <- replyChan
	<-replyChan
	w.reset()
}
