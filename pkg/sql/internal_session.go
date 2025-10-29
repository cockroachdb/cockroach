// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionmutator"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// ISessionFactoryHook is a global variable that is used to avoid a circular dependency with
// the `sql/isession` package.
type ISessionFactory func(context.Context, *ConnectionStateMachine) (isql.Session, error)

var ISessionFactoryHook ISessionFactory

// ConnectionStateMachine is a wrapper around connExecutor. It exposes the
// connExecutor so that it may be used by the `isql.Session` implementation.
type ConnectionStateMachine struct {
	executor *connExecutor
	buffer   *StmtBuf

	// config is set at construction time and used during initialization.
	config struct {
		args          SessionArgs
		appStats      *ssmemstorage.Container
		server        *Server
		sd            *sessiondata.SessionData
		sdMutIterator *sessionmutator.SessionDataMutatorIterator

		metrics MemoryMetrics
		monitor *mon.BytesMonitor
	}
}

// Init prepares the state machine for execution.
func (c *ConnectionStateMachine) Init(ctx context.Context, results ClientComm) error {
	if c.executor != nil {
		return errors.AssertionFailedf("connStateMachine already initialized")
	}

	stmtBuf := NewStmtBuf(0 /*to reserve*/)

	executor := c.config.server.newConnExecutor(
		ctx,
		executorTypeInternal,
		c.config.sdMutIterator,
		stmtBuf,
		results,
		c.config.metrics,
		&c.config.server.InternalMetrics,
		c.config.appStats,
		c.config.server.cfg.GenerateID(),
		false, /*underOuterTxn*/
		nil,   /*postSetupFn*/
	)
	c.executor = executor
	c.buffer = stmtBuf

	c.executor.activate(ctx, c.config.monitor, &mon.BoundAccount{})

	// TODO(jeffswenson): we may want to hook internal sessions up to the session
	// registry. The session registry powers features like `SHOW SESSIONS` and
	// `CANCEL SESSION`. For now, its not clear if we want those features to
	// apply to internal sessions.

	return nil
}

// RunOneCommand executes a single command in the buffer. The CSM client should
// submit all of its requests using `Push` then call `RunOneCommand` until it
// returns false. The results of running the state machine are pushed onto the
// `results` object that was registered when `Init` was called.
func (c *ConnectionStateMachine) RunOneCmd(ctx context.Context) (bool, error) {
	empty, err := c.buffer.Empty()
	if err != nil {
		return false, err
	}
	if empty {
		return true, nil
	}
	c.executor.ctxHolder.connCtx = ctx
	err = c.executor.execCmd()
	if err != nil {
		// NOTE: Unlike the pgwire session, we don't tolerate io.EOF or
		// errDrainingComplete here because we don't expect the
		// ConnectionStateMachine to run any commands after Close is called.
		return false, err
	}
	return false, nil
}

// Close releases resources owned by the ConnectionStateMachine.
func (c *ConnectionStateMachine) Close(ctx context.Context) {
	c.buffer.Close()
	// c.executor may be nil if Init was never called.
	if c.executor != nil {
		c.executor.close(ctx, normalClose)
	}
}

// Push adds a command to the state machines buffer.
func (c *ConnectionStateMachine) Push(ctx context.Context, cmd Command) error {
	return c.buffer.Push(ctx, cmd)
}

// SessionDataMutatorIterator returns the session data mutator iterator for this connection.
func (c *ConnectionStateMachine) SessionDataMutatorIterator() *sessionmutator.SessionDataMutatorIterator {
	return c.config.sdMutIterator
}

// NewInternalSession constructs an internal session instance.
func (s *Server) NewInternalSession(
	ctx context.Context,
	sessionName string,
	sd *sessiondata.SessionData,
	metrics MemoryMetrics,
	monitor *mon.BytesMonitor,
) (isql.Session, error) {
	if ISessionFactoryHook == nil {
		return nil, errors.AssertionFailedf("isession package not linked into the binary")
	}

	var args SessionArgs
	args.IsSuperuser = true
	args.User = username.RootUserName()

	applyInternalExecutorSessionExceptions(sd)
	sd.Internal = true
	sds := sessiondata.NewStack(sd)
	sdMutIterator := sessionmutator.MakeSessionDataMutatorIterator(sds, args.SessionDefaults, s.cfg.Settings)
	sdMutIterator.OnDefaultIntSizeChange = func(int32) {}

	// TODO: move all of the initialization we can to `NewInternalSession` so that Init is as simple as possible.
	csm := &ConnectionStateMachine{}
	csm.config.args = args
	csm.config.sdMutIterator = sdMutIterator
	csm.config.server = s
	csm.config.metrics = metrics
	csm.config.monitor = monitor
	csm.config.sd = sd
	csm.config.appStats = s.localSqlStats.GetApplicationStats(sessionName)

	return ISessionFactoryHook(ctx, csm)
}
