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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/ssmemstorage"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// ISessionFactoryHook is a global variable that is used to avoid a circular dependency with
// the `sql/isession` package.
type ISessionFactory func(context.Context, *ConnectionStateMachine) (isql.Session, error)

var ISessionFactoryHook ISessionFactory

type ConnectionStateMachine struct {
	executor *connExecutor
	buffer   *StmtBuf

	// config is set at construction time and used during initialization.
	config struct {
		args          SessionArgs
		appStats      *ssmemstorage.Container
		server        *Server
		sd            *sessiondata.SessionData
		sdMutIterator *sessionDataMutatorIterator

		metrics MemoryMetrics
		monitor *mon.BytesMonitor
	}
}

func (c *ConnectionStateMachine) Init(ctx context.Context, results ClientComm) error {
	if c.executor != nil {
		return errors.AssertionFailedf("connStateMachine already initialized")
	}

	stmtBuf := NewStmtBuf(0 /*to reserve*/)

	executor := c.config.server.newConnExecutor(
		ctx,
		// TODO(jeffswenson): audit every place this is used to ensure its appropriate
		// for the internal session.
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

	// TODO do we need the initializtion logic in executor.run here? In particular do
	// we need to register with the session monitor?
	c.executor.activate(ctx, c.config.monitor, &mon.BoundAccount{})

	return nil
}

func (c *ConnectionStateMachine) Step(ctx context.Context) (bool, error) {
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
		return false, err
	}
	return false, nil
}

func (c *ConnectionStateMachine) Close(ctx context.Context) {
	c.buffer.Close()
	// c.executor may be nil if Init was never called.
	if c.executor != nil {
		c.executor.close(ctx, normalClose)
	}
}

func (c *ConnectionStateMachine) Push(ctx context.Context, cmd Command) error {
	return c.buffer.Push(ctx, cmd)
}

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
	sds := sessiondata.NewStack(sd)
	sdMutIterator := makeSessionDataMutatorIterator(sds, args.SessionDefaults, s.cfg.Settings)
	sdMutIterator.onDefaultIntSizeChange = func(int32) {}

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
