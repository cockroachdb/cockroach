// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

type MonitorRegistry struct {
	// TODO(azhng): doc
	StreamingMemMonitor *mon.BytesMonitor

	// streamingMemAccounts are the memory accounts that are tracking the static
	// memory usage of the whole vectorized flow as well as all dynamic memory of
	// the streaming components.
	StreamingMemAccounts []*mon.BoundAccount

	// bufferingMemMonitors are the memory monitors of the buffering components.
	BufferingMemMonitors []*mon.BytesMonitor

	// bufferingMemAccounts are the memory accounts that are tracking the dynamic
	// memory usage of the buffering components.
	BufferingMemAccounts []*mon.BoundAccount
}

func NewMonitorRegistry(flowCtx *execinfra.FlowCtx) *MonitorRegistry {
	return &MonitorRegistry{
		StreamingMemMonitor:  flowCtx.EvalCtx.Mon,
		StreamingMemAccounts: make([]*mon.BoundAccount, 0),
		BufferingMemMonitors: make([]*mon.BytesMonitor, 0),
		BufferingMemAccounts: make([]*mon.BoundAccount, 0),
	}
}

// TODO(azhng): update comment
// newStreamingMemAccount creates a new memory account bound to the monitor in
// flowCtx and accumulates it into streamingMemAccounts slice.
func (m *MonitorRegistry) CreateStreamingMemAccount() *mon.BoundAccount {
	streamingMemAccount := m.StreamingMemMonitor.MakeBoundAccount()
	m.StreamingMemAccounts = append(m.StreamingMemAccounts, &streamingMemAccount)
	return &streamingMemAccount
}

func (m *MonitorRegistry) CreateLimitedBufferingMemAccount(
	ctx context.Context, flowCtx *execinfra.FlowCtx, name string,
) *mon.BoundAccount {
	limitedMonitor := execinfra.NewLimitedMonitor(ctx, flowCtx.EvalCtx.Mon, flowCtx.Cfg, name+"-limited")
	return m.createBufferingMemAccount(limitedMonitor)
}

// TODO(azhng): update this
// CreateBufferingUnlimitedMemAccount instantiates an unlimited memory monitor.
// These should only be used when spilling to disk and an operator is made aware
// of a memory usage limit separately.
// The receiver is updated to have a reference to the unlimited memory monitor.
func (m *MonitorRegistry) CreateUnlimitedBufferingMemAccount(
	ctx context.Context, flowCtx *execinfra.FlowCtx, name string,
) *mon.BoundAccount {
	unlimitedMonitor := execinfra.NewMonitor(ctx, flowCtx.EvalCtx.Mon, name+"-unlimited")
	return m.createBufferingMemAccount(unlimitedMonitor)
}

func (m *MonitorRegistry) CreateMultiMemAccountForNewUnlimitedMonitor(
	ctx context.Context, flowCtx *execinfra.FlowCtx, name string, numOfMemAccounts int,
) []*mon.BoundAccount {
	monitor := execinfra.NewMonitor(ctx, flowCtx.EvalCtx.Mon, name)
	accounts := make([]*mon.BoundAccount, numOfMemAccounts)
	for i := range accounts {
		acc := monitor.MakeBoundAccount()
		accounts[i] = &acc
	}
	m.BufferingMemMonitors = append(m.BufferingMemMonitors, monitor)
	m.BufferingMemAccounts = append(m.BufferingMemAccounts, accounts...)
	return accounts
}

// CreateStandaloneMemAccount instantiates an unlimited memory monitor and a
// memory account that have a standalone budget. This means that the memory
// registered with these objects is *not* reported to the root monitor (i.e.
// it will not count towards max-sql-memory). Use it only when the memory in
// use is accounted for with a different memory monitor. The receiver is
// updated to have references to both objects.
func (m *MonitorRegistry) CreateStandaloneMemAccount(
	ctx context.Context, flowCtx *execinfra.FlowCtx, name string,
) *mon.BoundAccount {
	standaloneMemMonitor := mon.MakeMonitor(
		name+"-standalone",
		mon.MemoryResource,
		nil,           /* curCount */
		nil,           /* maxHist */
		-1,            /* increment: use default increment */
		math.MaxInt64, /* noteworthy */
		flowCtx.Cfg.Settings,
	)
	m.BufferingMemMonitors = append(m.BufferingMemMonitors, &standaloneMemMonitor)
	standaloneMemMonitor.Start(ctx, nil, mon.MakeStandaloneBudget(math.MaxInt64))
	standaloneMemAccount := standaloneMemMonitor.MakeBoundAccount()
	m.BufferingMemAccounts = append(m.BufferingMemAccounts, &standaloneMemAccount)
	return &standaloneMemAccount
}

func (m *MonitorRegistry) Merge(other *MonitorRegistry) {
	m.StreamingMemAccounts = append(m.StreamingMemAccounts, other.StreamingMemAccounts...)
	m.BufferingMemAccounts = append(m.BufferingMemAccounts, other.BufferingMemAccounts...)
	m.BufferingMemMonitors = append(m.BufferingMemMonitors, other.BufferingMemMonitors...)
}

func (m *MonitorRegistry) CleanupBufferingMonitorsAndAccounts(ctx context.Context) {
	for _, a := range m.BufferingMemAccounts {
		a.Close(ctx)
	}
	// TODO(azhng): reset the slice too ?

	for _, mon := range m.BufferingMemMonitors {
		mon.Stop(ctx)
	}
}

func (m *MonitorRegistry) Cleanup(ctx context.Context) {
	m.CleanupBufferingMonitorsAndAccounts(ctx)

	for _, acc := range m.StreamingMemAccounts {
		acc.Close(ctx)
	}
}

func (m *MonitorRegistry) createBufferingMemAccount(monitor *mon.BytesMonitor) *mon.BoundAccount {
	m.BufferingMemMonitors = append(m.BufferingMemMonitors, monitor)
	bufferingMemAccount := monitor.MakeBoundAccount()
	m.BufferingMemAccounts = append(m.BufferingMemAccounts, &bufferingMemAccount)
	return &bufferingMemAccount
}
