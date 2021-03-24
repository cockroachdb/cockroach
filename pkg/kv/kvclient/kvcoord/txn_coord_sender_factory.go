// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// TxnCoordSenderFactory implements client.TxnSenderFactory.
type TxnCoordSenderFactory struct {
	log.AmbientContext

	st                     *cluster.Settings
	wrapped                kv.Sender
	clock                  *hlc.Clock
	heartbeatInterval      time.Duration
	linearizable           bool // enables linearizable behavior
	stopper                *stop.Stopper
	metrics                TxnMetrics
	condensedIntentsEveryN log.EveryN

	testingKnobs ClientTestingKnobs
}

var _ kv.TxnSenderFactory = &TxnCoordSenderFactory{}

// TxnCoordSenderFactoryConfig holds configuration and auxiliary objects that can be passed
// to NewTxnCoordSenderFactory.
type TxnCoordSenderFactoryConfig struct {
	AmbientCtx log.AmbientContext

	Settings *cluster.Settings
	Clock    *hlc.Clock
	Stopper  *stop.Stopper

	HeartbeatInterval time.Duration
	Linearizable      bool
	Metrics           TxnMetrics

	TestingKnobs ClientTestingKnobs
}

// NewTxnCoordSenderFactory creates a new TxnCoordSenderFactory. The
// factory creates new instances of TxnCoordSenders.
func NewTxnCoordSenderFactory(
	cfg TxnCoordSenderFactoryConfig, wrapped kv.Sender,
) *TxnCoordSenderFactory {
	tcf := &TxnCoordSenderFactory{
		AmbientContext:         cfg.AmbientCtx,
		st:                     cfg.Settings,
		wrapped:                wrapped,
		clock:                  cfg.Clock,
		stopper:                cfg.Stopper,
		linearizable:           cfg.Linearizable,
		heartbeatInterval:      cfg.HeartbeatInterval,
		metrics:                cfg.Metrics,
		condensedIntentsEveryN: log.Every(time.Second),
		testingKnobs:           cfg.TestingKnobs,
	}
	if tcf.st == nil {
		tcf.st = cluster.MakeTestingClusterSettings()
	}
	if tcf.heartbeatInterval == 0 {
		tcf.heartbeatInterval = base.DefaultTxnHeartbeatInterval
	}
	if tcf.metrics == (TxnMetrics{}) {
		tcf.metrics = MakeTxnMetrics(metric.TestSampleInterval)
	}
	return tcf
}

// RootTransactionalSender is part of the TxnSenderFactory interface.
func (tcf *TxnCoordSenderFactory) RootTransactionalSender(
	txn *roachpb.Transaction, pri roachpb.UserPriority,
) kv.TxnSender {
	return newRootTxnCoordSender(tcf, txn, pri)
}

// LeafTransactionalSender is part of the TxnSenderFactory interface.
func (tcf *TxnCoordSenderFactory) LeafTransactionalSender(
	tis *roachpb.LeafTxnInputState,
) kv.TxnSender {
	return newLeafTxnCoordSender(tcf, tis)
}

// NonTransactionalSender is part of the TxnSenderFactory interface.
func (tcf *TxnCoordSenderFactory) NonTransactionalSender() kv.Sender {
	return tcf.wrapped
}

// Metrics returns the factory's metrics struct.
func (tcf *TxnCoordSenderFactory) Metrics() TxnMetrics {
	return tcf.metrics
}
