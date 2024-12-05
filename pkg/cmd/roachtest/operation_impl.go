// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/operation"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

var errOperationFatal = errors.New("o.Fatal() was called")

type operationImpl struct {
	workerId        int
	spec            *registry.OperationSpec
	clusterSettings install.ClusterSettings
	startOpts       option.StartOpts

	// l is the logger that the operation will use for its output.
	l *logger.Logger

	mu struct {
		syncutil.RWMutex
		done bool

		// cancel, if set, is called from the o.Fatal() family of functions when the
		// op is being marked as failed (i.e. when the failures slice is being
		// appended to). This is used to cancel the context passed to o.spec.Run(),
		// so async goroutines can be notified.
		cancel func()

		// failures added via addFailures, in order. An operation may have multiple
		// calls to o.Fail()/Error(), with each call adding to this slice once.
		failures []error

		status string
	}

	workLoadCluster *clusterImpl
}

func (o *operationImpl) ClusterCockroach() string {
	return o.clusterSettings.Binary
}

func (o *operationImpl) ClusterSettings() install.ClusterSettings {
	return o.clusterSettings
}

func (o *operationImpl) StartOpts() option.StartOpts {
	return o.startOpts
}

func (o *operationImpl) Name() string {
	return o.spec.Name
}

// L returns the operation's logger.
func (o *operationImpl) L() *logger.Logger {
	return o.l
}

// Status sets the main status message for the operation. This is logged to
// o.L() and is the main way to log status of an operation.
func (o *operationImpl) Status(args ...interface{}) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.mu.status = fmt.Sprint(args...)
	if !o.L().Closed() {
		o.L().PrintfCtxDepth(context.TODO(), 3, "[%d] operation status: %s", o.workerId, o.mu.status)
	}
}

// Fatal marks the operation as failed, prints the args to o.L(), and calls the
// cancel method if specified. Also shuts down the process. Can be called
// multiple times.
func (o *operationImpl) Fatal(args ...interface{}) {
	o.addFailureAndCancel(1, "", args...)
	panic(errOperationFatal)
}

// Fatalf is like Fatal, but takes a format string.
func (o *operationImpl) Fatalf(format string, args ...interface{}) {
	o.addFailureAndCancel(1, format, args...)
	panic(errOperationFatal)
}

// FailNow implements the Operation interface.
func (o *operationImpl) FailNow() {
	o.addFailureAndCancel(1, "FailNow called")
	panic(errOperationFatal)
}

// Error implements the Operation interface
func (o *operationImpl) Error(args ...interface{}) {
	o.addFailureAndCancel(1, "", args...)
}

// Errorf implements the Operation interface.
func (o *operationImpl) Errorf(format string, args ...interface{}) {
	o.addFailureAndCancel(1, format, args...)
}

func (o *operationImpl) addFailureAndCancel(depth int, format string, args ...interface{}) {
	o.addFailure(depth+1, format, args...)
	o.mu.cancel()
}

// addFailure depth indicates how many stack frames to skip when reporting the
// site of the failure in logs. `0` will report the caller of addFailure, `1` the
// caller of the caller of addFailure, etc.
func (o *operationImpl) addFailure(depth int, format string, args ...interface{}) {
	if format == "" {
		format = strings.Repeat(" %v", len(args))[1:]
	}
	reportFailure := errors.NewWithDepthf(depth+1, format, args...)

	o.mu.Lock()
	defer o.mu.Unlock()
	o.mu.failures = append(o.mu.failures, reportFailure)

	msg := reportFailure.Error()

	failureNum := len(o.mu.failures)
	o.L().Printf("[%d] operation failure #%d: %s", o.workerId, failureNum, msg)
}

func (o *operationImpl) Failed() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()

	return len(o.mu.failures) > 0
}

// WorkloadCluster can return nil if o.workLoadCluster is not set.
func (o *operationImpl) WorkloadCluster() cluster.Cluster {
	if o.workLoadCluster == nil {
		return nil
	}
	return o.workLoadCluster
}

var _ operation.Operation = &operationImpl{}
