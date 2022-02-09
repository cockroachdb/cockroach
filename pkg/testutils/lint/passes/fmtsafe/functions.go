// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fmtsafe

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/testutils/lint/passes/errwrap"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
)

// requireConstMsg records functions for which the last string
// argument must be a constant string.
var requireConstMsg = map[string]bool{
	"github.com/cockroachdb/cockroach/pkg/util/log.Shout":     true,
	"github.com/cockroachdb/cockroach/pkg/util/log.Event":     true,
	"github.com/cockroachdb/cockroach/pkg/util/log.VEvent":    true,
	"github.com/cockroachdb/cockroach/pkg/util/log.VErrEvent": true,

	"(*github.com/cockroachdb/cockroach/pkg/util/tracing/Span).Record": true,

	"(*github.com/cockroachdb/cockroach/pkg/sql.optPlanningCtx).log": true,
}

// requireConstFmt records functions for which the string arg
// before the final ellipsis must be a constant string.
var requireConstFmt = map[string]bool{
	// Logging things.
	"log.Printf":           true,
	"log.Fatalf":           true,
	"log.Panicf":           true,
	"(*log.Logger).Fatalf": true,
	"(*log.Logger).Panicf": true,
	"(*log.Logger).Printf": true,

	"github.com/cockroachdb/cockroach/pkg/util/log.Shoutf":          true,
	"github.com/cockroachdb/cockroach/pkg/util/log.Eventf":          true,
	"github.com/cockroachdb/cockroach/pkg/util/log.vEventf":         true,
	"github.com/cockroachdb/cockroach/pkg/util/log.VEventf":         true,
	"github.com/cockroachdb/cockroach/pkg/util/log.VErrEventf":      true,
	"github.com/cockroachdb/cockroach/pkg/util/log.VEventfDepth":    true,
	"github.com/cockroachdb/cockroach/pkg/util/log.VErrEventfDepth": true,

	// Note: More of the logging functions are populated here via the
	// init() function below.

	"github.com/cockroachdb/cockroach/pkg/util/log.MakeLegacyEntry":        true,
	"github.com/cockroachdb/cockroach/pkg/util/log.makeUnstructuredEntry":  true,
	"github.com/cockroachdb/cockroach/pkg/util/log.FormatWithContextTags":  true,
	"github.com/cockroachdb/cockroach/pkg/util/log.formatOnlyArgs":         true,
	"github.com/cockroachdb/cockroach/pkg/util/log.renderArgsAsRedactable": true,
	"github.com/cockroachdb/cockroach/pkg/util/log.formatArgs":             true,
	"github.com/cockroachdb/cockroach/pkg/util/log.logfDepth":              true,
	"github.com/cockroachdb/cockroach/pkg/util/log.shoutfDepth":            true,
	"github.com/cockroachdb/cockroach/pkg/util/log.makeStartLine":          true,

	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash.ReportOrPanic": true,

	"(*github.com/cockroachdb/cockroach/pkg/util/tracing.Span).Recordf":      true,
	"(*github.com/cockroachdb/cockroach/pkg/util/tracing.spanInner).Recordf": true,

	"(github.com/cockroachdb/cockroach/pkg/rpc.breakerLogger).Debugf": true,
	"(github.com/cockroachdb/cockroach/pkg/rpc.breakerLogger).Infof":  true,

	"(*github.com/cockroachdb/cockroach/pkg/internal/rsg/yacc.Tree).errorf": true,

	"(github.com/cockroachdb/cockroach/pkg/storage.pebbleLogger).Infof":  true,
	"(github.com/cockroachdb/cockroach/pkg/storage.pebbleLogger).Fatalf": true,

	"(*github.com/cockroachdb/cockroach/pkg/util/grpcutil.grpcLogger).Infof":    true,
	"(*github.com/cockroachdb/cockroach/pkg/util/grpcutil.grpcLogger).Warningf": true,
	"(*github.com/cockroachdb/cockroach/pkg/util/grpcutil.grpcLogger).Errorf":   true,
	"(*github.com/cockroachdb/cockroach/pkg/util/grpcutil.grpcLogger).Fatalf":   true,

	"(*github.com/cockroachdb/cockroach/pkg/kv/kvserver.raftLogger).Debugf":   true,
	"(*github.com/cockroachdb/cockroach/pkg/kv/kvserver.raftLogger).Infof":    true,
	"(*github.com/cockroachdb/cockroach/pkg/kv/kvserver.raftLogger).Warningf": true,
	"(*github.com/cockroachdb/cockroach/pkg/kv/kvserver.raftLogger).Errorf":   true,
	"(*github.com/cockroachdb/cockroach/pkg/kv/kvserver.raftLogger).Fatalf":   true,
	"(*github.com/cockroachdb/cockroach/pkg/kv/kvserver.raftLogger).Panicf":   true,

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver.makeNonDeterministicFailure":     true,
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver.wrapWithNonDeterministicFailure": true,

	"(go.etcd.io/etcd/raft/v3.Logger).Debugf":   true,
	"(go.etcd.io/etcd/raft/v3.Logger).Infof":    true,
	"(go.etcd.io/etcd/raft/v3.Logger).Warningf": true,
	"(go.etcd.io/etcd/raft/v3.Logger).Errorf":   true,
	"(go.etcd.io/etcd/raft/v3.Logger).Fatalf":   true,
	"(go.etcd.io/etcd/raft/v3.Logger).Panicf":   true,

	"(google.golang.org/grpc/grpclog.Logger).Infof":    true,
	"(google.golang.org/grpc/grpclog.Logger).Warningf": true,
	"(google.golang.org/grpc/grpclog.Logger).Errorf":   true,

	"(github.com/cockroachdb/pebble.Logger).Infof":  true,
	"(github.com/cockroachdb/pebble.Logger).Fatalf": true,

	"(github.com/cockroachdb/circuitbreaker.Logger).Infof":  true,
	"(github.com/cockroachdb/circuitbreaker.Logger).Debugf": true,

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/exprgen.errorf": true,
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/exprgen.wrapf":  true,

	"(*github.com/cockroachdb/cockroach/pkg/sql.connExecutor).sessionEventf": true,

	"(*github.com/cockroachdb/cockroach/pkg/sql/logictest.logicTest).outf":   true,
	"(*github.com/cockroachdb/cockroach/pkg/sql/logictest.logicTest).Errorf": true,
	"(*github.com/cockroachdb/cockroach/pkg/sql/logictest.logicTest).Fatalf": true,

	"github.com/cockroachdb/cockroach/pkg/server.serverErrorf":        true,
	"github.com/cockroachdb/cockroach/pkg/server.guaranteedExitFatal": true,

	"(*github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl.kafkaLogAdapter).Printf": true,

	"github.com/cockroachdb/redact.Sprintf":              true,
	"github.com/cockroachdb/redact.Fprintf":              true,
	"(github.com/cockroachdb/redact.SafePrinter).Printf": true,
	"(github.com/cockroachdb/redact.SafeWriter).Printf":  true,
	"(*github.com/cockroachdb/redact.printer).Printf":    true,

	"(*github.com/cockroachdb/cockroach/pkg/sql/pgwire.authPipe).Logf": true,

	// Error things are populated in the init() message.
}

func init() {
	for _, sev := range logpb.Severity_name {
		capsev := strings.Title(strings.ToLower(sev))
		// log.Infof, log.Warningf etc.
		requireConstFmt["github.com/cockroachdb/cockroach/pkg/util/log."+capsev+"f"] = true
		// log.VInfof, log.VWarningf etc.
		requireConstFmt["github.com/cockroachdb/cockroach/pkg/util/log.V"+capsev+"f"] = true
		// log.InfofDepth, log.WarningfDepth, etc.
		requireConstFmt["github.com/cockroachdb/cockroach/pkg/util/log."+capsev+"fDepth"] = true
		// log.Info, log.Warning, etc.
		requireConstMsg["github.com/cockroachdb/cockroach/pkg/util/log."+capsev] = true

		for _, ch := range logpb.Channel_name {
			capch := strings.ReplaceAll(strings.Title(strings.ReplaceAll(strings.ToLower(ch), "_", " ")), " ", "")
			// log.Ops.Infof, log.Ops.Warningf, etc.
			requireConstFmt["(github.com/cockroachdb/cockroach/pkg/util/log.logger"+capch+")."+capsev+"f"] = true
			// log.Ops.VInfof, log.Ops.VWarningf, etc.
			requireConstFmt["(github.com/cockroachdb/cockroach/pkg/util/log.logger"+capch+").V"+capsev+"f"] = true
			// log.Ops.InfofDepth, log.Ops.WarningfDepth, etc.
			requireConstFmt["(github.com/cockroachdb/cockroach/pkg/util/log.logger"+capch+")."+capsev+"fDepth"] = true
			// log.Ops.Info, logs.Ops.Warning, etc.
			requireConstMsg["(github.com/cockroachdb/cockroach/pkg/util/log.logger"+capch+")."+capsev] = true
		}
	}
	for _, ch := range logpb.Channel_name {
		capch := strings.ReplaceAll(strings.Title(strings.ReplaceAll(strings.ToLower(ch), "_", " ")), " ", "")
		// log.Ops.Shoutf, log.Dev.Shoutf, etc.
		requireConstFmt["(github.com/cockroachdb/cockroach/pkg/util/log.logger"+capch+").Shoutf"] = true
		// log.Ops.Shout, log.Dev.Shout, etc.
		requireConstMsg["(github.com/cockroachdb/cockroach/pkg/util/log.logger"+capch+").Shout"] = true
	}

	for errorFn, formatStringIndex := range errwrap.ErrorFnFormatStringIndex {
		if formatStringIndex < 0 {
			requireConstMsg[errorFn] = true
		} else {
			requireConstFmt[errorFn] = true
		}
	}
}
