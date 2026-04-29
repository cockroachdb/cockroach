// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mmaprototype

import (
	"cmp"
	"context"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
)

// mmaLogger bridges callers in MMA rebalancing code to one of two logging
// destinations:
//
//   - verboseToInfof=true: logf always emits via InfofDepth. Used for the
//     outer-loop heartbeat and the per-shedding-store burst, where the
//     decision to promote has already been made by the caller.
//   - verboseToInfof=false: logf emits via VEventfDepth(level), gated by
//     vmodule/verbose-span at the call site of logf itself (not at the
//     site of makeMMALogger). This is what callees in arbitrary files
//     want: a vmodule setting on file F enables verbose output for logf
//     calls in F regardless of where the logger was constructed.
//
// Hot-path callers should guard expensive arg construction with V(ctx,
// level) using the same level they will pass to logf, since variadic
// interface{} args are constructed at the call site before logf has a
// chance to short-circuit:
//
//	if ml.V(ctx, 3) {
//	    ml.logf(ctx, 3, "expensive %v", expensive)
//	}
//
// Cold callers can call ml.logf unconditionally.
type mmaLogger struct {
	verboseToInfof bool
}

// makeMMALogger constructs a bridge. Pass verboseToInfof=true to promote
// every logf call to Infof; pass false to route logf through VEventfDepth
// gated per-call by vmodule/verbose-span at the emission site.
func makeMMALogger(verboseToInfof bool) mmaLogger {
	return mmaLogger{verboseToInfof: verboseToInfof}
}

// V reports whether a logf at the given level would emit. True when the
// logger is in always-Infof mode, or when vmodule/verbose-span enables
// `level` at the caller's source location. Depth=1 lifts the vmodule
// lookup past V itself so attribution lands on the caller's file.
func (ml mmaLogger) V(ctx context.Context, level log.Level) bool {
	return ml.verboseToInfof || log.ExpensiveLogEnabledVDepth(ctx, 1, level)
}

// logf emits at Infof when the logger is in verboseToInfof mode, otherwise
// at VEventfDepth(level) gated by vmodule/verbose-span at the caller of
// logf. Depth arithmetic lifts attribution past this method so the entry
// is attributed to the caller of logf.
func (ml mmaLogger) logf(ctx context.Context, level log.Level, format string, args ...interface{}) {
	if ml.verboseToInfof {
		log.KvDistribution.InfofDepth(ctx, 1, format, args...)
		return
	}
	if !log.ExpensiveLogEnabledVDepth(ctx, 1, level) {
		return
	}
	log.KvDistribution.VEventfDepth(ctx, 1, level, format, args...)
}

// formatLoadPendingChanges renders the unenacted entries in changes as a
// compact, deterministic-order summary suitable for a single log line. Each
// entry is "r{rangeID}{op}s{storeID}{loadDelta}" where op is + (add), -
// (remove), or ~ (other), separated by spaces. Returns "(none)" when there
// are no unenacted changes.
func formatLoadPendingChanges(changes map[changeID]*pendingReplicaChange) redact.RedactableString {
	type entry struct {
		id changeID
		c  *pendingReplicaChange
	}
	filtered := make([]entry, 0, len(changes))
	for id, c := range changes {
		if !c.enactedAtTime.IsZero() {
			continue
		}
		filtered = append(filtered, entry{id, c})
	}
	if len(filtered) == 0 {
		return redact.Sprint(redact.SafeString("(none)"))
	}
	slices.SortFunc(filtered, func(a, b entry) int { return cmp.Compare(a.id, b.id) })
	var buf redact.StringBuilder
	for i, e := range filtered {
		if i > 0 {
			buf.SafeRune(' ')
		}
		var op redact.SafeString
		switch e.c.replicaChangeType() {
		case AddReplica, AddLease:
			op = "+"
		case RemoveReplica, RemoveLease:
			op = "-"
		default:
			op = "~"
		}
		buf.Printf("r%d%ss%d%v", e.c.rangeID, op, e.c.target.StoreID, e.c.loadDelta)
	}
	return buf.RedactableString()
}
