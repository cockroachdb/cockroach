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

// mmaLogger bridges callers in MMA rebalancing code to a logging
// destination. The destination is either VEventf (verbose-only) or
// Infof (the per-shedding-store burst), captured in internalLogf.
//
// When noop is true, internalLogf is nil and the logf method is a
// no-op. Hot-path callers should additionally inspect noop directly to
// skip building expensive arg lists, since variadic interface{} args are
// constructed at the call site before logf has a chance to short-circuit:
//
//	if !ml.noop {
//	    ml.logf(ctx, 3, "expensive %v", expensive)
//	}
//
// Cold callers can call ml.logf unconditionally.
type mmaLogger struct {
	noop         bool
	internalLogf func(ctx context.Context, depth int, level log.Level, format string, args ...interface{})
}

// logf invokes internalLogf if this bridge is not a no-op, otherwise
// returns immediately. The hard-coded depth=1 lifts attribution past
// this method so internalLogf attributes the entry to the caller of
// logf rather than to logf itself.
func (ml mmaLogger) logf(ctx context.Context, level log.Level, format string, args ...interface{}) {
	if ml.noop {
		return
	}
	ml.internalLogf(ctx, 1, level, format, args...)
}

// standaloneInfof emits at Infof. Used as the internalLogf for the
// makeStandaloneLogger bridge below so the call site is a named
// function (lint-allowlisted) rather than an anonymous closure. The
// depth argument is added to the InfofDepth depth so that attribution
// reaches the caller of mmaLogger.logf rather than logf itself.
func standaloneInfof(
	ctx context.Context, depth int, _ log.Level, format string, args ...interface{},
) {
	log.KvDistribution.InfofDepth(ctx, 1+depth, format, args...)
}

// makeStandaloneLogger returns a bridge for callers outside a rebalanceEnv
// that still want detailed logging in load.go (and its callees) gated
// by V(3). Returns a noop bridge when V(3) is off.
func makeStandaloneLogger(ctx context.Context) mmaLogger {
	if !log.ExpensiveLogEnabled(ctx, 3) {
		return mmaLogger{noop: true}
	}
	return mmaLogger{internalLogf: standaloneInfof}
}

// logEnv carries the logf/detailedLogf methods that mmaLogger wraps.
// It exists as a distinct type from rebalanceEnv so that calling code
// can't accidentally invoke `re.logf(...)` directly and bypass the
// mmaLogger gating; the conversion to *logEnv only happens inside
// makeLogger.
type logEnv rebalanceEnv

// logf is the verbose-only internalLogf for the bridge: emits via
// VEventfDepth at the requested level. Adds 1 to depth to step past
// this method itself.
func (le *logEnv) logf(
	ctx context.Context, depth int, level log.Level, format string, args ...interface{},
) {
	log.KvDistribution.VEventfDepth(ctx, 1+depth, level, format, args...)
}

// detailedLogf is the Infof-promoting internalLogf for the bridge: it
// always emits at Infof, ignoring the level argument. Adds 1 to depth
// to step past this method itself.
func (le *logEnv) detailedLogf(
	ctx context.Context, depth int, _ log.Level, format string, args ...interface{},
) {
	log.KvDistribution.InfofDepth(ctx, 1+depth, format, args...)
}

// makeLogger builds the bridge to use for a single rebalancing pass or
// shedding-store iteration:
//
//   - detailedLog=true: route to detailedLogf (Infof). Used for the
//     outer-loop heartbeat and the per-shedding-store burst.
//   - detailedLog=false but any per-file vmodule level is set, or a
//     verbose tracing span is attached to ctx: route to logf
//     (VEventf at level). The V(1) inside ExpensiveLogEnabled is the
//     lowest verbose level, so any positive vmodule setting (file-
//     specific or *=N) activates this path; ExpensiveLogEnabled
//     additionally covers the verbose-span case.
//   - otherwise: noop bridge; emits nothing and allows hot-path callers
//     to skip arg construction.
func (re *rebalanceEnv) makeLogger(ctx context.Context, detailedLog bool) mmaLogger {
	le := (*logEnv)(re)
	switch {
	case detailedLog:
		return mmaLogger{internalLogf: le.detailedLogf}
	case log.ExpensiveLogEnabled(ctx, 1):
		return mmaLogger{internalLogf: le.logf}
	default:
		return mmaLogger{noop: true}
	}
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
