// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package admission

import (
	"time"

	"github.com/cockroachdb/redact"
)

// fitLogger accumulates state across fit() intervals so that a
// single log line can summarize multiple intervals. A log line is emitted
// every fitLogInterval calls or when isLowCPU flips, whichever comes
// first.
type fitLogger struct {
	// Number of fit() calls accumulated since the last log line.
	intervals int
	// Wall-clock time accumulated across the window.
	elapsed time.Duration
	// Cumulative CPU time consumed across the window.
	intCPU time.Duration
	// Cumulative tokens used across the window.
	tokensUsed int64
	// Multiplier range observed across the window.
	multiplierLo, multiplierHi float64
	// Previous isLowCPU value, used to detect transitions.
	prevIsLowCPU bool
	// SQL CPU stats accumulated across the window.
	sqlAdmittedCount  int64
	sqlTokensConsumed int64
	sqlTokensReturned int64
}

// fitLogInterval is the number of fit() calls between log lines. Since fit()
// is called once per second, this means one log line every 10s (modulo
// isLowCPU transitions, which trigger an immediate log line).
const fitLogInterval = 10

// accumulate records one fit() interval's data. If it's time to emit a
// log line (every fitLogInterval intervals or on an isLowCPU transition),
// accumulate returns (msg, true) and resets the accumulator. Otherwise
// it returns ("", false).
func (a *fitLogger) accumulate(
	multiplier float64,
	isLowCPU bool,
	intCPU time.Duration,
	tokensUsed int64,
	elapsed time.Duration,
	cpuCapacity float64,
	sqlAdmittedCount int64,
	sqlTokensConsumed int64,
	sqlTokensReturned int64,
) (redact.RedactableString, bool) {
	a.intervals++
	a.elapsed += elapsed
	a.intCPU += intCPU
	a.tokensUsed += tokensUsed
	a.sqlAdmittedCount += sqlAdmittedCount
	a.sqlTokensConsumed += sqlTokensConsumed
	a.sqlTokensReturned += sqlTokensReturned
	if a.intervals == 1 {
		a.multiplierLo = multiplier
		a.multiplierHi = multiplier
	} else {
		a.multiplierLo = min(a.multiplierLo, multiplier)
		a.multiplierHi = max(a.multiplierHi, multiplier)
	}

	isLowCPUFlip := isLowCPU != a.prevIsLowCPU
	if a.intervals < fitLogInterval && !isLowCPUFlip {
		a.prevIsLowCPU = isLowCPU
		return "", false
	}

	elapsedSec := a.elapsed.Seconds()
	if elapsedSec == 0 {
		elapsedSec = 1
	}
	cpuPerSec := float64(a.intCPU) / float64(time.Second) / elapsedSec
	tokPerSec := float64(a.tokensUsed) / float64(time.Second) / elapsedSec

	// CPU state suffix: on a flip show "(now high)" or "(now low)"; if
	// low but no flip show "(low)"; if high and no flip omit.
	var cpuState string
	if isLowCPUFlip {
		if isLowCPU {
			cpuState = " (now low)"
		} else {
			cpuState = " (now high)"
		}
	} else if isLowCPU {
		cpuState = " (low)"
	}

	// SQL stats: show per-work token average and return rate when SQL
	// CPU admission has been active during this window.
	var sqlSuffix redact.RedactableString
	if a.sqlAdmittedCount > 0 {
		sqlTokPerWork := time.Duration(a.sqlTokensConsumed / a.sqlAdmittedCount)
		sqlRetPerSec := float64(a.sqlTokensReturned) / float64(time.Second) / elapsedSec
		sqlSuffix = redact.Sprintf(
			", sql: %d admits, %s/work, %.2f ret-tok/s",
			redact.Safe(a.sqlAdmittedCount),
			redact.Safe(sqlTokPerWork),
			redact.Safe(sqlRetPerSec),
		)
	}

	msg := redact.Sprintf(
		"cpu time tokens: %.2f/%g vcpu/s%s, %.2f tok/s, mult=[%.2f,%.2f]%s",
		redact.Safe(cpuPerSec), redact.Safe(cpuCapacity), redact.SafeString(cpuState),
		redact.Safe(tokPerSec),
		redact.Safe(a.multiplierLo), redact.Safe(a.multiplierHi),
		sqlSuffix,
	)
	*a = fitLogger{prevIsLowCPU: isLowCPU}
	return msg, true
}
