# Diff: PR #163595 vs reimplementation

`git diff pr/163595 tbg/reimpl-goexectrace`

## Legend

Each diff chunk below is tagged with one of:

| Tag | ID | Description |
|---|---|---|
| **Fix** | I2 | pprof handler: log on partial write instead of `http.Error` after headers sent |
| **Fix** | I3 | `isSnapshotInProgress`: expanded comment acknowledging fragility (no stdlib sentinel) |
| **Fix** | I4 | Race: treat "disabled flight recorder" WriteTo error as `errDumpBusy` in `doDump` |
| **Fix** | I5 | HTTP reason param: don't mark as `redact.SafeString` |
| **Fix** | I6 | `validTag`: use ASCII range checks, not `unicode.IsLetter/IsDigit` |
| **Nit** | N2 | Remove redundant `enabled.Store(true)` in `ensureFRStarted` |
| **Nit** | N3 | Fix empty filename in periodic dump error log |
| **Nit** | N4 | Comment about best-effort rate limiting |
| **Nit** | N5 | Don't create initial FR in constructor; defer to `ensureFRStarted` |
| **Nit** | N7 | Add intent comment to `TestConcurrentDumpAndShutdown` |
| **Simplification** | S1 | Plumb `NodeFlightRecorder` via `cfg.SQLConfig` instead of extra function parameter |
| **Noise** | — | Accidental style/wrapping/ordering differences with no semantic impact |

---

## pkg/server/debug/server.go

### Fix I2: pprof handler partial-write corruption

Once `WriteTraceTo` starts writing, HTTP headers are already sent. Calling
`http.Error` at that point can't set a new status code and writes a plaintext
error body into what the client expects is a binary trace stream. The fix logs
a warning instead.

```diff
@@ -114,7 +114,9 @@ func setupProcessWideRoutes(
 			w.Header().Set("Content-Type", "application/octet-stream")
 			w.Header().Set("Content-Disposition", `attachment; filename="trace"`)
 			if _, err := etw.WriteTraceTo(r.Context(), w); err != nil {
-				http.Error(w, err.Error(), http.StatusInternalServerError)
+				// Response headers are already sent; logging is the only
+				// option since http.Error would corrupt the partial response.
+				log.Ops.Warningf(r.Context(), "goexectrace: error writing trace to HTTP response: %v", err)
 			}
 			return
 		}
```

### Fix I5: HTTP reason treated as unsafe for redaction

The `reason` query parameter is user-controlled. Wrapping it in
`redact.SafeString` could leak sensitive input into Sentry reports.

```diff
@@ -135,7 +137,7 @@ func setupProcessWideRoutes(
 			reason = "HTTP debug endpoint"
 		}
 		tag := r.URL.Query().Get("tag")
-		filename, err := etw.DumpNow(r.Context(), redact.Sprintf("%s", redact.SafeString(reason)), tag)
+		filename, err := etw.DumpNow(r.Context(), redact.Sprintf("%s", reason), tag)
 		if err != nil {
 			http.Error(w, err.Error(), http.StatusInternalServerError)
 			return
```

### Noise: `NewServer` ordering & comment cleanup

Extracts `vsrv` so it can be passed directly to `setupProcessWideRoutes`
rather than going through `spy.vsrv`. Also removes a redundant comment on
`/debug/hba_conf`. No behavioral change.

```diff
@@ -210,8 +212,9 @@ func NewServer(
 	// Set up the log spy, a tool that allows inspecting filtered logs at high
 	// verbosity. We require the tenant ID from the ambientCtx to set the logSpy
 	// tenant filter.
+	vsrv := &vmoduleServer{}
 	spy := logSpy{
-		vsrv:         &vmoduleServer{},
+		vsrv:         vsrv,
 		setIntercept: log.InterceptWith,
 	}
 	serverTenantID := ambientContext.ServerIDs.ServerIdentityString(serverident.IdentifyTenantID)
```

```diff
@@ -232,11 +235,9 @@ func NewServer(
 	}

 	// Debug routes that retrieve process-wide state.
-	setupProcessWideRoutes(ds, mux, st, tenantID, authorizer, spy.vsrv, profiler)
+	setupProcessWideRoutes(ds, mux, st, tenantID, authorizer, vsrv, profiler)

 	if hbaConfDebugFn != nil {
-		// Expose the processed HBA configuration through the debug
-		// interface for inspection during troubleshooting.
 		mux.HandleFunc("/debug/hba_conf", hbaConfDebugFn)
 	}
```

### Noise: tighten `RegisterFlightRecorder` doc comment

The old comment was overly verbose. The new comment is factual and concise.

```diff
@@ -245,11 +246,9 @@ func NewServer(
 	return ds
 }

-// RegisterFlightRecorder sets the execution trace writer used by the
-// /debug/pprof/trace handler. When the writer reports Enabled() == true, the
-// handler streams the flight recorder buffer instead of running a synchronous
-// pprof trace. This must be called during server startup before the HTTP
-// server accepts connections.
+// RegisterFlightRecorder sets the execution trace flight recorder for the
+// debug server, enabling the /debug/pprof/trace and
+// /debug/execution-trace/dump endpoints.
 func (ds *Server) RegisterFlightRecorder(fr *goexectrace.SimpleFlightRecorder) {
 	ds.flightRecorder = fr
 }
```

## pkg/server/server.go

### Simplification S1: plumb flight recorder via `SQLConfig`

Instead of passing `flightRecorder` as an extra parameter to
`makeSharedProcessTenantServerConfig`, stash it in `cfg.SQLConfig.NodeFlightRecorder`
right after creation. Shared-process tenants then just read it from `kvServerCfg.SQLConfig`.
This eliminates one function parameter and its import in `server_controller_new_server.go`.

```diff
@@ -953,6 +953,7 @@ func NewServer(cfg Config, stopper *stop.Stopper) (serverctl.ServerStartupInterf
 			log.Dev.Warningf(ctx, "failed to initialize flight recorder: %v", err)
 		}
 	}
+	cfg.SQLConfig.NodeFlightRecorder = flightRecorder

 	storeCfg := kvserver.StoreConfig{
 		DefaultSpanConfig:            cfg.DefaultZoneConfig.AsSpanConfig(),
```

### Noise: blank line before `if` block

Purely stylistic — adds a blank line for readability.

```diff
@@ -1382,6 +1383,7 @@ func NewServer(cfg Config, stopper *stop.Stopper) (serverctl.ServerStartupInterf
 		roachpb.SystemTenantID,
 		authorizer,
 	)
+
 	if flightRecorder != nil {
 		debugServer.RegisterFlightRecorder(flightRecorder)
 	}
```

## pkg/server/server_controller_new_server.go

### Simplification S1 (continued): remove `nodeFlightRecorder` parameter

All four chunks below are the consequence of S1. The flight recorder is now
read from `kvServerCfg.SQLConfig.NodeFlightRecorder`, so the explicit parameter,
its call-site argument, and the `goexectrace` import are all removed.

```diff
@@ -30,7 +30,6 @@ import (
 	"github.com/cockroachdb/cockroach/pkg/util/stop"
 	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
 	"github.com/cockroachdb/cockroach/pkg/util/tracing"
-	"github.com/cockroachdb/cockroach/pkg/util/tracing/goexectrace"
 	"github.com/cockroachdb/errors"
 	"github.com/cockroachdb/redact"
 )
```

```diff
@@ -191,7 +190,7 @@ func (s *topLevelServer) makeSharedProcessTenantConfig(
 	}

 	baseCfg, sqlCfg, err := makeSharedProcessTenantServerConfig(ctx, tenantID, tenantName, portStartHint, parentCfg,
-		localServerInfo, st, stopper, s.recorder, s.flightRecorder, tenantReadOnly)
+		localServerInfo, st, stopper, s.recorder, tenantReadOnly)
 	if err != nil {
 		return BaseConfig{}, SQLConfig{}, err
 	}
```

```diff
@@ -210,7 +209,6 @@ func makeSharedProcessTenantServerConfig(
 	st *cluster.Settings,
 	stopper *stop.Stopper,
 	nodeMetricsRecorder *status.MetricsRecorder,
-	nodeFlightRecorder *goexectrace.SimpleFlightRecorder,
 	tenantReadOnly bool,
 ) (baseCfg BaseConfig, sqlCfg SQLConfig, err error) {
 	tr := tracing.NewTracerWithOpt(ctx, tracing.WithClusterSettings(&st.SV))
```

```diff
@@ -388,7 +386,7 @@ func makeSharedProcessTenantServerConfig(
 	sqlCfg.LocalKVServerInfo = &kvServerInfo

 	sqlCfg.NodeMetricsRecorder = nodeMetricsRecorder
-	sqlCfg.NodeFlightRecorder = nodeFlightRecorder
+	sqlCfg.NodeFlightRecorder = kvServerCfg.SQLConfig.NodeFlightRecorder
 	sqlCfg.LicenseEnforcer = kvServerCfg.SQLConfig.LicenseEnforcer

 	return baseCfg, sqlCfg, nil
```

## pkg/server/tenant.go

### Noise: reorder flight-recorder init after `debugServer` creation

Moves the flight-recorder initialization block to after `debugServer` is created
so that the `RegisterFlightRecorder` call immediately follows. The logic is
equivalent; only the ordering and minor style (e.g. `frErr` → `err`,
explicit `var` declaration) changed.

```diff
@@ -506,22 +506,6 @@ func newTenantServer(
 		processCapAuthz = lsi.SameProcessCapabilityAuthorizer
 	}

-	// Use the node's flight recorder for shared-process tenants so all
-	// tenants share a single execution trace. Separate-process tenants
-	// create their own recorder from their configured trace directory.
-	flightRecorder := sqlCfg.NodeFlightRecorder
-	if flightRecorder == nil && baseCfg.ExecutionTraceDirName != "" {
-		var frErr error
-		// This flight recorder will be started via `startSampleEnvironment`
-		// which is only called on separate process tenants.
-		flightRecorder, frErr = goexectrace.NewFlightRecorder(
-			args.Settings, 10*time.Second, baseCfg.ExecutionTraceDirName,
-		)
-		if frErr != nil {
-			log.Dev.Warningf(ctx, "failed to initialize flight recorder: %v", frErr)
-		}
-	}
-
 	// Create the debug API server.
 	debugServer := debug.NewServer(
 		baseCfg.AmbientCtx,
@@ -531,6 +515,21 @@ func newTenantServer(
 		sqlCfg.TenantID,
 		processCapAuthz,
 	)
+
+	// Determine the flight recorder for this tenant. Shared-process tenants
+	// reuse the node's flight recorder; separate-process tenants create their
+	// own.
+	var flightRecorder *goexectrace.SimpleFlightRecorder
+	if sqlCfg.NodeFlightRecorder != nil {
+		flightRecorder = sqlCfg.NodeFlightRecorder
+	} else if baseCfg.ExecutionTraceDirName != "" {
+		var err error
+		flightRecorder, err = goexectrace.NewFlightRecorder(
+			args.Settings, 10*time.Second, baseCfg.ExecutionTraceDirName)
+		if err != nil {
+			log.Dev.Warningf(ctx, "failed to initialize flight recorder: %v", err)
+		}
+	}
 	if flightRecorder != nil {
 		debugServer.RegisterFlightRecorder(flightRecorder)
 	}
```

## pkg/util/tracing/goexectrace/simple_flight_recorder.go

### Fix I6: drop `unicode` import (now using ASCII range checks)

```diff
@@ -16,7 +16,6 @@ import (
 	"strings"
 	"sync/atomic"
 	"time"
-	"unicode"

 	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
 	"github.com/cockroachdb/cockroach/pkg/settings"
```

### Noise: reword `SimpleFlightRecorder` doc comment

The new comment is clearer about which settings control what.

```diff
@@ -71,9 +70,11 @@ var executionTracerOnDemandMinInterval = settings.RegisterDurationSetting(
 	settings.DurationWithMinimum(0),
 )

-// SimpleFlightRecorder is a wrapper around `trace.FlightRecorder`
-// that enables continuous trace capture over a configurable interval.
-// It supports both periodic dumps and on-demand dumps via DumpNow.
+// SimpleFlightRecorder is a wrapper around trace.FlightRecorder that enables
+// continuous trace capture. The flight recorder lifecycle is controlled by the
+// obs.execution_tracer.duration setting: a positive value enables it, zero
+// disables it. Periodic dumps to disk are independently controlled by
+// obs.execution_tracer.interval.
 type SimpleFlightRecorder struct {
 	dumpStore *dumpstore.DumpStore
```

### Noise: move `lastOnDemandDumpNanos` next to `enabled`

Groups the two atomic fields together. Also trims the `period` comment slightly
(removing "only accessed under frMu" since it's inside the `frMu` struct).

```diff
@@ -85,6 +86,10 @@ type SimpleFlightRecorder struct {
 	enabledCheckInterval time.Duration
 	enabled              atomic.Bool

+	// lastOnDemandDumpNanos stores the UnixNano timestamp of the last
+	// successful on-demand dump, used for rate limiting.
+	lastOnDemandDumpNanos atomic.Int64
+
 	// frMu protects the fr pointer, which is reassigned in
 	// ensureFRStarted when the configured duration changes. Without this
 	// mutex, concurrent readers (doDump, WriteTraceTo) would race with
@@ -96,18 +101,14 @@ type SimpleFlightRecorder struct {

 		fr *trace.FlightRecorder
 		// period is the MinAge used to construct the current fr instance.
-		// It is only accessed under frMu and is used to avoid reconstructing
-		// the FlightRecorder when the period hasn't changed.
+		// It is used to avoid reconstructing the FlightRecorder when
+		// the period hasn't changed.
 		period time.Duration
 	}
-
-	// lastOnDemandDumpNanos stores the UnixNano timestamp of the last
-	// successful on-demand dump, used for rate limiting.
-	lastOnDemandDumpNanos atomic.Int64
 }
```

### Noise: remove backtick-quoting in comments

Style preference — backtick-quoting type names in Go doc comments is nonstandard.

```diff
-// A `Dumper` implementation is needed to run `DumpStore.GC` periodically. This
-// enables `SimpleFlightRecorder` to identify its files for the automated
+// A Dumper implementation is needed to run DumpStore.GC periodically. This
+// enables SimpleFlightRecorder to identify its files for the automated
 // cleanup process.
 var _ dumpstore.Dumper = &SimpleFlightRecorder{}
```

### Nit N5: defer FR construction to `ensureFRStarted`

The original constructor eagerly created a `trace.FlightRecorder` that would be
immediately replaced by `ensureFRStarted` with the correct `MinAge`. This removes
the wasted allocation.

```diff
@@ -121,15 +122,12 @@ func NewFlightRecorder(
 		return nil, errors.Wrap(err, "cannot create execution trace directory, will not record execution traces")
 	}

-	sfr := &SimpleFlightRecorder{
+	return &SimpleFlightRecorder{
 		dumpStore:            dumpstore.NewStore(directory, executionTracerTotalDumpSizeLimit, st),
 		sv:                   &st.SV,
 		directory:            directory,
 		enabledCheckInterval: enabledCheckInterval,
-	}
-
-	sfr.frMu.fr = trace.NewFlightRecorder(trace.FlightRecorderConfig{})
-	return sfr, nil
+	}, nil
 }
```

### Noise: remove backtick-quoting in interface doc comments

Same style cleanup as above.

```diff
-// CheckOwnsFile is part of the `Dumper` interface.
+// CheckOwnsFile is part of the Dumper interface.
 func (sfr *SimpleFlightRecorder) CheckOwnsFile(ctx context.Context, fi os.DirEntry) bool {

-// PreFilter is part of the `Dumper` interface. In this case we do not mark any
+// PreFilter is part of the Dumper interface. In this case we do not mark any
```

### Fix I3 + Fix I4: expanded error helpers with fragility comments + `isDisabledRecorder`

`isSnapshotInProgress` existed in the original PR but had no comment about
the fragility of string matching. `isDisabledRecorder` is new — it lets
`doDump` treat a "stopped FR" error as `errDumpBusy` (Fix I4), preventing
a spurious error when `stopFR` runs between `DumpNow`'s enabled check and
the `WriteTo` call.

Both helpers are moved earlier in the file (before `doDump`) so they're
defined before first use.

```diff
+// isSnapshotInProgress reports whether err indicates a concurrent
+// FlightRecorder.WriteTo call is already in progress. Go 1.25's
+// runtime/trace does not export a sentinel error for this condition;
+// it returns a plain fmt.Errorf. String matching is the only option.
+// If the runtime changes this message, we'll lose the ability to
+// distinguish busy from broken, which is acceptable: the worst case
+// is that a concurrent dump returns an error instead of being silently
+// skipped.
+func isSnapshotInProgress(err error) bool {
+	return err != nil && strings.Contains(err.Error(), "already in progress")
+}
+
+// isDisabledRecorder reports whether err indicates WriteTo was called
+// on a stopped FlightRecorder. Same caveat as isSnapshotInProgress:
+// the runtime uses a plain fmt.Errorf with no sentinel.
+func isDisabledRecorder(err error) bool {
+	return err != nil && strings.Contains(err.Error(), "disabled flight recorder")
+}
```

### Fix I4 (continued): `doDump` doc comment updated

The doc now reflects that `errDumpBusy` is also returned for the
disabled-recorder case, not just the concurrent-snapshot case.

```diff
 // doDump performs a dump to a file with the given tag. If a concurrent WriteTo
-// is already in progress, doDump returns errDumpBusy without writing a file.
-// Callers should treat errDumpBusy as a non-fatal skip.
+// is already in progress, or if the FR was stopped between the caller's
+// enabled check and the actual write, doDump returns errDumpBusy without
+// writing a file.
```

### Fix I4 (continued): treat disabled-recorder error as `errDumpBusy`

This is the actual code change for I4: `isDisabledRecorder(writeErr)` is now
checked alongside `isSnapshotInProgress`.

```diff
@@ -191,7 +209,7 @@ func (sfr *SimpleFlightRecorder) doDump(tag string) (string, error) {
 	closeErr := tmpFile.Close()
 	if writeErr != nil {
 		_ = os.Remove(tmpName)
-		if isSnapshotInProgress(writeErr) {
+		if isSnapshotInProgress(writeErr) || isDisabledRecorder(writeErr) {
 			return "", errDumpBusy
 		}
 		return "", errors.Wrapf(writeErr, "writing flight record")
```

### Fix I6: `validTag` uses ASCII range checks

`unicode.IsLetter`/`unicode.IsDigit` accept non-ASCII characters despite the
comment saying "alphanumeric or underscore". The new implementation is explicit
about the ASCII restriction. `isSnapshotInProgress` was moved earlier (see
Fix I3 chunk above).

```diff
 func validTag(s string) bool {
 	for _, r := range s {
-		if r != '_' && !unicode.IsLetter(r) && !unicode.IsDigit(r) {
+		if r != '_' && !(r >= 'a' && r <= 'z') && !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') {
 			return false
 		}
 	}
 	return true
 }
-
-// isSnapshotInProgress reports whether err indicates a concurrent
-// FlightRecorder.WriteTo call is already in progress. The runtime/trace
-// package returns a plain fmt.Errorf for this case (no sentinel).
-func isSnapshotInProgress(err error) bool {
-	return err != nil && strings.Contains(err.Error(), "already in progress")
-}
```

### Nit N4: clarify best-effort nature of rate limiting

Two concurrent callers can both pass the time check before either stores
the new timestamp. This is acceptable but worth documenting.

```diff
 	// Rate limiting: check whether we're within the minimum interval since
-	// the last successful dump.
+	// the last successful dump. This is best-effort: two concurrent callers
+	// can both pass the check before either updates the timestamp.
 	minInterval := executionTracerOnDemandMinInterval.Get(sfr.sv)
```

### Noise: remove redundant comment

The code is self-explanatory — it stores the timestamp after a successful dump.

```diff
-	// Update the rate limit timestamp only after a successful dump so that
-	// failed dumps don't consume the rate limit window.
 	sfr.lastOnDemandDumpNanos.Store(timeutil.Now().UnixNano())
```

### Nit N5 (continued): nil-guard for `frMu.fr` + trim comment

Since `NewFlightRecorder` no longer eagerly creates a `trace.FlightRecorder`,
`frMu.fr` can be nil. The nil guard is added here and in `ensureFRStarted`
below. The "no-op" and "blocks until writes complete" comments were redundant
with the code.

```diff
-// stopFR stops the flight recorder under frMu. It is a no-op if the FR is
-// not currently running.
+// stopFR stops the flight recorder under frMu.
 func (sfr *SimpleFlightRecorder) stopFR() {
 	sfr.frMu.Lock()
 	defer sfr.frMu.Unlock()
-	if sfr.frMu.fr.Enabled() {
-		// This will already block until any active writes complete.
+	if sfr.frMu.fr != nil && sfr.frMu.fr.Enabled() {
 		sfr.frMu.fr.Stop()
 		sfr.enabled.Store(false)
 	}
 }
```

### Noise: tighten `ensureFRStarted` doc comment

Minor rewording; removes "false if Start failed" which is implied by "returns
true if running."

```diff
 // ensureFRStarted starts the flight recorder with the given period under
-// frMu, if it is not already running. A new FlightRecorder is only created
-// when the requested period differs from the current one, since
-// FlightRecorderConfig is immutable after construction. Returns true if the
-// FR is running after this call, false if Start failed.
+// frMu, if it is not already running with the same period. A new
+// FlightRecorder is only created when the requested period differs from the
+// current one, since FlightRecorderConfig is immutable after construction.
+// Returns true if the FR is running after this call.
 func (sfr *SimpleFlightRecorder) ensureFRStarted(ctx context.Context, period time.Duration) bool {
```

### Nit N5 (continued) + Noise: nil-guard + line wrapping

The nil-guard on `sfr.frMu.fr` is required because of N5. The line wrapping
is `crlfmt` noise (100-column limit).

```diff
 	if sfr.frMu.period != period {
-		log.Dev.Infof(ctx, "goexectrace: reconfiguring flight recorder period: %.1f sec -> %.1f sec",
+		log.Dev.Infof(ctx,
+			"goexectrace: reconfiguring flight recorder period: %.1f sec -> %.1f sec",
 			sfr.frMu.period.Seconds(), period.Seconds())
-		if sfr.frMu.fr.Enabled() {
+		if sfr.frMu.fr != nil && sfr.frMu.fr.Enabled() {
 			sfr.frMu.fr.Stop()
 		}
```

### Nit N2: remove redundant `enabled.Store(true)`

If the FR is already `.Enabled()`, `enabled` was already set to `true` by a
prior `ensureFRStarted` call. Re-storing is a no-op.

```diff
 	if sfr.frMu.fr.Enabled() {
-		sfr.enabled.Store(true)
 		return true
 	}
```

### Noise: line wrapping (`crlfmt` 100-column)

```diff
 	if err := sfr.frMu.fr.Start(); err != nil {
-		log.Dev.Warningf(ctx, "goexectrace: error while starting flight recorder, will try again: %v", err)
+		log.Dev.Warningf(ctx,
+			"goexectrace: error while starting flight recorder, will try again: %v", err)
 		return false
 	}
 	sfr.enabled.Store(true)
-	log.Dev.Infof(ctx, "goexectrace: flight recorder started with period: %.1f sec", period.Seconds())
+	log.Dev.Infof(ctx, "goexectrace: flight recorder started with period: %.1f sec",
+		period.Seconds())
 	return true
 }
```

### Noise: trim comment about `frMu` (implementation detail)

The "acquire frMu" part is an implementation detail of `stopFR`/`ensureFRStarted`,
not useful to the reader at this call site.

```diff
 				// The flight recorder lifecycle is controlled by the duration
-				// setting: positive enables, zero disables. stopFR and
-				// ensureFRStarted acquire frMu internally to prevent
-				// concurrent WriteTo calls during lifecycle changes.
+				// setting: positive enables, zero disables.
 				if duration == 0 {
```

### Noise: trim comment (parenthetical wasn't essential)

```diff
 				// Periodic dumps are optional; controlled by the interval
-				// setting. If zero, the FR is running (for DumpNow / pprof)
-				// but we don't dump to disk periodically.
+				// setting. If zero, the FR is running but we don't dump to
+				// disk periodically.
 				interval := ExecutionTracerInterval.Get(sfr.sv)
```

### Nit N3: fix empty filename in periodic dump error log

When `doDump` fails early (before creating the file), `filename` is `""`,
producing a confusing `(%s): %v` with an empty string. The fix removes
`filename` from the error log. The `_ = filename` suppresses the unused-var
lint while noting that `doDump` logs the filename internally on success.
The removed "Another WriteTo" comment was redundant with the code.

```diff
 				filename, err := sfr.doDump("" /* tag */)
 				if errors.Is(err, errDumpBusy) {
-					// Another WriteTo is in progress; skip this cycle.
 					t.Reset(max(interval-timeutil.Since(startTime), 0))
 					continue
 				}
 				if err != nil {
-					log.Dev.Warningf(ctx, "goexectrace: error during periodic dump (%s): %v", filename, err)
+					log.Dev.Warningf(ctx,
+						"goexectrace: error during periodic dump: %v", err)
 					t.Reset(max(interval-timeutil.Since(startTime), 0))
 					continue
 				}

 				sfr.dumpStore.GC(ctx, timeutil.Now(), sfr)
+				_ = filename // logged by doDump internals if needed
 				t.Reset(max(interval-timeutil.Since(startTime), 0))
```

## pkg/util/tracing/goexectrace/simple_flight_recorder_test.go

### Noise: remove narrating test comments

All of the chunks below remove comments that merely narrate what the next
line of code does ("Enable the flight recorder", "Verify file exists", etc.).
These add no information beyond what the code itself conveys. Per CockroachDB
coding guidelines, comments should explain *why*, not *what*.

```diff
@@ -120,7 +120,6 @@ func TestSettingCombinations(t *testing.T) {
 	err = fr.Start(ctx, stopper)
 	require.NoError(t, err)

-	// Disable on-demand rate limiting so DumpNow tests aren't affected.
 	executionTracerOnDemandMinInterval.Override(ctx, &st.SV, 0)
```

```diff
-			// Clear files written during the settings transition.
 			require.NoError(t, clearDir())
```

```diff
 			} else {
-				// The async loop has already observed the new settings
-				// (confirmed by SucceedsSoon above), so no periodic
-				// dumps should occur.
 				files, err := os.ReadDir(dir)
```

```diff
 			if tc.wantDumpNow {
-				// DumpNow may return ("", nil) if a periodic dump is
-				// in progress; retry until we get a file.
 				var filename string
```

```diff
-	// Transition: disable periodic dumps while FR stays on.
 	t.Run("disable_periodic_fr_stays_on", func(t *testing.T) {
-		// Start with both enabled.
 		ExecutionTracerDuration.Override(ctx, &st.SV, 10*time.Second)
```

```diff
-		// Disable periodic dumps only.
 		ExecutionTracerInterval.Override(ctx, &st.SV, 0)

 		// Wait for the async loop to observe interval=0 and stop
-		// dumping. A single empty-dir check is insufficient because
-		// clearDir+ReadDir is near-instantaneous and would always
-		// see an empty dir even while dumps are still occurring.
-		// Instead, require multiple consecutive empty observations
-		// across SucceedsSoon poll intervals (~25ms each), spanning
-		// well beyond the old 1ms dump interval.
+		// dumping. Multiple consecutive empty observations confirm
+		// periodic dumps have actually stopped.
 		emptyChecks := 0
```

```diff
-		// DumpNow still works. Retry in case a final in-flight
-		// periodic dump hasn't released its WriteTo lock yet.
 		var filename string
```

```diff
-	// Transition: change duration while FR is already running to verify
-	// ensureFRStarted rebuilds the FR with the new period.
 	t.Run("change_duration_while_running", func(t *testing.T) {
```

```diff
-		// Change duration to a different non-zero value without going
-		// through zero. The FR should stay enabled and accept dumps.
 		ExecutionTracerDuration.Override(ctx, &st.SV, 20*time.Second)
```

```diff
-		// Verify the FR is functional with the new period by taking a dump.
 		var filename string
```

```diff
-		// Verify the internal period was updated by the async loop.
 		testutils.SucceedsSoon(t, func() error {
```

```diff
 	t.Run("fails when not enabled", func(t *testing.T) {
-		// FR is started but the duration setting is 0, so it's not enabled yet.
 		_, err := fr.DumpNow(ctx, "test reason", "test_tag")
```

```diff
-	// Enable the flight recorder via duration (no periodic dumps needed).
 	ExecutionTracerDuration.Override(ctx, &st.SV, 10*time.Second)
-	// Disable rate limiting for tests.
 	executionTracerOnDemandMinInterval.Override(ctx, &st.SV, 0)
```

```diff
-		// Verify file exists and is non-empty.
 		fi, err := os.Stat(filename)
 		require.NoError(t, err)
 		require.Greater(t, fi.Size(), int64(0))

-		// Verify the filename contains the tag.
 		base := filepath.Base(filename)
 		require.Contains(t, base, "scheduling_latency")
-
-		// Verify the file matches the GC regex.
 		require.True(t, fileMatchRegexp.MatchString(base))
```

```diff
-	// Enable the flight recorder via duration (no periodic dumps).
 	ExecutionTracerDuration.Override(ctx, &st.SV, 10*time.Second)
```

```diff
-	// First dump should succeed.
 	filename, err := fr.DumpNow(ctx, "first dump", "first")
 	require.NoError(t, err)
 	require.NotEmpty(t, filename)

-	// Second dump should be rate limited (returns "", nil).
 	filename2, err := fr.DumpNow(ctx, "second dump", "second")
```

```diff
-	// Enable the flight recorder via duration (no periodic dumps).
 	ExecutionTracerDuration.Override(ctx, &st.SV, 10*time.Second)
 	executionTracerOnDemandMinInterval.Override(ctx, &st.SV, 0)
```

```diff
-	// Launch multiple concurrent DumpNow calls. If a dump is already in
-	// progress, concurrent callers are skipped (return "", nil).
 	const numGoroutines = 5
```

```diff
-	// All goroutines should complete without error. Some may return ""
-	// (skipped due to concurrent snapshot), but at least one should
-	// produce a file.
 	var gotFilename string
```

```diff
-	// Verify the file exists and is non-empty.
 	fi, err := os.Stat(gotFilename)
```

```diff
-	// Enable the flight recorder via duration (no periodic dumps needed).
 	ExecutionTracerDuration.Override(ctx, &st.SV, 10*time.Second)
```

```diff
-	// WriteTraceTo should write data to the buffer.
 	var buf bytes.Buffer
```

```diff
-	// Verify various tagged filenames match the GC regex.
 	tags := []string{"", "scheduling_latency", "stmt_diag", "lease_expiry"}
```

### Nit N7: expand `TestConcurrentDumpAndShutdown` intent comment

The original just said "primarily useful under the race detector". The expanded
version explains *what* it's verifying (`frMu` prevents data races).

```diff
 // TestConcurrentDumpAndShutdown exercises the race between DumpNow (which
 // calls WriteTo) and the Start loop shutting down the flight recorder. This
-// test is primarily useful under the race detector (-race).
+// test is primarily useful under the race detector (-race) to verify that the
+// frMu locking correctly prevents data races between concurrent WriteTo calls
+// and FR lifecycle changes.
 func TestConcurrentDumpAndShutdown(t *testing.T) {
```

### Noise: remove narrating test comments (continued)

```diff
-	// Hammer DumpNow and WriteTraceTo while toggling the duration setting
-	// to trigger Stop/Start in the background loop.
 	var wg sync.WaitGroup
```

```diff
-	// Toggle the duration setting to trigger FR lifecycle changes.
 	for range iterations {
```
