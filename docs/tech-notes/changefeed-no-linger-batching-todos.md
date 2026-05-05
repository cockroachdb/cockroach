# No-Linger Batching: Review TODOs

Findings from `/review-crdb` on PR #169760 (commits through `8e47ca14e47`,
2026-05-05). Items grouped by where they belong; check off as addressed.

## Cleanup commit (small follow-up to current PR)

- [ ] **`completeBatch` panic safety + lock-held latency.** Switch to
      snapshot-under-lock then release-after-unlock pattern. Fixes both
      "manual unlock could leak the mutex on panic in `Release`/
      `freeRowEvent`" and "holding `mu` across N alloc releases serializes
      addRow/getBatch unnecessarily." `pending_buffer.go:108`.
- [ ] **Group mu-protected fields under `mu struct{ syncutil.Mutex; ... }`**
      per CRDB convention. Use `syncutil.Mutex` for test deadlock
      detection. Easier to do now than after the M4 heap lands.
      `pending_buffer.go:42`.
- [ ] **Drop `pendingBatch.inflightKeys`.** Redundant with
      `events[i].key`; have `completeBatch` iterate `batch.events`. M4
      will reshape this anyway. `pending_buffer.go:31`.
- [ ] **Validate `newPendingBuffer` config.** Reject zero/negative
      `bufferLimit`, `maxMessages`, `maxBytes` (silent breakage
      otherwise: zero `bufferLimit` blocks forever; zero per-batch caps
      make every getBatch return empty). `pending_buffer.go:60`.
- [ ] **Tighten doc comments.** `events`, `closed`, struct doc reference
      unimplemented `getBatch`/`close`/`noLingerSink`. Either defer the
      field+doc to the commit that uses it, or qualify ("…in a later
      commit"). `pending_buffer.go:54, 74, 88`.
- [ ] **`bufferLimit` unused-in-M2 hint.** One-line
      `// Honored once backpressure lands.` for cold readers.
      `pending_buffer.go:18`.
- [ ] **`completeBatch` ctx asymmetry doc.** One line noting ctx is
      forwarded only to `Alloc.Release`, not used for cancellation.
      `pending_buffer.go:108`.
- [ ] **Verify `BatchBuffer.Append` reference in `rowEvent` doc** is the
      current method name; update if stale. `sink_event.go:27`.

## PR / commit message hygiene

- [ ] **Rewrite PR title and body.** "Batching attempt 2" → something
      descriptive. Body: one paragraph naming `noLingerSink` /
      `pendingBuffer` and linking
      `docs/tech-notes/changefeed-no-linger-batching.md`; one roadmap
      line on the incremental commit plan; `Epic: none` footer.
- [ ] **Rewrite first commit "design doc"**: subject needs package/area
      prefix (e.g. `docs: add no-linger batching design doc and notes`);
      body should name the three files and explain why all three exist
      (or remove the `-notes.md` files if they're scratch and shouldn't
      live in the repo).

## Bake into Commit 4 (backpressure + close)

- [ ] **ctx-aware cond.Wait.** `sync.Cond` doesn't honor ctx. Pattern:
      separate goroutine that calls `cond.Broadcast` on `ctx.Done()`,
      and the wait loop checks `ctx.Err()` on every wakeup. Without
      this, addRow on a full buffer will silently drop rows when ctx
      cancels — and the row's `kvevent.Alloc` would leak. Real
      data-correctness footgun.
- [ ] **Document close-time event drain policy.** What happens to events
      in `b.events` when close fires before any worker has pulled them?
      Today there's no path that releases their `Alloc`s. Either close
      drains-and-releases or the contract is "caller releases on
      close-error from addRow."

## Bake into Commit 7 (assertion tests)

- [ ] **Every `cond.Wait` sits inside a `for` predicate loop**, not
      `if`. Verify in code review of commit 3.
- [ ] **"Wasted Signal" case.** addRow only `Signal`s. If only one
      worker is awake and the only pending event's key is inflight, the
      signal is consumed without progress. Construct: K is inflight,
      addRow(K) → Signal consumed → addRow(J) must still wake a worker
      for J. Most likely real-bug surface.
- [ ] **Producer key aliasing.** Test where producer reuses backing
      buffer (`key = append(key[:0], ...)`) — confirm inflight tracking
      either copies or relies on rowEvent ownership semantics.
- [ ] **Double-completion guard.** Asserting that `completeBatch` on the
      same `*pendingBatch` twice doesn't double-release the alloc /
      double-pool the rowEvent. Currently no defense.
- [ ] **`leaktest.AfterTest(t)()` in every concurrent test.** Standard
      in this package; critical with `sync.Cond` waiters.
- [ ] **`completeBatch` under cancelled ctx.** Add to `noEventLoss` or
      its own case. Verified `kvevent.Alloc.Release` does not check
      ctx.Err so memory still releases — lock down with a test.

## Performance / future polish (defer)

- [ ] `log.V(2)` formats `%x` of every key — args evaluate even when
      verbosity is off. Hot path. `pending_buffer.go:85`.
- [ ] `addRow` pre-lock `ctx.Err()` check has minor value (lock is
      uncontended in the common case). Either drop or note its real
      purpose is to fast-fail on already-cancelled ctx.
