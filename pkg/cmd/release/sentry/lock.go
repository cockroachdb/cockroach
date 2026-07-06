// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/api/googleapi"
)

// The release pipeline runs many releases in parallel (one per supported
// series). Each one triggers an identical `force_panic('testing')` demo crash,
// which Sentry groups into a single issue by stack trace. Without coordination
// the parallel runs clobber each other: the version-agnostic Sentry query
// matches every run's panic, and whichever run deletes the issue first leaves
// the others to either delete an issue another run still needed or fail with
// "no Sentry issues found".
//
// To avoid this we serialize the panic->find->delete critical section across
// all parallel releases with a global advisory lock backed by a single GCS
// object. Because the panic itself happens while the lock is held, at most one
// run's synthetic issue exists un-deleted at any time, so each run sees only its
// own issue. The expensive parts (the Bazel build and artifact download) run
// outside the lock and stay fully parallel.
//
// The lock object stores who holds it and since when, so a lock left behind by
// a run that died without releasing (e.g. an OOM-killed runner) can be reclaimed
// once it is older than lockTTL.
const (
	// lockBucket/lockObject identify the lock object. The staged-prod artifacts
	// bucket is reused because this tool already reads release artifacts from it,
	// so no additional IAM grant is required.
	lockBucket = "cockroach-release-artifacts-staged-prod"
	lockObject = "locks/sentry-panic.lock"

	// lockTTL bounds how long a lock is honored before it is treated as
	// abandoned. It must comfortably exceed the critical section's real runtime
	// (a panic plus the one-minute Sentry settle wait plus a few API calls).
	lockTTL = 15 * time.Minute

	// lockAcquireTimeout bounds how long a run waits for the lock. With a handful
	// of parallel releases each holding the lock for ~80s, the worst-case wait is
	// only a few minutes; 30 minutes leaves generous headroom before giving up.
	lockAcquireTimeout = 30 * time.Minute

	// lockPollInterval is the delay between acquire attempts while the lock is
	// held by another run.
	lockPollInterval = 10 * time.Second
)

// lockMetadata is the JSON payload stored in the lock object. It is purely for
// observability (who holds the lock) and stale-lock reclamation; correctness
// comes from GCS generation preconditions, not from this content.
type lockMetadata struct {
	Version    string    `json:"version"`
	RunID      string    `json:"run_id"`
	AcquiredAt time.Time `json:"acquired_at"`
}

// lockIsStale reports whether a lock acquired at acquiredAt should be treated as
// abandoned as of now (its holder presumably died without releasing it).
//
// A lock whose acquiredAt is in the future is never stale (now.Sub is negative).
// That is deliberate: it stops clock skew between runners from letting one run
// steal another's live lock. The trade-off is that a lock stamped with a badly
// wrong far-future time would wedge waiters until lockAcquireTimeout, but that
// requires a grossly misconfigured CI clock and is the safer failure to accept.
func lockIsStale(acquiredAt, now time.Time, ttl time.Duration) bool {
	return now.Sub(acquiredAt) > ttl
}

// acquireSentryLock blocks until it holds the global sentry-panic lock or
// lockAcquireTimeout elapses. On success it returns a release function that is
// safe to call exactly once (typically via defer); the function deletes the
// lock object, guarded by the generation we created, so it never removes a lock
// that was reclaimed from us as stale.
func acquireSentryLock(
	ctx context.Context, client *storage.Client, meta lockMetadata,
) (release func(), err error) {
	obj := client.Bucket(lockBucket).Object(lockObject)
	deadline := timeutil.Now().Add(lockAcquireTimeout)

	for {
		// Stamp the acquisition time on each attempt so AcquiredAt reflects when
		// this run actually won the lock, not when it started waiting. lockTTL
		// then measures how long the lock has been *held*: a run that waited
		// almost the whole TTL before winning still gets a full TTL before other
		// runs consider its lock stale (see lockIsStale / maybeStealStaleLock).
		meta.AcquiredAt = timeutil.Now()
		gen, err := tryCreateLock(ctx, obj, meta)
		if err == nil {
			log.Printf("acquired sentry-panic lock (generation %d)", gen)
			return makeReleaser(obj, gen), nil
		}
		if !isPreconditionFailed(err) {
			return nil, errors.Wrap(err, "creating lock object")
		}

		// The lock is held. Reclaim it if its holder looks dead; otherwise wait.
		if err := maybeStealStaleLock(ctx, obj); err != nil {
			log.Printf("could not reclaim sentry-panic lock (will retry): %v", err)
		}
		if timeutil.Now().After(deadline) {
			return nil, errors.Newf(
				"timed out after %s waiting for the sentry-panic lock", lockAcquireTimeout)
		}
		select {
		case <-time.After(lockPollInterval):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// tryCreateLock attempts to create the lock object, failing with a
// precondition error (see isPreconditionFailed) if it already exists. On
// success it returns the generation of the object it created.
func tryCreateLock(
	ctx context.Context, obj *storage.ObjectHandle, meta lockMetadata,
) (int64, error) {
	data, err := json.Marshal(meta)
	if err != nil {
		return 0, errors.Wrap(err, "marshaling lock metadata")
	}
	w := obj.If(storage.Conditions{DoesNotExist: true}).NewWriter(ctx)
	// The payload is tiny, so the writer buffers it and flushes on Close: the
	// DoesNotExist precondition (a 412) therefore normally surfaces from Close
	// below, not from Write. We still check Write for completeness — on error we
	// close to release the writer and report the original error.
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return 0, err
	}
	if err := w.Close(); err != nil {
		return 0, err
	}
	return w.Attrs().Generation, nil
}

// maybeStealStaleLock deletes the existing lock object if its holder appears to
// have died (the lock is older than lockTTL or its metadata is unparseable). The
// delete is guarded by the holder's generation so that, if several runs race to
// reclaim it, only one succeeds; the losers (and the case where the holder
// released in the meantime) see a precondition or not-found error and simply
// retry. A live, non-stale lock is left untouched.
func maybeStealStaleLock(ctx context.Context, obj *storage.ObjectHandle) error {
	r, err := obj.NewReader(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotExist) {
			return nil // released already; the next create attempt will win.
		}
		return errors.Wrap(err, "reading lock")
	}
	gen := r.Attrs.Generation
	data, readErr := io.ReadAll(r)
	_ = r.Close()
	if readErr != nil {
		return errors.Wrap(readErr, "reading lock body")
	}

	var meta lockMetadata
	if err := json.Unmarshal(data, &meta); err != nil {
		// Treat a corrupt lock as stale so a bad write can't deadlock the
		// pipeline forever.
		log.Printf("sentry-panic lock metadata unparseable (%v); reclaiming", err)
	} else if !lockIsStale(meta.AcquiredAt, timeutil.Now(), lockTTL) {
		return nil // legitimately held.
	} else {
		log.Printf("reclaiming stale sentry-panic lock held by version=%s run=%s since %s",
			meta.Version, meta.RunID, meta.AcquiredAt.Format(time.RFC3339))
	}

	if err := obj.If(storage.Conditions{GenerationMatch: gen}).Delete(ctx); err != nil {
		if isPreconditionFailed(err) || errors.Is(err, storage.ErrObjectNotExist) {
			return nil // someone else reclaimed or the holder released; retry.
		}
		return errors.Wrap(err, "deleting stale lock")
	}
	return nil
}

// makeReleaser returns a release function that deletes the lock object guarded
// by gen (the generation we created), so it is a no-op if our lock was already
// reclaimed as stale. It is safe to call more than once.
func makeReleaser(obj *storage.ObjectHandle, gen int64) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			// Use a fresh, bounded context: the caller's context may already be
			// canceled by the time we release.
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			err := obj.If(storage.Conditions{GenerationMatch: gen}).Delete(ctx)
			switch {
			case err == nil:
				log.Printf("released sentry-panic lock")
			case errors.Is(err, storage.ErrObjectNotExist) || isPreconditionFailed(err):
				// Our lock was already reclaimed as stale; nothing to release.
				log.Printf("sentry-panic lock already gone at release time: %v", err)
			default:
				log.Printf("WARNING: failed to release sentry-panic lock: %v", err)
			}
		})
	}
}

// isPreconditionFailed reports whether err is a GCS HTTP 412, which the API
// returns when a generation precondition (DoesNotExist or GenerationMatch) is
// not met.
func isPreconditionFailed(err error) bool {
	var gerr *googleapi.Error
	return errors.As(err, &gerr) && gerr.Code == http.StatusPreconditionFailed
}
