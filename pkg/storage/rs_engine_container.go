// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// rsEngineContainer implements RSEngine by managing one active innerRSEngine
// and tracking quiesced engines from past manifest changes.
type rsEngineContainer struct {
	opts       RSEngineOptions
	openEngine OpenInnerRSEngineFunc
	stopper    *stop.Stopper
	// activeMu protects the active engine pointer, the pending engine from
	// PrepareExternalManifest, and the quiesced engine list. Short operations
	// hold RLock for their duration; long operations hold RLock briefly to
	// load and ref the active engine. InstallPreparedManifest and
	// removeQuiesced hold a (write) Lock.
	activeMu struct {
		syncutil.RWMutex
		active innerRSEngine
		// pendingEngine is non-nil iff PrepareExternalManifest has opened a new
		// engine. pendingPrepared is true iff PrepareExternalManifest has
		// successfully called prepareExternalManifest on active. At most one of
		// these is true.
		pendingEngine   innerRSEngine
		pendingPrepared bool
		quiesced        []innerRSEngine
	}
	// mu protects container-level ref counting.
	mu struct {
		syncutil.Mutex
		cond *sync.Cond
		refs int
	}
	// shMu holds shared fields that can be read under either activeMu or mu, and
	// require both mutexes for modification (activeMu < mu).
	shMu struct {
		closing bool
	}
	closeWG sync.WaitGroup
}

var _ RSEngine = (*rsEngineContainer)(nil)

// makeInnerRSEngineOptions builds InnerRSEngineOptions.
func (opts *RSEngineOptions) makeInnerRSEngineOptions() InnerRSEngineOptions {
	return InnerRSEngineOptions{
		manifestChangeCommitter: opts.ManifestChangeCommitter,
		basaltFS:                opts.BasaltFS,
		basaltDir:               opts.BasaltDir,
		basaltScratchPathDir:    opts.BasaltScratchPathDir,
		logCtx:                  opts.LogCtx,
	}
}

// OpenRSEngine creates a new rsEngineContainer, opening the initial underlying
// engine at manifestNum.
func OpenRSEngine(manifestNum DiskFileNum, opts RSEngineOptions) (RSEngine, error) {
	innerOpts := opts.makeInnerRSEngineOptions()
	openInnerRSEngine := OpenInnerRSEngine
	if opts.TestingOpenRSEngineFunc != nil {
		openInnerRSEngine = opts.TestingOpenRSEngineFunc
	}
	initial, err := openInnerRSEngine(manifestNum, innerOpts)
	if err != nil {
		return nil, err
	}
	c := &rsEngineContainer{
		opts:       opts,
		openEngine: openInnerRSEngine,
		stopper:    opts.Stopper,
	}
	c.activeMu.active = initial
	c.mu.cond = sync.NewCond(&c.mu.Mutex)
	return c, nil
}

// acquireActive loads the active engine under activeMu.RLock and refs it. The
// caller must call engine.unref() when done. Used by long operations to delay
// the returned engine from completing closeInner.
func (c *rsEngineContainer) acquireActive() innerRSEngine {
	c.activeMu.RLock()
	defer c.activeMu.RUnlock()
	active := c.activeMu.active
	active.ref()
	return active
}

// CurrentManifestNum implements RSEngine.
func (c *rsEngineContainer) CurrentManifestNum() DiskFileNum {
	// Fast, hence hold activeMu.
	c.activeMu.RLock()
	defer c.activeMu.RUnlock()
	return c.activeMu.active.currentManifestNum()
}

// CompactionToggle implements RSEngine.
func (c *rsEngineContainer) CompactionToggle(enable bool) {
	// Fast, hence hold activeMu.
	c.activeMu.RLock()
	defer c.activeMu.RUnlock()
	// NB: we don't care if another RSEngine becomes active and starts doing
	// compactions (in the case enable is false) since this disabling is
	// best-effort.
	c.activeMu.active.compactionToggle(enable)
}

// WaitForOngoingManifestChanges implements RSEngine.
func (c *rsEngineContainer) WaitForOngoingManifestChanges() {
	active := c.acquireActive()
	defer active.unref()
	// NB: we don't care if another RSEngine becomes active and starts doing
	// manifest changes since this wait is best-effort.
	active.waitForOngoingManifestChanges()
}

// FlushSSTables implements RSEngine.
func (c *rsEngineContainer) FlushSSTables(
	scratchNames []string, flushCommit *FlushCommitInfo,
) error {
	active := c.acquireActive()
	defer active.unref()
	return active.flushSSTables(scratchNames, flushCommit)
}

// AddSSTables implements RSEngine.
func (c *rsEngineContainer) AddSSTables(scratchNames []string) error {
	active := c.acquireActive()
	defer active.unref()
	return active.addSSTables(scratchNames)
}

// Ref implements RSEngine.
func (c *rsEngineContainer) Ref() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.shMu.closing {
		panic(errors.AssertionFailedf("Ref called after Close has started"))
	}
	c.mu.refs++
}

// Unref implements RSEngine.
func (c *rsEngineContainer) Unref() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.refs--
	if c.mu.refs < 0 {
		panic(errors.AssertionFailedf("rsEngineContainer: externalRefs went negative"))
	}
	c.mu.cond.Signal()
}

// NewSnapshot implements RSEngine.
func (c *rsEngineContainer) NewSnapshot() RSEngineSnapshot {
	// Fast, hence hold activeMu.
	c.activeMu.RLock()
	defer c.activeMu.RUnlock()
	return c.activeMu.active.newSnapshot()
}

// PrepareExternalManifest implements RSEngine.
func (c *rsEngineContainer) PrepareExternalManifest(manifestNum DiskFileNum) error {
	var active innerRSEngine
	var err error
	func() {
		c.activeMu.RLock()
		defer c.activeMu.RUnlock()
		if c.shMu.closing {
			err = errors.AssertionFailedf("PrepareExternalManifest called after Close has started")
			return
		}
		if c.activeMu.pendingEngine != nil || c.activeMu.pendingPrepared {
			panic(errors.AssertionFailedf("rsEngineContainer: PrepareExternalManifest called with pending"))
		}
		active = c.activeMu.active
		active.ref()
	}()
	if err != nil {
		return err
	}
	defer active.unref()
	err = active.prepareExternalManifest(manifestNum)
	if err == nil {
		// Fast path.
		c.activeMu.Lock()
		c.activeMu.pendingPrepared = true
		c.activeMu.Unlock()
		return nil
	}
	// Slow path -- open a new inner engine.
	newOpts := c.opts.makeInnerRSEngineOptions()
	newEngine, err := c.openEngine(manifestNum, newOpts)
	if err != nil {
		return err
	}
	// TODO(sumeer): we are calling quiesce after opening the new engine, which is
	// not what we claimed we would do. It happens to be harmless since we know
	// two active engines can co-exist since they touch different files (due to
	// GetFileNums and different scratch dirs), but this isn't ideal. Given that
	// PrepareExternalManifest is happening in state machine application, there is
	// no way for CockroachDB to recover from an error here, in that the range
	// needs to be become unavailable. So we could quiesce first.
	active.quiesce()
	c.activeMu.Lock()
	c.activeMu.pendingEngine = newEngine
	c.activeMu.Unlock()
	return nil
}

// InstallPreparedManifest implements RSEngine.
func (c *rsEngineContainer) InstallPreparedManifest(manifestNum DiskFileNum) {
	var oldEngine innerRSEngine
	func() {
		c.activeMu.Lock()
		defer c.activeMu.Unlock()
		if c.shMu.closing {
			panic(errors.AssertionFailedf("InstallPreparedManifest called after Close has started"))
		}
		if c.activeMu.pendingEngine == nil && !c.activeMu.pendingPrepared {
			panic(errors.AssertionFailedf("rsEngineContainer: InstallPreparedManifest called without pending"))
		}
		if c.activeMu.pendingPrepared {
			c.activeMu.active.installPreparedManifest(manifestNum)
			c.activeMu.pendingPrepared = false
			return
		}
		// Make the pendingEngine active, and add the old active engine to the
		// quiesced list and closed asynchronously via the Stopper.
		oldEngine = c.activeMu.active
		c.activeMu.active = c.activeMu.pendingEngine
		c.activeMu.pendingEngine = nil
		c.activeMu.quiesced = append(c.activeMu.quiesced, oldEngine)
	}()
	if oldEngine == nil {
		return
	}
	c.closeWG.Add(1)
	ctx := c.opts.LogCtx
	if err := c.stopper.RunAsyncTask(ctx, "close-quiesced-rsengine", func(ctx context.Context) {
		defer c.closeWG.Done()
		oldEngine.closeInner()
		c.removeQuiesced(oldEngine)
	}); err != nil {
		// TODO(sumeer): Revisit. We don't want to just leak an innerRSEngine. If
		// handling this gracefully, also call c.closeWG.Done().
		panic(err)
	}
}

// removeQuiesced removes a closed engine from the quiesced list. When the list
// becomes empty, it calls enableUnreferencedFileDeletion on the active engine.
func (c *rsEngineContainer) removeQuiesced(engine innerRSEngine) {
	activeToEnableDeletion := func() innerRSEngine {
		c.activeMu.Lock()
		defer c.activeMu.Unlock()
		n := len(c.activeMu.quiesced)
		i := 0
		for i < n {
			if c.activeMu.quiesced[i] == engine {
				break
			}
			i++
		}
		if i == n {
			panic(errors.AssertionFailedf("removeQuiesced: engine not found"))
		}
		c.activeMu.quiesced[i], c.activeMu.quiesced[n-1] = c.activeMu.quiesced[n-1], c.activeMu.quiesced[i]
		c.activeMu.quiesced = c.activeMu.quiesced[:n-1]
		if n > 1 {
			return nil
		}
		active := c.activeMu.active
		if active != nil {
			active.ref()
		}
		return active
	}()
	if activeToEnableDeletion != nil {
		// NB: it doesn't matter if the active changes after activeMu is released
		// and before we call enableUnreferencedFileDeletion. This is the inner
		// engine that can safely do deletion.
		activeToEnableDeletion.enableUnreferencedFileDeletion()
		activeToEnableDeletion.unref()
	}
}

// Close implements RSEngine. It closes the container and all engines. Waits for
// container-level refs to drain, then quiesces and closes the active and
// pending engine, and waits for all quiesced engine close goroutines to finish.
func (c *rsEngineContainer) Close() {
	func() {
		c.activeMu.Lock()
		defer c.activeMu.Unlock()
		c.mu.Lock()
		defer c.mu.Unlock()
		c.shMu.closing = true
	}()
	func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for c.mu.refs > 0 {
			c.mu.cond.Wait()
		}
	}()
	var active innerRSEngine
	var pending innerRSEngine
	func() {
		c.activeMu.Lock()
		defer c.activeMu.Unlock()
		active = c.activeMu.active
		c.activeMu.active = nil
		pending = c.activeMu.pendingEngine
		c.activeMu.pendingEngine = nil
		c.activeMu.pendingPrepared = false
	}()
	if active != nil {
		active.quiesce()
		active.closeInner()
	}
	if pending != nil {
		pending.quiesce()
		pending.closeInner()
	}
	c.closeWG.Wait()
}

// TestingInnerEngine returns the active underlying innerRSEngine. For use
// in tests that need to inspect engine internals (e.g. PrintTestingRSEngineState).
func (c *rsEngineContainer) TestingInnerEngine() innerRSEngine {
	c.activeMu.RLock()
	defer c.activeMu.RUnlock()
	return c.activeMu.active
}

// String returns a debug string describing the container's state.
func (c *rsEngineContainer) String() string {
	var activeManifest DiskFileNum
	var hasPendingEngine, hasPendingPrepared bool
	var numQuiesced int
	var closing bool
	func() {
		c.activeMu.RLock()
		defer c.activeMu.RUnlock()
		activeManifest = c.activeMu.active.currentManifestNum()
		hasPendingEngine = c.activeMu.pendingEngine != nil
		hasPendingPrepared = c.activeMu.pendingPrepared
		numQuiesced = len(c.activeMu.quiesced)
		closing = c.shMu.closing
	}()
	c.mu.Lock()
	refs := c.mu.refs
	c.mu.Unlock()
	return fmt.Sprintf(
		"rsEngineContainer{active=%d, pending=(eng:%v,prep:%v), quiesced=%d, refs=%d, closing=%v}",
		activeManifest, hasPendingEngine, hasPendingPrepared, numQuiesced, refs, closing)
}
