// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goroutines

import (
	"context"
	"math/rand"
	"net"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// Options controls the goroutine population and the kind of stacks/states.
type Options struct {
	Initial int
	Max     int

	// StackDepth is the legacy/fixed depth. If Zipf knobs below are not set,
	// each worker will use exactly this depth.
	// Values > 16 are capped to 16.
	StackDepth int

	// Zipf stack depth sampling (optional).
	//
	// Enable Zipf sampling by setting StackDepthMax > 0 or StackDepthZipfS > 0.
	// Each worker will pick a depth d in [StackDepthMin, StackDepthMax].
	//
	// Defaults when Zipf is enabled:
	//   Min: 0
	//   Max: StackDepth (if StackDepth>0) else 16
	//   S:   1.3
	//   V:   2
	//
	// Note: max is capped to 16 to match the withDepth implementation; increase
	// that if you extend withDepth beyond 16.
	StackDepthMin   int
	StackDepthMax   int
	StackDepthZipfS float64
	StackDepthZipfV float64
	StackDepthSeed  int64 // 0 => stable default (1); otherwise, set explicitly in tests.

	Mix Mix

	Name string

	SpawnBatch     int           // default 10k
	IOWaitMaxConns int           // default 2048 (concurrent client dials)
	TimerPeriod    time.Duration // default 10s
	CPUSleep       time.Duration // default 20ms
	CPUCap         int           // default GOMAXPROCS
}

type Mix struct {
	CPU    int
	Mutex  int
	IO     int
	Chan   int
	Timer  int
	Select int // blocked in a multi-way select loop
	Cond   int // blocked in sync.Cond.Wait
}

type Handle struct {
	opts    Options
	stopper *stop.Stopper

	started atomic.Bool
	stopped atomic.Bool

	// startedCount is total workers started (monotonic).
	startedCount atomic.Int64
	// runningCount is current number of worker goroutines still alive.
	runningCount atomic.Int64

	// Cancellation + coordinated shutdown.
	ctx          context.Context
	cancel       context.CancelFunc
	shutdownOnce sync.Once
	stopCh       chan struct{}

	// Growth: lossless + coalesced. GrowBy adds to pending and pokes the spawner.
	pending atomic.Int64
	pokeCh  chan struct{}

	// Wait for everything we start via ctxgroup.
	g ctxgroup.Group

	cpuSem chan struct{}
	sink   uint64 // prevents dead-code elimination

	// Blocked-state primitives.
	blockMu sync.Mutex // held while running to make mutex waiters block

	// ch is used for channel waiters; closed on shutdown.
	ch chan struct{}

	// select wait: closed on shutdown (two channels to give stacks more variety).
	selCh1 chan struct{}
	selCh2 chan struct{}

	// cond wait infra.
	condMu   sync.Mutex
	cond     *sync.Cond
	condDone bool

	// IO wait infra (netpoll TCP), capped.
	ln      net.Listener
	ioMu    sync.Mutex
	ioConns []net.Conn

	ioClientMu sync.Mutex
	ioClients  []net.Conn

	ioCount atomic.Int64 // current open client dials (best-effort)
}

var DefaultMix = Mix{Chan: 3, Mutex: 3, IO: 3, Timer: 1, Select: 2, Cond: 2, CPU: 1}

func Start(parent context.Context, stopper *stop.Stopper, opts Options) (*Handle, error) {
	// Defaults/sanitize.
	if opts.Initial < 0 {
		opts.Initial = 0
	}
	if opts.Max <= 0 {
		opts.Max = opts.Initial
	}
	if opts.Max < opts.Initial {
		opts.Max = opts.Initial
	}
	if opts.Mix == (Mix{}) {
		opts.Mix = DefaultMix
	}
	if opts.SpawnBatch <= 0 {
		opts.SpawnBatch = 10_000
	}
	if opts.IOWaitMaxConns <= 0 {
		opts.IOWaitMaxConns = 2048
	}
	if opts.TimerPeriod <= 0 {
		opts.TimerPeriod = 10 * time.Second
	}
	if opts.CPUSleep <= 0 {
		opts.CPUSleep = 20 * time.Millisecond
	}
	if opts.CPUCap <= 0 {
		opts.CPUCap = runtime.GOMAXPROCS(0)
	}

	// Cap the legacy fixed depth.
	if opts.StackDepth > 16 {
		opts.StackDepth = 16
	}
	if opts.StackDepth < 0 {
		opts.StackDepth = 0
	}

	// Zipf defaults and caps.
	zipfEnabled := opts.StackDepthMax > 0 || opts.StackDepthZipfS > 0 || opts.StackDepthZipfV > 0 || opts.StackDepthMin > 0
	if zipfEnabled {
		if opts.StackDepthMin < 0 {
			opts.StackDepthMin = 0
		}
		if opts.StackDepthMax <= 0 {
			if opts.StackDepth > 0 {
				opts.StackDepthMax = opts.StackDepth
			} else {
				opts.StackDepthMax = 16
			}
		}
		if opts.StackDepthMax > 16 {
			opts.StackDepthMax = 16
		}
		if opts.StackDepthMin > opts.StackDepthMax {
			opts.StackDepthMin = opts.StackDepthMax
		}
		if opts.StackDepthZipfS <= 0 {
			opts.StackDepthZipfS = 1.3
		}
		if opts.StackDepthZipfV <= 0 {
			opts.StackDepthZipfV = 2
		}
	}
	if opts.StackDepthSeed == 0 {
		// Intentionally stable default (helps tests unless you want randomness).
		opts.StackDepthSeed = 1
	}

	ctx, cancel := context.WithCancel(parent)
	h := &Handle{
		opts:    opts,
		stopper: stopper,
		ctx:     ctx,
		cancel:  cancel,
		stopCh:  make(chan struct{}),
		pokeCh:  make(chan struct{}, 1),

		ch:     make(chan struct{}),
		selCh1: make(chan struct{}),
		selCh2: make(chan struct{}),

		cpuSem: make(chan struct{}, opts.CPUCap),
	}
	h.cond = sync.NewCond(&h.condMu)
	h.g = ctxgroup.WithContext(h.ctx)

	// Hold blockMu while running so mutex waiters actually block in Lock().
	if opts.Mix.Mutex > 0 {
		h.blockMu.Lock()
		h.g.GoCtx(func(ctx context.Context) error {
			select {
			case <-h.stopCh:
			case <-ctx.Done():
			}
			h.blockMu.Unlock()
			return nil
		})
	}

	// Set up IO wait listener + accept loop (best-effort).
	if opts.Mix.IO > 0 {
		if ln, err := net.Listen("tcp", "127.0.0.1:0"); err == nil {
			h.ln = ln
			h.g.GoCtx(func(ctx context.Context) error {
				for {
					c, err := ln.Accept()
					if err != nil {
						// If we are stopping, this is expected.
						if h.stopped.Load() || ctx.Err() != nil {
							return nil
						}
						// On Go 1.20+ net.ErrClosed is common.
						if errors.Is(err, net.ErrClosed) {
							return nil
						}
						return errors.Newf("goroutinemix(%s): failed to accept IO mix conn: %v", h.opts.Name, err)
					}
					// Bound storage even if someone changes dial caps later.
					h.ioMu.Lock()
					if len(h.ioConns) >= h.opts.IOWaitMaxConns {
						h.ioMu.Unlock()
						_ = c.Close()
						// N.B. we'll accept and close excess conns, but dialers will stop dialing once they hit their cap.
						continue
					}
					h.ioConns = append(h.ioConns, c)
					h.ioMu.Unlock()

					select {
					case <-h.stopCh:
						return nil
					case <-ctx.Done():
						return nil
					default:
					}
				}
			})
		} else {
			return nil, errors.Newf("goroutinemix(%s): failed to listen for IO mix: %v", h.opts.Name, err)
		}
	}

	// Spawner: wakes on poke and drains pending (lossless & coalesced).
	h.g.GoCtx(func(ctx context.Context) error {
		for {
			select {
			case <-h.pokeCh:
				delta := int(h.pending.Swap(0))
				if delta > 0 {
					h.spawnDelta(delta)
				}
			case <-h.stopCh:
				return nil
			case <-ctx.Done():
				return nil
			}
		}
	})

	// Trigger shutdown immediately on quiesce, *before* stopper waits.
	_ = stopper.RunAsyncTask(parent, "goroutinemix-quiesce-shutdown", func(ctx context.Context) {
		<-stopper.ShouldQuiesce()
		h.shutdown()
	})

	// Closer: shutdown (idempotent) and wait for ctxgroup goroutines to exit.
	stopper.AddCloser(stop.CloserFn(func() {
		h.shutdown()
		if err := h.g.Wait(); err != nil && !(errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
			// Besides context cancellation, we expect no errors.
			panic(err)
		}
	}))

	h.started.Store(true)

	// Request initial spawn asynchronously (does not block Activate()).
	h.GrowBy(opts.Initial)
	return h, nil
}

func (h *Handle) Started() int { return int(h.startedCount.Load()) }
func (h *Handle) Running() int { return int(h.runningCount.Load()) }

// Double attempts to double the current running goroutines up to Max.
func (h *Handle) Double() {
	if !h.started.Load() || h.stopped.Load() {
		return
	}
	cur := h.Running()
	if cur == 0 {
		cur = 1
	}
	target := cur * 2
	if target > h.opts.Max {
		target = h.opts.Max
	}
	h.GrowBy(target - cur)
}

// GrowBy requests n more workers; lossless and coalesced.
func (h *Handle) GrowBy(n int) {
	if n <= 0 || !h.started.Load() || h.stopped.Load() {
		return
	}

	// Respect Max using current running. Even if we overshoot briefly,
	// spawnDelta re-checks Max before starting workers.
	cur := h.Running()
	if cur >= h.opts.Max {
		return
	}
	if cur+n > h.opts.Max {
		n = h.opts.Max - cur
	}
	if n <= 0 {
		return
	}

	h.pending.Add(int64(n))
	select {
	case h.pokeCh <- struct{}{}:
	default:
		// already poked
	}
}

func (h *Handle) shutdown() {
	h.shutdownOnce.Do(func() {
		h.stopped.Store(true)

		// Stop signals/cancellation first.
		close(h.stopCh)
		h.cancel()

		// Unblock channel/select waiters.
		close(h.ch)
		close(h.selCh1)
		close(h.selCh2)

		// Unblock cond waiters.
		h.condMu.Lock()
		h.condDone = true
		h.cond.Broadcast()
		h.condMu.Unlock()

		// Close listener (unblocks Accept).
		if h.ln != nil {
			_ = h.ln.Close()
		}
		// Close client-side conns (unblocks worker Read()).
		h.ioClientMu.Lock()
		clients := h.ioClients
		h.ioClients = nil
		h.ioClientMu.Unlock()
		for _, c := range clients {
			_ = c.Close()
		}

		// Close server-side accepted conns (best effort).
		h.ioMu.Lock()
		conns := h.ioConns
		h.ioConns = nil
		h.ioMu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
}

func (h *Handle) spawnDelta(delta int) {
	if delta <= 0 || h.stopped.Load() {
		return
	}

	cur := h.Running()
	if cur >= h.opts.Max {
		return
	}
	if cur+delta > h.opts.Max {
		delta = h.opts.Max - cur
	}
	if delta <= 0 {
		return
	}
	for delta > 0 && !h.stopped.Load() {
		b := h.opts.SpawnBatch
		if delta < b {
			b = delta
		}

		base := int(h.startedCount.Load())
		depthSampler := h.newDepthSampler(int64(base))

		for i := 0; i < b; i++ {
			k := h.pickKind(base + i)
			d := depthSampler()
			h.startWorker(k, d)
		}

		delta -= b
	}
}

// newDepthSampler returns a per-batch sampler to avoid global RNG contention.
// base should be something that changes across batches (e.g. startedCount).
func (h *Handle) newDepthSampler(base int64) func() int {
	zipfEnabled := h.opts.StackDepthMax > 0 || h.opts.StackDepthZipfS > 0 || h.opts.StackDepthZipfV > 0 || h.opts.StackDepthMin > 0
	if !zipfEnabled {
		d := h.opts.StackDepth
		return func() int { return d }
	}

	minD := h.opts.StackDepthMin
	maxD := h.opts.StackDepthMax
	if maxD < minD {
		maxD = minD
	}
	if maxD > 32 {
		maxD = 32
	}
	if minD < 0 {
		minD = 0
	}

	width := maxD - minD
	if width <= 0 {
		return func() int { return minD }
	}

	// Mix seed with base so batches differ without needing shared state.
	seed := h.opts.StackDepthSeed ^ (base * 0x9e3779b97f4a7c)
	r := rand.New(rand.NewSource(seed))
	z := rand.NewZipf(r, h.opts.StackDepthZipfS, h.opts.StackDepthZipfV, uint64(width))

	return func() int {
		return minD + int(z.Uint64())
	}
}

type kind int

const (
	kindCPU kind = iota
	kindMutex
	kindIO
	kindChan
	kindTimer
	kindSelect
	kindCond
)

func (h *Handle) pickKind(i int) kind {
	total := h.opts.Mix.Chan + h.opts.Mix.Mutex + h.opts.Mix.IO + h.opts.Mix.Timer + h.opts.Mix.Select + h.opts.Mix.Cond + h.opts.Mix.CPU
	if total <= 0 {
		return kindChan
	}
	x := i % total
	if x < h.opts.Mix.Chan {
		return kindChan
	}
	x -= h.opts.Mix.Chan
	if x < h.opts.Mix.Mutex {
		return kindMutex
	}
	x -= h.opts.Mix.Mutex
	if x < h.opts.Mix.IO {
		return kindIO
	}
	x -= h.opts.Mix.IO
	if x < h.opts.Mix.Timer {
		return kindTimer
	}
	x -= h.opts.Mix.Timer
	if x < h.opts.Mix.Select {
		return kindSelect
	}
	x -= h.opts.Mix.Select
	if x < h.opts.Mix.Cond {
		return kindCond
	}
	return kindCPU
}

func (h *Handle) startWorker(k kind, depth int) {
	h.startedCount.Add(1)
	h.runningCount.Add(1)

	h.g.GoCtx(func(ctx context.Context) error {
		defer h.runningCount.Add(-1)

		lbls := pprof.Labels(
			"goroutinemixer_kind", k.String(),
		)
		ctx = pprof.WithLabels(ctx, lbls)
		pprof.SetGoroutineLabels(ctx)

		withDepth(depth, func() {
			h.runKind(ctx, k)
		})
		return nil
	})
}

// withDepth creates deeper stacks using distinct frames up to depth 32.
func withDepth(d int, f func()) {
	switch {
	case d <= 0:
		f()
	case d == 1:
		depth01(f)
	case d == 2:
		depth02(f)
	case d == 3:
		depth03(f)
	case d == 4:
		depth04(f)
	case d == 5:
		depth05(f)
	case d == 6:
		depth06(f)
	case d == 7:
		depth07(f)
	case d == 8:
		depth08(f)
	case d == 9:
		depth09(f)
	case d == 10:
		depth10(f)
	case d == 11:
		depth11(f)
	case d == 12:
		depth12(f)
	case d == 13:
		depth13(f)
	case d == 14:
		depth14(f)
	case d == 15:
		depth15(f)
	default:
		depth16(f)
	}
}

func depth01(f func()) { f() }
func depth02(f func()) { depth01(f) }
func depth03(f func()) { depth02(f) }
func depth04(f func()) { depth03(f) }
func depth05(f func()) { depth04(f) }
func depth06(f func()) { depth05(f) }
func depth07(f func()) { depth06(f) }
func depth08(f func()) { depth07(f) }
func depth09(f func()) { depth08(f) }
func depth10(f func()) { depth09(f) }
func depth11(f func()) { depth10(f) }
func depth12(f func()) { depth11(f) }
func depth13(f func()) { depth12(f) }
func depth14(f func()) { depth13(f) }
func depth15(f func()) { depth14(f) }
func depth16(f func()) { depth15(f) }

func (h *Handle) runKind(ctx context.Context, k kind) {
	switch k {
	case kindCPU:
		// CPU-ish runnable goroutine: short burst + sleep.
		for {
			// Acquire token or stop.
			select {
			case h.cpuSem <- struct{}{}:
			case <-h.stopCh:
				return
			case <-ctx.Done():
				return
			}
			// Burn CPU for a short burst.
			var x uint64 = 0x9e3779b97f4a7c15
			const batch = 128
			for i := 0; i < batch; i++ {
				x ^= x >> 12
				x ^= x << 25
				x ^= x >> 27
				x *= 0x2545F4914F6CDD1D
			}
			atomic.AddUint64(&h.sink, x)
			// Release token.
			<-h.cpuSem
			time.Sleep(h.opts.CPUSleep)
		}

	case kindMutex:
		// True mutex waiters: block in Lock() until shutdown releases blockMu.
		h.blockMu.Lock()
		h.blockMu.Unlock()
		return

	case kindIO:
		c := h.dialIOConn()
		if c == nil {
			// If we can't acquire an FD, fallback to a different parked kind.
			h.runKind(ctx, kindChan)
			return
		}
		defer func() {
			_ = c.Close()
			h.ioCount.Add(-1)
		}()

		// Block in Read until shutdown closes listener/conns.
		buf := make([]byte, 1)
		_, _ = c.Read(buf)
		return

	case kindChan:
		select {
		case <-h.stopCh:
			return
		case <-ctx.Done():
			return
		case <-h.ch:
			return // closed on shutdown
		}

	case kindTimer:
		t := time.NewTimer(h.opts.TimerPeriod)
		defer t.Stop()
		for {
			select {
			case <-h.stopCh:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				t.Reset(h.opts.TimerPeriod)
			}
		}

	case kindSelect:
		// Multi-way select parking, similar to many CRDB goroutine loops.
		for {
			select {
			case <-h.stopCh:
				return
			case <-ctx.Done():
				return
			case <-h.selCh1:
				return
			case <-h.selCh2:
				return
			}
		}

	case kindCond:
		h.condMu.Lock()
		for !h.condDone {
			h.cond.Wait()
		}
		h.condMu.Unlock()
		return
	}
}

func (h *Handle) dialIOConn() net.Conn {
	if h.ln == nil || h.stopped.Load() {
		return nil
	}

	// Cap conns to avoid FD exhaustion at large scale.
	n := h.ioCount.Add(1)
	if int(n) > h.opts.IOWaitMaxConns {
		h.ioCount.Add(-1)
		return nil
	}
	d := net.Dialer{Timeout: 500 * time.Millisecond}
	c, err := d.Dial("tcp", h.ln.Addr().String())
	if err != nil {
		h.ioCount.Add(-1)
		return nil
	}
	// Track the *client* conn so shutdown can always unblock Read().
	h.ioClientMu.Lock()
	if h.stopped.Load() {
		h.ioClientMu.Unlock()
		_ = c.Close()
		h.ioCount.Add(-1)
		return nil
	}
	h.ioClients = append(h.ioClients, c)
	h.ioClientMu.Unlock()

	return c
}

func (k kind) String() string {
	switch k {
	case kindCPU:
		return "cpu"
	case kindMutex:
		return "mutex"
	case kindIO:
		return "io"
	case kindChan:
		return "chan"
	case kindTimer:
		return "timer"
	case kindSelect:
		return "select"
	case kindCond:
		return "cond"
	default:
		return "unknown"
	}
}
