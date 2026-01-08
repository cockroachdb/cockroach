// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fingerprint

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/stretchr/testify/require"
)

func sp(k byte) roachpb.Span {
	return roachpb.Span{Key: roachpb.Key([]byte{k}), EndKey: roachpb.Key([]byte{k + 1})}
}

// testPersister is a configurable in-memory persister.
type testPersister struct {
	state            *checkpointState
	lastFrac         float64
	loadErr, saveErr error
	onStore          func()
}

func (p *testPersister) load(ctx context.Context) (checkpointState, bool, error) {
	if p.loadErr != nil {
		return checkpointState{}, false, p.loadErr
	}
	if p.state == nil {
		return checkpointState{}, false, nil
	}
	return *p.state, true, nil
}

func (p *testPersister) store(ctx context.Context, state checkpointState, frac float64) error {
	if p.saveErr != nil {
		return p.saveErr
	}
	p.state, p.lastFrac = &state, frac
	if p.onStore != nil {
		p.onStore()
	}
	return nil
}

// testPartitioner distributes spans across n partitions (default 1).
type testPartitioner struct {
	n   int
	err error
}

func (p testPartitioner) partition(
	ctx context.Context, spans []roachpb.Span,
) ([]sql.SpanPartition, error) {
	if p.err != nil {
		return nil, p.err
	}
	if len(spans) == 0 {
		return nil, nil
	}
	n := p.n
	if n <= 0 {
		n = 1
	}
	parts := make([]sql.SpanPartition, n)
	for i, sp := range spans {
		parts[i%n].Spans = append(parts[i%n].Spans, sp)
	}
	var result []sql.SpanPartition
	for _, part := range parts {
		if len(part.Spans) > 0 {
			result = append(result, part)
		}
	}
	return result, nil
}

// makeFingerprinter creates a fingerprinter. Returns fp XOR'd with first key byte
// so each span produces a distinct fingerprint.
func makeFingerprinter(fp uint64, err error) spanFingerprinter {
	return func(ctx context.Context, sp roachpb.Span) (spanFingerprintResult, error) {
		if err != nil {
			return spanFingerprintResult{}, err
		}
		return spanFingerprintResult{span: sp, fingerprint: fp ^ uint64(sp.Key[0])}, nil
	}
}

func newGatherer(
	spans []roachpb.Span, p persister, part partitioner, fn spanFingerprinter,
) *gatherer {
	return &gatherer{
		spans: spans, persister: p, partitioner: part, fn: fn,
		chkptFreq: func() time.Duration { return time.Hour },
	}
}

func TestGatherer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	t.Run("basic", func(t *testing.T) {
		spans := []roachpb.Span{sp('a'), sp('c'), sp('e')}
		g := newGatherer(spans, &testPersister{}, testPartitioner{}, makeFingerprinter(0, nil))
		result, err := g.run(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64('a'^'c'^'e'), result) // Each span contributes its key byte
	})

	t.Run("resume", func(t *testing.T) {
		spans := []roachpb.Span{sp('a'), sp('c')}
		done, _ := span.MakeFrontier(spans...)
		_, _ = done.Forward(sp('a'), hlc.Timestamp{WallTime: 1})
		p := &testPersister{state: &checkpointState{fingerprint: 100, done: done}}
		g := newGatherer(spans, p, testPartitioner{}, makeFingerprinter(0, nil))
		result, err := g.run(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64(100^'c'), result) // Only c-d processed
	})
}

func TestGathererCheckpointing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// Since time only advances in a synctest bubble when all goroutines are
	// "durably blocked", we use time.Sleep in the fingerprinter to allow the
	// checkpoint ticker to fire between span completions.
	//
	// Each fingerprint fn() "sleeps" a unique duration, determined by multiplying
	// its by 10, while the checkpoint ticker is 17 to make it coprime. This means
	// each span is processed and checkpoint occurs at a distinct point on the
	// timeline ensuring the test's behavior is deterministic.
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		spans := []roachpb.Span{sp('a'), sp('c'), sp('e'), sp('g'), sp('i'), sp('k')}

		var checkpoints int
		p := &testPersister{onStore: func() { checkpoints++ }}

		g := &gatherer{
			spans:       spans,
			persister:   p,
			partitioner: testPartitioner{n: 3},
			fn: func(ctx context.Context, s roachpb.Span) (spanFingerprintResult, error) {
				fp := uint64(s.Key[0])
				time.Sleep(time.Duration((s.Key[0]-'a')+1) * 10)
				return spanFingerprintResult{span: s, fingerprint: fp}, nil
			},
			chkptFreq: func() time.Duration { return 17 },
		}

		result, err := g.run(ctx)
		require.NoError(t, err)
		require.Equal(t, uint64('a'^'c'^'e'^'g'^'i'^'k'), result)
		// Checkpoints at T=17, 34, 51, 68, 85, 102, 119, 136, 153 = 9 total.
		require.Equal(t, 9, checkpoints)
		// Last checkpoint at T=153 has 5/6 spans done.
		require.Equal(t, 5.0/6.0, p.lastFrac)
	})
}

func TestGathererCheckpointPreservation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// This test verifies that checkpointed progress is preserved when an error
	// occurs. Specifically, we want to ensure that spans completed before a
	// checkpoint are not re-processed when the gatherer is resumed after an error.
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		spans := []roachpb.Span{sp('a'), sp('c'), sp('e'), sp('g'), sp('i'), sp('k')}

		// Track which spans have been processed across all runs.
		processedSpans := make(map[byte]int)

		p := &testPersister{}

		// First run: process a, c, e successfully, then fail on g.
		g := &gatherer{
			spans:       spans,
			persister:   p,
			partitioner: testPartitioner{n: 1}, // Single worker for deterministic ordering
			fn: func(ctx context.Context, s roachpb.Span) (spanFingerprintResult, error) {
				key := s.Key[0]
				processedSpans[key]++
				fp := uint64(key)
				// Sleep to allow checkpointing to occur between spans.
				// Spans sleep: a=10, c=30, e=50, g=70, i=90, k=110
				time.Sleep(time.Duration((key-'a')+1) * 10)
				// Fail when processing 'g'
				if key == 'g' {
					return spanFingerprintResult{}, context.DeadlineExceeded
				}
				return spanFingerprintResult{span: s, fingerprint: fp}, nil
			},
			// Checkpoint at T=25, 50, 75, ...
			// This means we checkpoint after 'a' (T=10) at T=25, and after 'c' (T=40) at T=50.
			chkptFreq: func() time.Duration { return 25 },
		}

		_, err := g.run(ctx)
		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)

		// Verify that a checkpoint occurred and saved progress.
		require.NotNil(t, p.state)
		require.NotNil(t, p.state.done)

		// Verify that at least 'a' was checkpointed (done frontier is at WallTime >= 1).
		var aCheckpointed bool
		for span, ts := range p.state.done.Entries() {
			if span.Equal(sp('a')) && !ts.Less(hlc.Timestamp{WallTime: 1}) {
				aCheckpointed = true
				break
			}
		}
		require.True(t, aCheckpointed, "span 'a' should be checkpointed")

		// Track which spans were processed in the first run.
		firstRunProcessed := make(map[byte]bool)
		for key := range processedSpans {
			firstRunProcessed[key] = true
		}

		// Verify that a, c, e, g were processed in the first run.
		require.Equal(t, 1, processedSpans['a'])
		require.Equal(t, 1, processedSpans['c'])
		require.Equal(t, 1, processedSpans['e'])
		require.Equal(t, 1, processedSpans['g']) // Failed here

		// Second run: resume from checkpoint, should only process remaining spans.
		g2 := &gatherer{
			spans:       spans,
			persister:   p, // Reuse persister with checkpoint
			partitioner: testPartitioner{n: 1},
			fn: func(ctx context.Context, s roachpb.Span) (spanFingerprintResult, error) {
				key := s.Key[0]
				processedSpans[key]++
				fp := uint64(key)
				time.Sleep(time.Duration((key-'a')+1) * 10)
				return spanFingerprintResult{span: s, fingerprint: fp}, nil
			},
			chkptFreq: func() time.Duration { return 25 },
		}

		result, err := g2.run(ctx)
		require.NoError(t, err)

		// Verify the final fingerprint is correct.
		require.Equal(t, uint64('a'^'c'^'e'^'g'^'i'^'k'), result)

		// Critical assertion: Determine which spans were checkpointed by seeing
		// which ones were NOT re-processed in the second run.
		var checkpointed []byte
		for key := range firstRunProcessed {
			if processedSpans[key] == 1 {
				checkpointed = append(checkpointed, key)
			}
		}

		// Verify that at least one span was checkpointed (preserved across runs).
		require.NotEmpty(t, checkpointed, "at least one span should have been checkpointed")

		// Verify that the span that failed ('g') was retried.
		require.Equal(t, 2, processedSpans['g'], "span 'g' should have been retried after failure")

		// Verify that spans not processed in first run were only processed in second run.
		require.Equal(t, 1, processedSpans['i']) // Only in second run
		require.Equal(t, 1, processedSpans['k']) // Only in second run

		// Verify that at least 'a' was checkpointed (we know from earlier assertion).
		require.Contains(t, checkpointed, byte('a'), "span 'a' should be checkpointed")
	})
}
