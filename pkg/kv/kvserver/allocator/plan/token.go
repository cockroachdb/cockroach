// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package plan

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// AllocatorToken is a token which provides mutual exclusion for allocator
// execution. When the token is acquired, other acquirers will fail until
// release.
//
// The leaseholder replica should acquire an allocator token before beginning
// replica or lease changes on a range. After the changes have
// failed/succeeded, the token should be released. The goal is to limit the
// amount of concurrent reshuffling activity of a range.
type AllocatorToken struct {
	mu struct {
		syncutil.Mutex
		acquired     bool
		acquiredName string
	}
}

// TryAcquire tries to acquire the token, returning nil if successful and an
// ErrAllocatorToken otherwise.
func (at *AllocatorToken) TryAcquire(ctx context.Context, name string) error {
	at.mu.Lock()
	defer at.mu.Unlock()

	if !at.mu.acquired {
		at.mu.acquired = true
		at.mu.acquiredName = name
		log.KvDistribution.VEventf(ctx, 3, "acquired allocator token")
		return nil
	}

	return NewErrAllocatorToken(at.mu.acquiredName)
}

// Release releases the AllocatorToken, it may now be acquired.
func (at *AllocatorToken) Release(ctx context.Context) {
	at.mu.Lock()
	defer at.mu.Unlock()

	if !at.mu.acquired {
		panic("expected allocator token to be held before release")
	}

	at.mu.acquired = false
	at.mu.acquiredName = ""
	log.KvDistribution.VEventf(ctx, 3, "released allocator token")
}

// ErrAllocatorToken is an error returned when an allocator token acquisition
// fails. It indicates that the replica should be sent to purgatory, to retry
// the acquisition and operation later.
type ErrAllocatorToken struct {
	holder string
}

var _ errors.SafeFormatter = ErrAllocatorToken{}

// NewErrAllocatorToken returns a new ErrAllocatorToken with the holder
// initialized to the argument supplied.
func NewErrAllocatorToken(holder string) ErrAllocatorToken {
	return ErrAllocatorToken{holder: holder}
}

func (e ErrAllocatorToken) Error() string { return fmt.Sprint(e) }

func (e ErrAllocatorToken) Format(s fmt.State, verb rune) {
	errors.FormatError(e, s, verb)
}

func (e ErrAllocatorToken) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("can't acquire allocator token, held by %s", e.holder)
	return nil
}

func (ErrAllocatorToken) PurgatoryErrorMarker() {}
