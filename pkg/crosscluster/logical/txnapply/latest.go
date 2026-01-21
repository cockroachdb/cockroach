// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnapply

// Latest is a utility that
type Latest[T any] struct {
	Chan  chan T
	input chan T
}

// MakeLatest creates a channel that emits only the most recent value that is
// set.
func MakeLatest[T any]() Latest[T] {
	input := make(chan T)
	Chan := make(chan T)
	go func() {
		defer close(Chan)
		for {
			var latest T
			latest, ok := <-input
			if !ok {
				return
			}

		delivered:
			for {
				select {
				case latest, ok = <-input:
					if !ok {
						return
					}
				case Chan <- latest:
					break delivered
				}
			}
		}
	}()
	return Latest[T]{Chan: Chan, input: input}
}

func (l *Latest[T]) Close() {
	close(l.input)
}

func (l *Latest[T]) Set(v T) {
	l.input <- v
}
