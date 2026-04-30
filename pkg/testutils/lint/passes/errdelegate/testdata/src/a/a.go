// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package a

// OCFReader simulates a reader with an Err() method.
type OCFReader struct {
	err error
}

func (r *OCFReader) Read() ([]byte, error) {
	return nil, nil
}

func (r *OCFReader) Err() error {
	return r.err
}

// Scanner simulates a scanner with Err() method.
type Scanner struct {
	err error
}

func (s *Scanner) Scan() bool {
	return false
}

func (s *Scanner) Err() error {
	return s.err
}

// BadWrapper - this should be flagged because it doesn't delegate to underlying.Err().
type BadWrapper struct {
	underlying *OCFReader
	localErr   error
}

func (w *BadWrapper) Err() error { // want `Err\(\) method does not delegate to underlying.Err\(\)`
	return w.localErr // Only returns local error, ignores underlying.Err()
}

// GoodWrapper - this should NOT be flagged.
type GoodWrapper struct {
	underlying *OCFReader
	localErr   error
}

func (w *GoodWrapper) Err() error {
	if w.localErr != nil {
		return w.localErr
	}
	return w.underlying.Err() // Delegates to underlying
}

// CombinedWrapper - this should NOT be flagged.
type CombinedWrapper struct {
	underlying *OCFReader
	localErr   error
}

func (w *CombinedWrapper) Err() error {
	// Combines both errors
	if err := w.underlying.Err(); err != nil {
		return err
	}
	return w.localErr
}

// NoFieldWithErr - this should NOT be flagged (no field with Err()).
type NoFieldWithErr struct {
	data []byte
	err  error
}

func (n *NoFieldWithErr) Err() error {
	return n.err // No field has Err(), so this is fine
}

// NolintWrapper - this should NOT be flagged.
type NolintWrapper struct {
	underlying *OCFReader
	localErr   error
}

//nolint:errdelegate
func (w *NolintWrapper) Err() error {
	return w.localErr
}

// MultipleFields - should flag for each field not delegated.
type MultipleFields struct {
	reader  *OCFReader
	scanner *Scanner
	err     error
}

func (m *MultipleFields) Err() error { // want `Err\(\) method does not delegate to reader.Err\(\)` `Err\(\) method does not delegate to scanner.Err\(\)`
	return m.err
}

// PartialDelegation - should flag for the field not delegated.
type PartialDelegation struct {
	reader  *OCFReader
	scanner *Scanner
	err     error
}

func (p *PartialDelegation) Err() error { // want `Err\(\) method does not delegate to scanner.Err\(\)`
	if err := p.reader.Err(); err != nil {
		return err
	}
	return p.err
}

// FullDelegation - should NOT be flagged.
type FullDelegation struct {
	reader  *OCFReader
	scanner *Scanner
	err     error
}

func (f *FullDelegation) Err() error {
	if err := f.reader.Err(); err != nil {
		return err
	}
	if err := f.scanner.Err(); err != nil {
		return err
	}
	return f.err
}
