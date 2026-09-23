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

import "context"

// Iterator represents a simple iterator for testing.
type Iterator struct{}

func (it *Iterator) Next(ctx context.Context) (bool, error) {
	return false, nil
}

func (it *Iterator) Value() int {
	return 0
}

// Scanner represents a scanner for testing.
type Scanner struct{}

func (s *Scanner) Scan() (bool, error) {
	return false, nil
}

func (s *Scanner) Err() error {
	return nil
}

// UncheckedIteratorError - this should be flagged.
func UncheckedIteratorError(ctx context.Context) ([]int, error) {
	it := &Iterator{}
	var results []int
	var ok bool
	var err error
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		results = append(results, it.Value())
	}
	return results, nil // want `return after iterator loop without checking error`
}

// UncheckedWithShortDecl - this should be flagged.
func UncheckedWithShortDecl(ctx context.Context) ([]int, error) {
	it := &Iterator{}
	var results []int
	for ok, err := it.Next(ctx); ok; ok, err = it.Next(ctx) {
		_ = err // use err to avoid unused variable error
		results = append(results, it.Value())
	}
	return results, nil // want `return after iterator loop without checking error`
}

// ProperlyCheckedError - this should NOT be flagged.
func ProperlyCheckedError(ctx context.Context) ([]int, error) {
	it := &Iterator{}
	var results []int
	var ok bool
	var err error
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		results = append(results, it.Value())
	}
	if err != nil {
		return nil, err
	}
	return results, nil
}

// ErrorReturned - this should NOT be flagged.
func ErrorReturned(ctx context.Context) ([]int, error) {
	it := &Iterator{}
	var results []int
	var ok bool
	var err error
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		results = append(results, it.Value())
	}
	return results, err // error is returned
}

// ErrorPassedToFunction - this should NOT be flagged.
func ErrorPassedToFunction(ctx context.Context) ([]int, error) {
	it := &Iterator{}
	var results []int
	var ok bool
	var err error
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		results = append(results, it.Value())
	}
	logError(err)
	return results, nil
}

func logError(err error) {
	_ = err
}

// NolintComment - this should NOT be flagged.
func NolintComment(ctx context.Context) ([]int, error) {
	it := &Iterator{}
	var results []int
	var ok bool
	var err error
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		results = append(results, it.Value())
	}
	//nolint:iteratorerr
	return results, nil
}

// ScannerPattern - this should be flagged.
func ScannerPattern(ctx context.Context) ([]int, error) {
	s := &Scanner{}
	var results []int
	var ok bool
	var err error
	for ok, err = s.Scan(); ok; ok, err = s.Scan() {
		results = append(results, 1)
	}
	return results, nil // want `return after iterator loop without checking error`
}

// ScannerWithErrCheck - this should NOT be flagged.
func ScannerWithErrCheck(ctx context.Context) ([]int, error) {
	s := &Scanner{}
	var results []int
	var ok bool
	var err error
	for ok, err = s.Scan(); ok; ok, err = s.Scan() {
		results = append(results, 1)
	}
	if err != nil {
		return nil, err
	}
	return results, nil
}

// Use context to avoid import errors.
var _ = context.Background
