// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package stats implements several statistical distributions,
// hypothesis tests, and functions for descriptive statistics.
//
// Currently stats is fairly small, but for what it does implement, it
// focuses on high quality, fast implementations with good, idiomatic
// Go APIs.
//
// This is a trimmed fork of github.com/aclements/go-moremath/stats.
package stats

import (
	"errors"
	"math"
)

var inf = math.Inf(1)
var nan = math.NaN()

// TODO: Put all errors in the same place and maybe unify them.

var (
	ErrSamplesEqual = errors.New("all samples are equal")
)
