// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlsmith

func (s *Smither) coin() bool {
	return s.rnd.Intn(2) == 0
}

func (s *Smither) d6() int {
	return s.rnd.Intn(6) + 1
}

func (s *Smither) d9() int {
	return s.rnd.Intn(9) + 1
}

func (s *Smither) d100() int {
	return s.rnd.Intn(100) + 1
}

// geom returns a sample from a geometric distribution whose mean is 1 / (1 -
// p). Its return value is >= 1. p must satisfy 0 < p < 1. For example, pass
// .5 to this function (whose mean would thus be 2), and 50% of the time this
// function will return 1, 25% will return 2, 12.5% return 3, 6.25% will return
// 4, etc. See: https://en.wikipedia.org/wiki/Geometric_distribution.
func (s *Smither) geom(p float64) int {
	if p <= 0 || p >= 1 {
		panic("bad p")
	}
	count := 1
	for s.rnd.Float64() < p {
		count++
	}
	return count
}

// sample invokes fn mean number of times (but at most n times) with a
// geometric distribution. The i argument to fn will be unique each time and
// randomly chosen to be between 0 and n-1, inclusive. This can be used to pick
// on average mean samples from a list. If n is <= 0, fn is never invoked.
func (s *Smither) sample(n, mean int, fn func(i int)) {
	if n <= 0 {
		return
	}
	perms := s.rnd.Perm(n)
	m := float64(mean)
	p := (m - 1) / m
	k := s.geom(p)
	if k > n {
		k = n
	}
	for ki := 0; ki < k; ki++ {
		fn(perms[ki])
	}
}
