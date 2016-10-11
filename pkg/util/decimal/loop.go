// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package decimal

import (
	"fmt"
	"math"

	"gopkg.in/inf.v0"
)

// loop provides a looping structure that determines when a given computation
// on an *inf.Dec has converged. It was adapted from robpike.io/ivy/value's loop
// implementation.
type loop struct {
	name          string  // The name of the function we are evaluating.
	i             uint64  // Loop count.
	maxIterations uint64  // When to give up.
	stallCount    int     // Iterations since |delta| changed more than threshold.
	arg           inf.Dec // original argument to function; only used for diagnostic.
	prevZ         inf.Dec // Result from the previous iteration.
	delta         inf.Dec // |Change| from previous iteration.
	stallThresh   inf.Dec // The maximum |delta| to be considered a stall.
}

var digitsToBitsRatio = 1 / math.Log10(2)

// newLoop returns a new loop checker. The arguments are the name
// of the function being evaluated, the argument to the function,
// and the desired scale of the result, and the iterations
// per bit.
func newLoop(name string, x *inf.Dec, s inf.Scale, itersPerBit int) *loop {
	bits := x.UnscaledBig().BitLen()
	incrPrec := s - x.Scale()
	if incrPrec > 0 {
		bits += int(float64(incrPrec) * digitsToBitsRatio)
	}
	l := &loop{
		name:          name,
		maxIterations: 10 + uint64(itersPerBit*bits),
	}
	l.arg.Set(x)
	l.stallThresh.SetUnscaled(1).SetScale(s + 1)
	return l
}

// done reports whether the loop is done. If it does not converge
// after the maximum number of iterations, it panics.
func (l *loop) done(z *inf.Dec) bool {
	l.delta.Sub(&l.prevZ, z)
	switch l.delta.Sign() {
	case 0:
		return true
	case -1:
		l.delta.Neg(&l.delta)
	}
	if l.delta.Cmp(&l.stallThresh) < 0 {
		l.stallCount++
		if l.stallCount > 2 {
			return true
		}
	} else {
		l.stallCount = 0
	}
	l.i++
	if l.i == l.maxIterations {
		panic(fmt.Sprintf("%s %s: did not converge after %d iterations; prev,last result %s,%s delta %s", l.name, l.arg.String(), l.maxIterations, z.String(), l.prevZ.String(), l.delta.String()))
	}
	l.prevZ.Set(z)
	return false

}
