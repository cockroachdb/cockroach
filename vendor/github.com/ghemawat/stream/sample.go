package stream

import (
	"math/rand"
	"time"
)

// Sample picks n pseudo-randomly chosen input items.  Different executions
// of a Sample filter will chose different items.
func Sample(n int) Filter {
	return SampleWithSeed(n, time.Now().UnixNano())
}

// SampleWithSeed picks n pseudo-randomly chosen input items. It uses
// seed as the argument for its random number generation and therefore
// different executions of SampleWithSeed with the same arguments will
// chose the same items.
func SampleWithSeed(n int, seed int64) Filter {
	return FilterFunc(func(arg Arg) error {
		// Could speed this up by using Algorithm Z from Vitter.
		r := rand.New(rand.NewSource(seed))
		reservoir := make([]string, 0, n)
		i := 0
		for s := range arg.In {
			if i < n {
				reservoir = append(reservoir, s)
			} else {
				j := r.Intn(i + 1)
				if j < n {
					reservoir[j] = s
				}
			}
			i++
		}
		for _, s := range reservoir {
			arg.Out <- s
		}
		return nil
	})
}
