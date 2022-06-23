package stream

import "sync"

// Parallel returns a Filter that runs n copies of f.  The input to
// the Parallel Filter is divided up amongst the n copies.  The output
// of the n copies is merged (in an unspecified order) and forms the
// output of the Parallel filter.
func Parallel(n int, f Filter) Filter {
	return FilterFunc(func(arg Arg) error {
		var e filterErrors
		var wg sync.WaitGroup
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				e.record(f.RunFilter(arg))
				wg.Done()
			}()
		}
		wg.Wait()
		return e.getError()
	})
}
