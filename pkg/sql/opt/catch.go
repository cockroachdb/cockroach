package opt

import (
	"runtime"

	"github.com/cockroachdb/errors"
)

// CatchOptimizerError catches any runtime panics from optimizer functions and
// returns them as errors. This allows the optimizer to propagate errors
// internally as panics without adding error checks everywhere. This is only
// possible because the optimizer code does not update shared state and does not
// manipulate locks.
func CatchOptimizerError() error {
	r := recover()
	if r == nil {
		return nil
	}
	err, ok := r.(error)
	if !ok {
		// Not an error object. For serious internal errors e.g. in the scheduler,
		// bad goroutine state, allocator problem etc, the go runtime throws a
		// string which does not implement error. So in this case we suspect we are
		// not able to recover, and must crash.
		panic(r)
	}
	if errors.HasInterface(err, (*runtime.Error)(nil)) {
		// Convert runtime errors to assertion failures, which include stacks
		// and get reported to Sentry.
		return errors.HandleAsAssertionFailure(err)
	}
	return err
}
