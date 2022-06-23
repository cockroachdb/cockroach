/*
Package stream provides filters that can be chained together in a manner
similar to Unix pipelines.  A simple example that prints all go files
under the current directory:

	stream.Run(
		stream.Find("."),
		stream.Grep(`\.go$`),
		stream.WriteLines(os.Stdout),
	)

stream.Run is passed a list of filters that are chained together
(stream.Find, stream.Grep, stream.WriteLines are filters).  Each
filter takes as input a sequence of strings and produces a sequence of
strings. The empty sequence is passed as input to the first
filter. The output of one filter is fed as input to the next filter.

stream.Run is just one way to execute filters.  Others are stream.Contents
(returns the output of the last filter as a []string), and
stream.ForEach (executes a supplied function for every output item).

Error handling

Filter execution can result in errors.  These are returned from stream
functions normally.  For example, the following call will return a
non-nil error.

	err := stream.Run(
		stream.Items("hello", "world"),
		stream.Grep("["), // Invalid regular expression
		stream.WriteLines(os.Stdout),
	)
	// err will be non-nil

User defined filters

Each filter takes as input a sequence of strings (read from a channel)
and produces as output a sequence of strings (written to a channel).
The stream package provides a bunch of useful filters.  Applications can
define their own filters easily. For example, here is a filter that
repeats every input n times:

	func Repeat(n int) stream.FilterFunc {
		return func(arg stream.Arg) error {
			for s := range arg.In {
				for i := 0; i < n; i++ {
					arg.Out <- s
				}
			}
			return nil
		}
	}

	stream.Run(
		stream.Items("hello", "world"),
		Repeat(2),
		stream.WriteLines(os.Stdout),
	)

The output will be:

	hello
	hello
	world
	world

Note that Repeat returns a FilterFunc, a function type that implements the
Filter interface. This is a common implementation pattern: many simple filters
can be expressed as a single function of type FilterFunc.

Tunable Filters

FilterFunc is an appropriate type to use for most filters like Repeat
above.  However for some filters, dynamic customization is
appropriate.  Such filters provide their own implementation of the
Filter interface with extra methods. For example, stream.Sort provides
extra methods that can be used to control how items are sorted:

	stream.Run(
		stream.Command("ls", "-l"),
		stream.Sort().Num(5),  // Sort numerically by size (column 5)
		stream.WriteLines(os.Stdout),
	)

Acknowledgments

The interface of this package is inspired by the http://labix.org/pipe
package. Users may wish to consider that package in case it fits their
needs better.
*/
package stream

import "sync"

// filterErrors records errors accumulated during the execution of a filter.
type filterErrors struct {
	mu  sync.Mutex
	err error
}

func (e *filterErrors) record(err error) {
	if err != nil {
		e.mu.Lock()
		if e.err == nil {
			e.err = err
		}
		e.mu.Unlock()
	}
}

func (e *filterErrors) getError() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.err
}

// Arg contains the data passed to Filter.Run. Arg.In is a channel that
// produces the input to the filter, and Arg.Out is a channel that
// receives the output from the filter.
type Arg struct {
	In    <-chan string
	Out   chan<- string
	dummy bool // To allow later expansion
}

// The Filter interface represents a process that takes as input a
// sequence of strings from a channel and produces a sequence on
// another channel.
type Filter interface {
	// RunFilter reads a sequence of items from Arg.In and produces a
	// sequence of items on Arg.Out.  RunFilter returns nil on success,
	// an error otherwise.  RunFilter must *not* close the Arg.Out
	// channel.
	RunFilter(Arg) error
}

// FilterFunc is an adapter type that allows the use of ordinary
// functions as Filters.  If f is a function with the appropriate
// signature, FilterFunc(f) is a Filter that calls f.
type FilterFunc func(Arg) error

// RunFilter calls this function. It implements the Filter interface.
func (f FilterFunc) RunFilter(arg Arg) error { return f(arg) }

const channelBuffer = 1000

// Sequence returns a filter that is the concatenation of all filter arguments.
// The output of a filter is fed as input to the next filter.
func Sequence(filters ...Filter) Filter {
	if len(filters) == 1 {
		return filters[0]
	}
	return FilterFunc(func(arg Arg) error {
		e := &filterErrors{}
		in := arg.In
		for _, f := range filters {
			c := make(chan string, channelBuffer)
			go runFilter(f, Arg{In: in, Out: c}, e)
			in = c
		}
		for s := range in {
			arg.Out <- s
		}
		return e.getError()
	})
}

// Run executes the sequence of filters and discards all output.
// It returns either nil, an error if any filter reported an error.
func Run(filters ...Filter) error {
	return ForEach(Sequence(filters...), func(s string) {})
}

// ForEach calls fn(s) for every item s in the output of filter and
// returns either nil, or any error reported by the execution of the filter.
func ForEach(filter Filter, fn func(s string)) error {
	in := make(chan string)
	close(in)
	out := make(chan string, channelBuffer)
	e := &filterErrors{}
	go runFilter(filter, Arg{In: in, Out: out}, e)
	for s := range out {
		fn(s)
	}
	return e.getError()
}

// Contents returns a slice that contains all items that are
// the output of filters.
func Contents(filters ...Filter) ([]string, error) {
	var result []string
	err := ForEach(Sequence(filters...), func(s string) {
		result = append(result, s)
	})
	if err != nil {
		result = nil // Discard results on error
	}
	return result, err
}

func runFilter(f Filter, arg Arg, e *filterErrors) {
	e.record(f.RunFilter(arg))
	close(arg.Out)
	for range arg.In { // Discard all unhandled input
	}
}
