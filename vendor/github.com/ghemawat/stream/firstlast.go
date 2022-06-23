package stream

// First yields the first n items that it receives.
func First(n int) Filter {
	return FilterFunc(func(arg Arg) error {
		seen := 0
		for s := range arg.In {
			if seen >= n {
				break
			}
			arg.Out <- s
			seen++
		}
		return nil
	})
}

// DropFirst yields all items except for the first n items that it receives.
func DropFirst(n int) Filter {
	return FilterFunc(func(arg Arg) error {
		seen := 0
		for s := range arg.In {
			if seen >= n {
				arg.Out <- s
			}
			seen++
		}
		return nil
	})
}

// ring is a circular buffer.
type ring struct {
	buf     []string
	next, n int
}

func newRing(n int) *ring   { return &ring{buf: make([]string, n)} }
func (r *ring) empty() bool { return r.n == 0 }
func (r *ring) full() bool  { return r.n == len(r.buf) }
func (r *ring) pushBack(s string) {
	r.buf[r.next] = s
	r.next = (r.next + 1) % len(r.buf)
	if r.n < len(r.buf) {
		r.n++
	}
}
func (r *ring) popFront() string {
	// The addition of len(r.buf) is so that the dividend is
	// non-negative and therefore remainder is non-negative.
	first := (r.next - r.n + len(r.buf)) % len(r.buf)
	s := r.buf[first]
	r.n--
	return s
}

// Last yields the last n items that it receives.
func Last(n int) Filter {
	return FilterFunc(func(arg Arg) error {
		r := newRing(n)
		for s := range arg.In {
			r.pushBack(s)
		}
		for !r.empty() {
			arg.Out <- r.popFront()
		}
		return nil
	})
}

// DropLast yields all items except for the last n items that it receives.
func DropLast(n int) Filter {
	return FilterFunc(func(arg Arg) error {
		r := newRing(n)
		for s := range arg.In {
			if r.full() {
				arg.Out <- r.popFront()
			}
			r.pushBack(s)
		}
		return nil
	})
}
