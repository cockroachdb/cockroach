package stream

import "fmt"

// Items emits items.
func Items(items ...string) Filter {
	return FilterFunc(func(arg Arg) error {
		for _, s := range items {
			arg.Out <- s
		}
		return nil
	})
}

// Repeat emits n copies of s.
func Repeat(s string, n int) Filter {
	return FilterFunc(func(arg Arg) error {
		for i := 0; i < n; i++ {
			arg.Out <- s
		}
		return nil
	})
}

// Numbers emits the integers x..y
func Numbers(x, y int) Filter {
	return FilterFunc(func(arg Arg) error {
		for i := x; i <= y; i++ {
			arg.Out <- fmt.Sprint(i)
		}
		return nil
	})
}

// Map calls fn(x) for every item x and yields the outputs of the fn calls.
func Map(fn func(string) string) Filter {
	return FilterFunc(func(arg Arg) error {
		for s := range arg.In {
			arg.Out <- fn(s)
		}
		return nil
	})
}

// If emits every input x for which fn(x) is true.
func If(fn func(string) bool) Filter {
	return FilterFunc(func(arg Arg) error {
		for s := range arg.In {
			if fn(s) {
				arg.Out <- s
			}
		}
		return nil
	})
}

// Uniq squashes adjacent identical items in arg.In into a single output.
func Uniq() Filter {
	return FilterFunc(func(arg Arg) error {
		first := true
		last := ""
		for s := range arg.In {
			if first || last != s {
				arg.Out <- s
			}
			last = s
			first = false
		}
		return nil
	})
}

// UniqWithCount squashes adjacent identical items in arg.In into a single
// output prefixed with the count of identical items followed by a space.
func UniqWithCount() Filter {
	return FilterFunc(func(arg Arg) error {
		current := ""
		count := 0
		for s := range arg.In {
			if s != current {
				if count > 0 {
					arg.Out <- fmt.Sprintf("%d %s", count, current)
				}
				count = 0
				current = s
			}
			count++
		}
		if count > 0 {
			arg.Out <- fmt.Sprintf("%d %s", count, current)
		}
		return nil
	})
}

// Reverse yields items in the reverse of the order it received them.
func Reverse() Filter {
	return FilterFunc(func(arg Arg) error {
		var data []string
		for s := range arg.In {
			data = append(data, s)
		}
		for i := len(data) - 1; i >= 0; i-- {
			arg.Out <- data[i]
		}
		return nil
	})
}

// NumberLines prefixes its item with its index in the input sequence
// (starting at 1) followed by a space.
func NumberLines() Filter {
	return FilterFunc(func(arg Arg) error {
		line := 1
		for s := range arg.In {
			arg.Out <- fmt.Sprintf("%5d %s", line, s)
			line++
		}
		return nil
	})
}

// Columns splits each item into columns and yields the concatenation
// (separated by spaces) of the columns numbers passed as arguments.
// Columns are numbered starting at 1.  If a column number is bigger
// than the number of columns in an item, it is skipped.
func Columns(columns ...int) Filter {
	return FilterFunc(func(arg Arg) error {
		for _, c := range columns {
			if c <= 0 {
				return fmt.Errorf("stream.Columns: invalid column number %d", c)
			}
		}
		for s := range arg.In {
			result := ""
			for _, col := range columns {
				if _, c := column(s, col); c != "" {
					if result != "" {
						result = result + " "
					}
					result = result + c
				}
			}
			arg.Out <- result
		}
		return nil
	})
}
