package stream

import (
	"sort"
	"strconv"
	"unicode"
)

// SortFilter is a Filter that sorts its input items by a sequence of
// sort keys.  If two items compare equal by all specified sort keys
// (this always happens if no sort keys are specified), the items
// are compared lexicographically.
type SortFilter struct {
	cmp []sortComparer
}

// Sort returns a filter that sorts its input items. By default, the
// filter sorts items lexicographically. However this can be adjusted
// by calling methods like Num, Text that add sorting keys to the filter.
func Sort() *SortFilter {
	return &SortFilter{}
}

// sortComparer compares a and b and returns -1 if a occurs before b,
// +1 if a occurs after b, 0 otherwise.
type sortComparer func(a, b string) int

// column(s, n) returns 0,x where x is the nth column (1-based) in s,
// or -1,"" if s does not have n columns.  A zero column number is
// treated specially: 0,s is returned.
func column(s string, n int) (int, string) {
	if n == 0 {
		return 0, s
	}
	currentColumn := 0
	wstart := -1
	for i, c := range s {
		sp := unicode.IsSpace(c)
		switch {
		case !sp && wstart < 0: // Start of word
			currentColumn++
			wstart = i
		case sp && wstart >= 0 && currentColumn == n: // End of nth col
			return 0, s[wstart:i]
		case sp && wstart >= 0: // End of another column
			wstart = -1
		}
	}
	if wstart >= 0 && currentColumn == n { // nth column ends string
		return 0, s[wstart:]
	}

	// col not found. Treat as a value smaller than all strings
	return -1, ""
}

// Text sets the next sort key to sort by column n in lexicographic
// order. Column 0 means the entire string. Items that do not have
// column n sort to the front.
func (s *SortFilter) Text(n int) *SortFilter {
	s.add(func(a, b string) int {
		a1, a2 := column(a, n)
		b1, b2 := column(b, n)
		switch {
		case a1 < b1:
			return -1
		case a1 > b1:
			return +1
		case a2 < b2:
			return -1
		case a2 > b2:
			return +1
		}
		return 0
	})
	return s
}

// TextDecreasing sets the next sort key to sort by column n in
// reverse lexicographic order. Column 0 means the entire
// string. Items that do not have column n sort to the end.
func (s *SortFilter) TextDecreasing(n int) *SortFilter {
	return s.Text(n).flipLast()
}

// Num sets the next sort key to sort by column n in numeric
// order. Column 0 means the entire string. Items that do not have
// column n sort to the front.  Items whose column n is not a number
// sort to the end.
func (s *SortFilter) Num(n int) *SortFilter {
	s.add(func(a, b string) int {
		a1, a2 := column(a, n)
		b1, b2 := column(b, n)
		switch {
		case a1 < b1:
			return -1
		case a1 > b1:
			return +1
		}

		// Convert columns from strings to numbers.
		a3, a4 := strconv.ParseFloat(a2, 64)
		b3, b4 := strconv.ParseFloat(b2, 64)

		if a4 != b4 {
			// Errors sort after numbers.
			if a4 != nil { // a had a parse error, b did not
				return +1
			}
			// b had a parse error, a did not
			return -1
		}

		switch {
		case a3 < b3:
			return -1
		case a3 > b3:
			return +1
		}
		return 0
	})
	return s
}

// NumDecreasing sets the next sort key to sort by column n in reverse
// numeric order. Column 0 means the entire string. Items that do not
// have column n sort to the end.  Items whose column n is not a
// number sort to the front.
func (s *SortFilter) NumDecreasing(n int) *SortFilter {
	return s.Num(n).flipLast()
}

// By adds a sort key to sort by the output of the specified less function.
func (s *SortFilter) By(less func(a, b string) bool) *SortFilter {
	s.add(func(a, b string) int {
		if less(a, b) {
			return -1
		}
		if less(b, a) {
			return +1
		}
		return 0
	})
	return s
}

func (s *SortFilter) add(cmp sortComparer) {
	s.cmp = append(s.cmp, cmp)
}

// flipLast reverses the comparison order for the last sort key.
func (s *SortFilter) flipLast() *SortFilter {
	last := s.cmp[len(s.cmp)-1]
	s.cmp[len(s.cmp)-1] = func(a, b string) int { return last(b, a) }
	return s
}

type sortState struct {
	cmp  []sortComparer
	data []string
}

func (s sortState) Len() int      { return len(s.data) }
func (s sortState) Swap(i, j int) { s.data[i], s.data[j] = s.data[j], s.data[i] }
func (s sortState) Less(i, j int) bool {
	a := s.data[i]
	b := s.data[j]
	for _, cmp := range s.cmp {
		r := cmp(a, b)
		if r != 0 {
			return r < 0
		}
	}
	return a < b
}

// RunFilter sorts items by the specified sorting keys. It implements
// the Filter interface.
func (s *SortFilter) RunFilter(arg Arg) error {
	state := sortState{s.cmp, nil}
	for item := range arg.In {
		state.data = append(state.data, item)
	}
	sort.Sort(state)
	for _, item := range state.data {
		arg.Out <- item
	}
	return nil
}
