package stream

import "regexp"

// Grep emits every input x that matches the regular expression r.
func Grep(r string) Filter {
	re, err := regexp.Compile(r)
	if err != nil {
		return FilterFunc(func(Arg) error { return err })
	}
	return If(re.MatchString)
}

// GrepNot emits every input x that does not match the regular expression r.
func GrepNot(r string) Filter {
	re, err := regexp.Compile(r)
	if err != nil {
		return FilterFunc(func(Arg) error { return err })
	}
	return If(func(s string) bool { return !re.MatchString(s) })
}

// Substitute replaces all occurrences of the regular expression r in
// an input item with replacement.  The replacement string can contain
// $1, $2, etc. which represent submatches of r.
func Substitute(r, replacement string) Filter {
	return FilterFunc(func(arg Arg) error {
		re, err := regexp.Compile(r)
		if err != nil {
			return err
		}
		for s := range arg.In {
			arg.Out <- re.ReplaceAllString(s, replacement)
		}
		return nil
	})
}
