// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package flagutil

import (
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/spf13/pflag"
)

func Example() {
	var when time.Time
	var re *regexp.Regexp
	flags := pflag.NewFlagSet("flags", pflag.PanicOnError)
	flags.Var(Time(&when), "when", "sets the time when it happens")
	flags.Var(Regexp(&re), "re", "pattern to tell if it's a match")
	if err := flags.Parse([]string{
		"--when", "13:02",
		"--re", "a match$",
	}); err != nil {
		panic(err)
	}
	fmt.Println("it happens at", when.Format(time.Kitchen), re.MatchString("it's a match"))
	// Output:
	// it happens at 1:02PM true
}

func TestZeroValueEmptyString(t *testing.T) {
	if got := Time(&time.Time{}).String(); got != "" {
		t.Fatalf("got unexpected %v from empty timeFlag.String()", got)
	}
	var r *regexp.Regexp
	if got := Regexp(&r).String(); got != "" {
		t.Fatalf("unexpected value %v from empty regexp string", got)
	}
}

func TestDuration(t *testing.T) {
	got := parseTime(t, "1m")
	if then := timeutil.Now().Add(-1 * time.Minute); then.Sub(got) > 50*time.Millisecond {
		t.Fatalf("Parsed duration is not near now less a minute: got %v, expected near %v, delta %v",
			got, then, then.Sub(got))
	}
}

func TestTimeNegative(t *testing.T) {
	var when time.Time
	flags := pflag.NewFlagSet("test", pflag.PanicOnError)
	flags.Var(Time(&when), "time", "it's a test")
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("did not panic during Parse")
		}
	}()
	if err := flags.Parse([]string{"--time", "junk"}); err != nil {
		t.Fatalf("error parsing flags: %v", err)
	}
}

func TestType(t *testing.T) {
	Regexp(nil).Type()
	Time(nil).Type()
}

func TestTimeFlag(t *testing.T) {
	for _, c := range []timeCase{
		{"12:01", mustParse(time.Kitchen, "12:01PM")},
		{"00:01", mustParse(time.Kitchen, "12:01AM")},
		{"07:01Z", mustParse("15:04:05.999999999Z07:00", "07:01:00.0Z")},
	} {
		c.run(t)
	}
}

type timeCase struct {
	flag     string
	expected time.Time
}

func (c *timeCase) run(t *testing.T) {
	when := parseTime(t, c.flag)
	if !when.Equal(c.expected) {
		t.Errorf("parsing of %v did not equal %v, got %v", c.flag, c.expected, when)
	}
	s := Time(&when).String()
	expected := when.Format(log.MessageTimeFormat)
	if s != expected {
		t.Errorf("String() method returned unexpected %q, expected %q", s, expected)
	}
}

func mustParse(format, s string) time.Time {
	t, err := time.Parse(format, s)
	if err != nil {
		panic(err)
	}
	return t.UTC()
}

func parseTime(t *testing.T, flag string) time.Time {
	var when time.Time
	flags := pflag.NewFlagSet("test", pflag.PanicOnError)
	flags.Var(Time(&when), "time", "it's a test")
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked during Parse: %v", r)
		}
	}()
	if err := flags.Parse([]string{"--time", flag}); err != nil {
		t.Fatalf("unexpected error from flag.Parse: %v", err)
	}
	return when
}

func TestRegexpNegative(t *testing.T) {
	var re *regexp.Regexp
	flags := pflag.NewFlagSet("test", pflag.PanicOnError)
	flags.Var(Regexp(&re), "re", "it's a test")
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("did not panic during Parse")
		}
	}()
	if err := flags.Parse([]string{"--re", "a+*"}); err != nil {
		t.Fatalf("shouldn't have gotten to this code")
	}
}

func TestRegexpString(t *testing.T) {
	re := regexp.MustCompile(".*")
	if Regexp(&re).String() != re.String() {
		t.Fatalf("unexpected string value from non-empty Regexp")
	}
}

func TestEmptyStringZeroes(t *testing.T) {
	now := timeutil.Now()
	re := regexp.MustCompile(".*")
	flags := pflag.NewFlagSet("test", pflag.PanicOnError)
	flags.Var(Time(&now), "time", "it's a test")
	flags.Var(Regexp(&re), "re", "it's a test")
	if err := flags.Parse([]string{"--time", "", "--re", ""}); err != nil {
		t.Fatalf("error parsing flags: %v", err)
	}
	if re != nil {
		t.Errorf("expected empty string to zero regexp")
	}
	if !now.IsZero() {
		t.Errorf("expected empty string to zero time")
	}
}
