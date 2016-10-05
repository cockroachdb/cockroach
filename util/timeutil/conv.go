package timeutil

import (
	"time"

	"github.com/jeffjen/datefmt"
	"github.com/leekchan/timeutil"
)

// Strftime converts a time to a string using some C-style format.
func Strftime(time time.Time, fmt string) (string, error) {
	return timeutil.Strftime(&time, fmt), nil
}

// Strptime converts a string to a time using some C-style format.
func Strptime(time, fmt string) (time.Time, error) {
	// TODO(knz) The `datefmt` package uses C's `strptime` which doesn't
	// know about microseconds. We may want to change to an
	// implementation that does this better.
	return datefmt.Strptime(fmt, time)
}
