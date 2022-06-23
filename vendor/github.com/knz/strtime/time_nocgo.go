// +build !cgo

package strtime

import (
	"errors"
	"time"
)

func Strptime(value string, layout string) (time.Time, error) {
	return time.Time{}, errors.New("cgo required")
}
