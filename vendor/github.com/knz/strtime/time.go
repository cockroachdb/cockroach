package strtime

import (
	"time"
	"unsafe"

	"github.com/pkg/errors"
)

// #include <stdlib.h>
// #include "bsdshim.h"
// extern int bsd_strptime(const char *s, const char *format, struct mytm *tm);
import "C"

func Strptime(value string, layout string) (time.Time, error) {
	cValue := C.CString(value)
	defer C.free(unsafe.Pointer(cValue))
	cLayout := C.CString(layout)
	defer C.free(unsafe.Pointer(cLayout))
	var cTime C.struct_mytm

	if r, err := C.bsd_strptime(cValue, cLayout, &cTime); r == 0 || err != nil {
		return time.Time{}, errors.Wrapf(err, "could not parse %s as %s", value, layout)
	}
	return time.Date(
		int(cTime.tm_year)+1900,
		time.Month(cTime.tm_mon+1),
		int(cTime.tm_mday),
		int(cTime.tm_hour),
		int(cTime.tm_min),
		int(cTime.tm_sec),
		int(cTime.tm_nsec),
		time.FixedZone(C.GoString(cTime.tm_zone), int(cTime.tm_gmtoff)),
	), nil
}
