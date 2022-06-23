//go:build aix
// +build aix

package load

/*
#cgo LDFLAGS: -L/usr/lib -lperfstat

#include <libperfstat.h>
#include <procinfo.h>
*/
import "C"

import (
	"context"
	"unsafe"

	"github.com/power-devops/perfstat"
)

func Avg() (*AvgStat, error) {
	return AvgWithContext(context.Background())
}

func AvgWithContext(ctx context.Context) (*AvgStat, error) {
	c, err := perfstat.CpuTotalStat()
	if err != nil {
		return nil, err
	}
	ret := &AvgStat{
		Load1:  float64(c.LoadAvg1),
		Load5:  float64(c.LoadAvg5),
		Load15: float64(c.LoadAvg15),
	}

	return ret, nil
}

// Misc returns miscellaneous host-wide statistics.
// darwin use ps command to get process running/blocked count.
// Almost same as Darwin implementation, but state is different.
func Misc() (*MiscStat, error) {
	return MiscWithContext(context.Background())
}

func MiscWithContext(ctx context.Context) (*MiscStat, error) {
	info := C.struct_procentry64{}
	cpid := C.pid_t(0)

	ret := MiscStat{}
	for {
		// getprocs first argument is a void*
		num, err := C.getprocs64(unsafe.Pointer(&info), C.sizeof_struct_procentry64, nil, 0, &cpid, 1)
		if err != nil {
			return nil, err
		}

		ret.ProcsTotal++
		switch info.pi_state {
		case C.SACTIVE:
			ret.ProcsRunning++
		case C.SSTOP:
			ret.ProcsBlocked++
		}

		if num == 0 {
			break
		}
	}
	return &ret, nil
}
