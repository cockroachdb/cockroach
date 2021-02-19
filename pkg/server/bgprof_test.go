package server_test

import (
	"context"
	"hash/crc32"
	"runtime"
	runtimepprof "runtime/pprof"
	"testing"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	stop "github.com/cockroachdb/cockroach/pkg/util/stop"
)

func TestBackgroundProfiling(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	//tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	//defer tc.Stopper().Stop(ctx)
	s := stop.NewStopper()
	defer s.Stop(ctx)

	done := make(chan struct{})
	s.RunAsyncTask(ctx, "spin", func(ctx context.Context) {
		runtimepprof.Do(ctx, runtimepprof.Labels("foo", "bar"), func(ctx context.Context) {
			buf := make([]byte, 1024)
			for {
				select {
				case <-done:
					return
				default:
				}
				for i := 0; i < 1000; i++ {
					crc32.Update(0, crc32.IEEETable, buf)
				}
			}
		})
	})

	runtime.EnableProfCPU(100)
	time.Sleep(time.Second)
	runtime.EnableProfCPU(0)
	close(done)
	n := 0
	readProfile(t, runtime.ReadProfCPU, func(d time.Duration, frames *runtime.Frames, labels map[string]string) {
		n++
		if n < 100 {
			fr, _ := frames.Next()
			t.Logf("ts: %s\nlabels: %s\nfn: %s", d, labels, fr.Function)
		}
		return
	})
}

func readProfile(
	t *testing.T,
	read func() ([]uint64, []unsafe.Pointer, bool),
	f func(d time.Duration, frames *runtime.Frames, labels map[string]string),
) {
	var us []uintptr
	for {
		data, tags, eof := read()
		if eof {
			break
		}
		for len(data) > 0 {
			n := int(data[0])
			if n < 3 || n > len(data) {
				t.Errorf("bad record: n=%d, len(data)=%d", n, len(data))
				break
			}
			_ = runtime.ReadProfCPU
			if count := data[2]; count != 0 { // 1 or num_samples or overflow (0)
				var labelMap map[string]string
				if tags[0] != nil {
					labelMap = *(*map[string]string)(tags[0])
				}

				d := time.Duration(data[1]) // time since process start
				for _, n := range data[3:n] {
					us = append(us, uintptr(n))
				}
				frames := runtime.CallersFrames(us)
				us = us[:0]
				f(d, frames, labelMap)
			} // else: overflow record

			data = data[n:]
			tags = tags[1:]
		}
	}
}
