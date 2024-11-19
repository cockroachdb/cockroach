// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hashbench

import (
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"hash"
	"runtime"
	"testing"
	"time"
)

// chunkingHasher wraps a hash but chunks up writes passed to it.
type chunkingHash struct {
	hash.Hash
	chunkSize int64
}

var _ hash.Hash = chunkingHash{}

func (c chunkingHash) Write(b []byte) (n int, _ error) {
	for int64(len(b)) > c.chunkSize {
		w, _ := c.Hash.Write(b[:c.chunkSize])
		b = b[w:]
		n += w
	}
	w, _ := c.Hash.Write(b)
	return n + w, nil
}

// BenchmarkLatencyWhileHashing demonstrates the latency impact (in this case on
// a loop that just sleeps) that hashing various sizes of blocks of bytes has in
// the presence of GC, due to https://github.com/golang/go/issues/64417.
func BenchmarkLatencyWhileHashing(b *testing.B) {
	for _, hashImpl := range []struct {
		name string
		hash.Hash
	}{{name: "MD5", Hash: md5.New()}, {name: "SHA256", Hash: sha256.New()}} {
		for _, blockSizeKB := range []int64{0, 64, 128, 512, 1024, 2048, 4096} {
			for _, fileSizeMB := range []int64{4, 8, 16, 32} {
				blockSizeStr := fmt.Sprintf("%dKB", blockSizeKB)
				if blockSizeKB == 0 {
					blockSizeStr = "whole"
				}
				b.Run(fmt.Sprintf("hash=%s/block=%s/file=%dMB", hashImpl.name, blockSizeStr, fileSizeMB), func(b *testing.B) {
					h, fileSize := hashImpl.Hash, fileSizeMB<<20
					if blockSizeKB > 0 {
						h = chunkingHash{Hash: h, chunkSize: blockSizeKB << 10}
					}

					stop, stoppedGC, stoppedHashing := make(chan struct{}), make(chan struct{}), make(chan struct{})

					// Hammer GC, in a loop, for the duration of the test. These GCs may
					// stop the world, which would contribute to the latency measured in
					// the benchmark loop below.
					go func() {
						defer close(stoppedGC)
						for {
							select {
							case <-stop:
								return
							default:
								runtime.GC()
							}
						}
					}()

					// Do some hashing work for the duration of the test. These hashing
					// calls will need to be stopped along with the rest of the world by
					// the GC's being being run in the loop above; it they are slow to
					// stop, they'd delay the GC stops and thus increase the observed
					// latency in the benchmark loop.
					go func() {
						defer close(stoppedHashing)
						for {
							select {
							case <-stop:
								return
							default:
								if h != nil {
									buf := make([]byte, fileSize)
									for i := 0; i < 10; i++ {
										h.Write(buf)
										h.Sum(nil)
									}
								}
							}
						}
					}()

					b.ResetTimer()

					// Measure worst-case latency one loop iteration to the next, with
					// each iteration sleeping a fixed interval, while the GC and hashing
					// loops above are also running.
					var worst time.Duration
					before := time.Now()
					for i := 0; i < b.N; i++ {
						time.Sleep(time.Microsecond * 100)
						if d := time.Since(before); d > worst {
							worst = d
						}
						before = time.Now()
					}

					b.StopTimer()
					b.ReportMetric(float64(worst.Microseconds()), "max-latency")

					// Stop the the two background loops and wait for them to exit before
					// moving on to the next test-case.
					close(stop)
					<-stoppedGC
					<-stoppedHashing
				})
			}
		}
	}
}
