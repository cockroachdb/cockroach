// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package benchprof

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	runtimepprof "runtime/pprof"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/sniffarg"
	"github.com/google/pprof/profile"
)

// StopFn is a function that stops profiling.
type StopFn func(testing.TB)

// Stop stops profiling.
func (f StopFn) Stop(b testing.TB) {
	f(b)
}

// StartCPUProfile starts collection of a CPU profile if the "-test.cpuprofile"
// flag has been set. The profile will omit CPU samples collected prior to
// calling StartCPUProfile, and include all allocations made until the returned
// StopFn is called.
//
// Example usage:
//
//	func BenchmarkFoo(b *testing.B) {
//	    // ..
//	    b.Run("case", func(b *testing.B) {
//	        defer benchprof.StartCPUProfile(b).Stop(b)
//	        // Benchmark loop.
//	    })
//	}
//
// The file name of the profile will include the prefix of the profile flags and
// the benchmark names. For example, "foo_benchmark_thing_1.cpu" would be
// created for a "BenchmarkThing/1" benchmark if the "-test.cpuprofile=foo.cpu"
// flag is set.
//
// NB: The "foo.cpu" file will not be created because we must stop the global
// CPU profiler in order to collect profiles that omit setup samples.
func StartCPUProfile(tb testing.TB) StopFn {
	var cpuProfFile string
	if err := sniffarg.DoEnv("test.cpuprofile", &cpuProfFile); err != nil {
		tb.Fatal(err)
	}
	if cpuProfFile == "" {
		// Not CPU profile requested.
		return func(b testing.TB) {}
	}

	prefix := profilePrefix(cpuProfFile)

	// Hijack the harness's profile to make a clean profile.
	// The flag is set, so likely a CPU profile started by the Go harness is
	// running (unless -count is specified, but StopCPUProfile is idempotent).
	runtimepprof.StopCPUProfile()

	var outputDir string
	if err := sniffarg.DoEnv("test.outputdir", &outputDir); err != nil {
		tb.Fatal(err)
	}
	if outputDir != "" {
		cpuProfFile = filepath.Join(outputDir, cpuProfFile)
	}

	// Remove the harness's profile file to avoid confusion.
	_ = os.Remove(cpuProfFile)

	// Create a new profile file.
	fileName := profileFileName(tb, outputDir, prefix, "cpu")
	f, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		tb.Fatal(err)
	}

	// Start profiling.
	if err := runtimepprof.StartCPUProfile(f); err != nil {
		tb.Fatal(err)
	}

	return func(b testing.TB) {
		runtimepprof.StopCPUProfile()
		if err := f.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

// StartMemProfile starts collection of a heap profile if the "-test.memprofile"
// flag has been set. The profile will omit memory allocations prior to calling
// StartMemProfile, and include all allocations made until the returned StopFn
// is called.
//
// Example usage:
//
//	func BenchmarkFoo(b *testing.B) {
//	    // ..
//	    b.Run("case", func(b *testing.B) {
//	        defer benchprof.StartMemProfile(b).Stop(b)
//	        // Benchmark loop.
//	    })
//	}
//
// The file name of the profile will include the prefix of the profile flags and
// the benchmark names. For example, "foo_benchmark_thing_1.mem" would be
// created for a "BenchmarkThing/1" benchmark if the "-test.memprofile=foo.mem"
// flag is set.
//
// NB: The "foo.mem" file will still be created and include all allocations made
// during the entire duration of the benchmark.
func StartMemProfile(tb testing.TB) StopFn {
	var memProfFile string
	if err := sniffarg.DoEnv("test.memprofile", &memProfFile); err != nil {
		tb.Fatal(err)
	}
	if memProfFile == "" {
		// No heap profile requested.
		return func(b testing.TB) {}
	}

	prefix := profilePrefix(memProfFile)

	var outputDir string
	if err := sniffarg.DoEnv("test.outputdir", &outputDir); err != nil {
		tb.Fatal(err)
	}
	if outputDir != "" {
		memProfFile = filepath.Join(outputDir, memProfFile)
	}

	// Create a new profile file.
	fileName := profileFileName(tb, outputDir, prefix, "mem")
	diffAllocs := diffProfile(func() []byte {
		p := runtimepprof.Lookup("allocs")
		var buf bytes.Buffer
		runtime.GC()
		if err := p.WriteTo(&buf, 0); err != nil {
			tb.Fatal(err)
		}
		return buf.Bytes()
	})

	return func(b testing.TB) {
		if sl := diffAllocs(b); len(sl) > 0 {
			if err := os.WriteFile(fileName, sl, 0644); err != nil {
				b.Fatal(err)
			}
		}
	}
}

func diffProfile(take func() []byte) func(testing.TB) []byte {
	// The below is essentially cribbed from pprof.go in net/http/pprof.

	baseBytes := take()
	if baseBytes == nil {
		return func(tb testing.TB) []byte { return nil }
	}
	return func(b testing.TB) []byte {
		newBytes := take()
		pBase, err := profile.ParseData(baseBytes)
		if err != nil {
			b.Fatal(err)
		}
		pNew, err := profile.ParseData(newBytes)
		if err != nil {
			b.Fatal(err)
		}
		pBase.Scale(-1)
		pMerged, err := profile.Merge([]*profile.Profile{pBase, pNew})
		if err != nil {
			b.Fatal(err)
		}
		pMerged.TimeNanos = pNew.TimeNanos
		pMerged.DurationNanos = pNew.TimeNanos - pBase.TimeNanos

		buf := bytes.Buffer{}
		if err := pMerged.Write(&buf); err != nil {
			b.Fatal(err)
		}
		return buf.Bytes()
	}
}

func profilePrefix(profileArg string) string {
	i := strings.Index(profileArg, ".")
	if i == -1 {
		return profileArg
	}
	return profileArg[:i]
}

func profileFileName(tb testing.TB, outputDir, prefix, suffix string) string {
	saniRE := regexp.MustCompile(`\W+`)
	testName := strings.TrimPrefix(tb.Name(), "Benchmark")
	testName = strings.ToLower(testName)
	testName = saniRE.ReplaceAllString(testName, "_")

	fileName := prefix + "_" + testName + "." + suffix
	if outputDir != "" {
		fileName = filepath.Join(outputDir, fileName)
	}
	return fileName
}
