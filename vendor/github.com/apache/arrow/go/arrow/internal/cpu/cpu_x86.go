// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// +build 386 amd64 amd64p32

package cpu

import (
	"os"
	"strings"
)

const CacheLineSize = 64

// cpuid is implemented in cpu_x86.s.
func cpuid(eaxArg, ecxArg uint32) (eax, ebx, ecx, edx uint32)

// xgetbv with ecx = 0 is implemented in cpu_x86.s.
func xgetbv() (eax, edx uint32)

func init() {
	maxID, _, _, _ := cpuid(0, 0)

	if maxID < 1 {
		return
	}

	_, _, ecx1, edx1 := cpuid(1, 0)
	X86.HasSSE2 = isSet(26, edx1)

	X86.HasSSE3 = isSet(0, ecx1)
	X86.HasPCLMULQDQ = isSet(1, ecx1)
	X86.HasSSSE3 = isSet(9, ecx1)
	X86.HasFMA = isSet(12, ecx1)
	X86.HasSSE41 = isSet(19, ecx1)
	X86.HasSSE42 = isSet(20, ecx1)
	X86.HasPOPCNT = isSet(23, ecx1)
	X86.HasAES = isSet(25, ecx1)
	X86.HasOSXSAVE = isSet(27, ecx1)

	osSupportsAVX := false
	// For XGETBV, OSXSAVE bit is required and sufficient.
	if X86.HasOSXSAVE {
		eax, _ := xgetbv()
		// Check if XMM and YMM registers have OS support.
		osSupportsAVX = isSet(1, eax) && isSet(2, eax)
	}

	X86.HasAVX = isSet(28, ecx1) && osSupportsAVX

	if maxID < 7 {
		return
	}

	_, ebx7, _, _ := cpuid(7, 0)
	X86.HasBMI1 = isSet(3, ebx7)
	X86.HasAVX2 = isSet(5, ebx7) && osSupportsAVX
	X86.HasBMI2 = isSet(8, ebx7)
	X86.HasERMS = isSet(9, ebx7)
	X86.HasADX = isSet(19, ebx7)

	// NOTE(sgc): added ability to disable extension via environment
	checkEnvironment()
}
func checkEnvironment() {
	if ext, ok := os.LookupEnv("INTEL_DISABLE_EXT"); ok {
		exts := strings.Split(ext, ",")

		for _, x := range exts {
			switch x {
			case "ALL":
				X86.HasAVX2 = false
				X86.HasAVX = false
				X86.HasSSE42 = false
				X86.HasSSE41 = false
				X86.HasSSSE3 = false
				X86.HasSSE3 = false
				X86.HasSSE2 = false

			case "AVX2":
				X86.HasAVX2 = false
			case "AVX":
				X86.HasAVX = false
			case "SSE":
				X86.HasSSE42 = false
				X86.HasSSE41 = false
				X86.HasSSSE3 = false
				X86.HasSSE3 = false
				X86.HasSSE2 = false
			case "SSE4":
				X86.HasSSE42 = false
				X86.HasSSE41 = false
			case "SSSE3":
				X86.HasSSSE3 = false
			case "SSE3":
				X86.HasSSE3 = false
			case "SSE2":
				X86.HasSSE2 = false
			}
		}
	}
}

func isSet(bitpos uint, value uint32) bool {
	return value&(1<<bitpos) != 0
}
