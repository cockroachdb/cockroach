// Copyright 2021 Peter Mattis.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

// Assembly to mimic runtime.getg.

//go:build arm64 && gc && go1.5
// +build arm64
// +build gc
// +build go1.5

#include "textflag.h"

// func getg() *g
TEXT ·getg(SB),NOSPLIT,$0-8
	MOVD g, ret+0(FP)
	RET
