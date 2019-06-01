// Copyright 2019 The Cockroach Authors.
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
// permissions and limitations under the License.

package growstack

// Grow grows the goroutine stack by 16 KB. Goroutine stacks currently start
// at 2 KB in size. The code paths through the storage package often need a
// stack that is 32 KB in size. The stack growth is mildly expensive making it
// useful to trick the runtime into growing the stack early. Since goroutine
// stacks grow in multiples of 2 and start at 2 KB in size, by placing a 16 KB
// object on the stack early in the lifetime of a goroutine we force the
// runtime to use a 32 KB stack for the goroutine.
func Grow()
