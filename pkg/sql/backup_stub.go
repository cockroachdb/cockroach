// Copyright 2016 The Cockroach Authors.
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

package sql

import (
	_ "cloud.google.com/go/storage"    // force a dependency (see below)
	_ "github.com/rlmcpherson/s3gof3r" // force a dependency (see below)
)

// HACK(dt): Work on backup and restore is happening on a branch, including
// code that uses new external libraries. These dummy imports mean master has
// the same dependencies, and thus the branch can use the same `vendor` dir.
