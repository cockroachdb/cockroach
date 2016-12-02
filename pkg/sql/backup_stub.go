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
	_ "cloud.google.com/go/storage"
	_ "github.com/rlmcpherson/s3gof3r"
)

// HACK(dt): Work on backup and restore is happening on a branch, including
// code that uses new external libraries. Unfortunately, since we're using a
// submodule for `vendor`, we can't simply rebase the addition of those deps,
// and rather than handle frequent and expensive to resolve conflicts over the
// submodule ref, we're adding the new tdeps to master's vendor now, even while
// their usage is still on the branch. These dummy imports to ensure they are
// part of the dependency graph of master, and thus remain vendored.
