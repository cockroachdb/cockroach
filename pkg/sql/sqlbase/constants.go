// Copyright 2017 The Cockroach Authors.
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

package sqlbase

import "github.com/cockroachdb/cockroach/pkg/sql/sessiondata"

// DefaultSearchPath is the search path used by virgin sessions.
var DefaultSearchPath = sessiondata.MakeSearchPath([]string{"public"})

// AdminRole is the default (and non-droppable) role with superuser privileges.
var AdminRole = "admin"
