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
// permissions and limitations under the License.
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package base

// ModuleTestingKnobs is an interface for testing knobs for a submodule.
type ModuleTestingKnobs interface {
	// ModuleTestingKnobs is a dummy function.
	ModuleTestingKnobs()
}

// TestingKnobs contains facilities for controlling various parts of the
// system for testing.
type TestingKnobs struct {
	Store           ModuleTestingKnobs
	SQLExecutor     ModuleTestingKnobs
	SQLLeaseManager ModuleTestingKnobs
}
