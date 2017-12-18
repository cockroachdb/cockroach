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

package cli

// TestingReset resets global mutable state so that Run can be called multiple
// times from the same test process. It is public for cliccl.
func TestingReset() {
	// Reset the client context for each test. We don't reset the
	// pointers (because they are tied into the flags), but instead
	// overwrite the existing structs' values.
	baseCfg.InitDefaults()
	cliCtx.isInteractive = false
	cliCtx.tableDisplayFormat = tableDisplayTSV
	cliCtx.showTimes = false
	dumpCtx.dumpMode = dumpBoth
	dumpCtx.asOf = ""
	sqlCtx.echo = false
	sqlCtx.execStmts = nil
	zoneCtx.zoneConfig = ""
	zoneCtx.zoneDisableReplication = false
	cmdTimeout = defaultCmdTimeout
}
