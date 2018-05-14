// Copyright 2018 The Cockroach Authors.
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

package main

func registerTests(r *registry) {
	// Helpful shell pipeline to generate the list below:
	//
	// grep -h -E 'func register[^(]+\(.*registry\) {' *.go | grep -E -o 'register[^(]+' | grep -v '^registerTests$' | sort | awk '{printf "\t%s(r)\n", $0}'

	registerAllocator(r)
	registerBackup(r)
	registerCancel(r)
	registerClock(r)
	registerCopy(r)
	registerDebug(r)
	registerDecommission(r)
	registerDrop(r)
	registerHotSpotSplits(r)
	registerImportTPCC(r)
	registerImportTPCH(r)
	registerJepsen(r)
	registerKV(r)
	registerKVScalability(r)
	registerKVSplits(r)
	registerLargeRange(r)
	registerRestore(r)
	registerRoachmart(r)
	registerScaleData(r)
	registerSchemaChange(r)
	registerTPCC(r)
	registerUpgrade(r)
	registerVersion(r)
}
