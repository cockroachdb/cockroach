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

/*
Package telemetry collects information about roachprod operations.

The following feature-counters are used:
  * <top-level-command>; to look at overall usage patterns
  * create.(ok | error).<cluster-name>.<cloud>.<region>.<instance-type>; to track instance allocations.
  * (destroy | gc).(ok | error).<cluster-name>.<cloud>.<region>.<instance-type>; explicit teardowns.
  * (destroy | gc).lifetime.<cluster-name>; reports the instance uptimes in seconds.

The COCKROACH_TELEMETRY_PRODUCT and COCKROACH_TELEMETRY_URL environment
variables can be set in order to override the built-in defaults. If
present, the COCKROACH_NO_TELEMETRY environment variable will disable
reporting.
*/
package telemetry
