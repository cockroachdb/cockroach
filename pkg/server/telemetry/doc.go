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
// permissions and limitations under the License.

/*
Package telemetry contains helpers for capturing diagnostics information.

Telemetry is captured and shared with cockroach labs if enabled to help the team
prioritize new and existing features or configurations. The docs include more
information about this telemetry, what it includes and how to configure it.

When trying to measure the usage of a given feature, the existing reporting of
"scrubbed" queries -- showing the structure but not the values -- can serve as a
means to measure eg. how many clusters use BACKUP or window functions, etc.
However many features cannot be easily measured just from these statements,
either because they are cannot be reliably inferred from a scrubbed query or
are simply not involved in a SQL statement execution at all.

For such features we also have light-weight `telemetry.Count("some.feature")` to
track their usage. These increment in-memory counts that are then included with
existing diagnostics reporting if enabled. Some notes on using these:
  - "some.feature" should always be a literal string constant -- it must not
    include any user-submitted data.
  - Contention-sensitive, high-volume callers should use an initial `GetCounter`
		to get a Counter they can then `Inc` repeatedly instead to avoid contention
		and map lookup over around the name resolution on each increment.
	-	When naming a counter, by convention we use dot-separated, dashed names, eg.
		`feature-area.specific-feature`.
*/
package telemetry
