// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
