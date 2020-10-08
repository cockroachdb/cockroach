// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/*
Package sqltelemetry contains telemetry counter definitions
for various SQL features.

Centralizing the counters in a single place achieves three objectives:

- the comments that accompany the counters enable non-technical users
  to comprehend what is being reported without having to read code.

- the counters are placed side-by-side, grouped by category, so as to
  enable exploratory discovery of available telemetry.

- the counters are pre-registered and their unicity is asserted,
  so that no two features end up using the same counter name.
*/
package sqltelemetry
