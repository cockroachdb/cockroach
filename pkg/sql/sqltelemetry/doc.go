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
