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

package sqltelemetry

import "github.com/cockroachdb/cockroach/pkg/server/telemetry"

// CteUseCounter is to be incremented every time a CTE (WITH ...)
// is planned without error in a query.
var CteUseCounter = telemetry.GetCounterOnce("sql.plan.cte")

// SubqueryUseCounter is to be incremented every time a subquery is
// planned.
var SubqueryUseCounter = telemetry.GetCounterOnce("sql.plan.subquery")

// CorrelatedSubqueryUseCounter is to be incremented every time a
// correlated subquery has been processed during planning.
var CorrelatedSubqueryUseCounter = telemetry.GetCounterOnce("sql.plan.subquery.correlated")
