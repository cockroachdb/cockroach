package plpgsqltelemetry

import "github.com/cockroachdb/cockroach/pkg/server/telemetry"

var StmtAssign = telemetry.GetCounter("plpgsql.stmt_assign")
var StmtCase = telemetry.GetCounter("plpgsql.stmt_case")
