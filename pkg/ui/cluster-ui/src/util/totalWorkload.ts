import * as protos from "@cockroachlabs/crdb-protobuf-client";
import { longToInt } from "src/barCharts/utils";
import { AggregateStatistics } from "src/statementsTable";

type Statement = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
type statementType = AggregateStatistics | Statement;
type statementsType = Array<statementType>;

/**
 * Function to calculate total workload of statements
 * Currently is recalculating every time is called, if that becames an issue
 * on the future, consider use of cache and memoize the function
 * @param statements array of statements (AggregateStatistics or Statement)
 * @returns the total workload of all statements
 */
export function calculateTotalWorkload(statements: statementsType) {
  return statements.reduce((totalWorkload: number, stmt: statementType) => {
    return (totalWorkload +=
      longToInt(stmt.stats.count) * stmt.stats.service_lat.mean);
  }, 0);
}
