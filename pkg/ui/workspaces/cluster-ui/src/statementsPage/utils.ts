import {
  AggregateStatistics,
  ColumnDescriptor,
  Filters,
  flattenTreeAttributes,
  getTimeValueInSeconds,
  planNodeToString,
} from "src";
import { containAny } from "src/util";

// filterBySearchQuery returns true if a search query matches the statement.
export function filterBySearchQuery(
  statement: AggregateStatistics,
  search: string,
): boolean {
  const label = statement.label;
  const plan = planNodeToString(
    flattenTreeAttributes(
      statement.stats.sensitive_info &&
        statement.stats.sensitive_info.most_recent_plan_description,
    ),
  );
  const matchString = `${label} ${plan}`.toLowerCase();
  return search
    .toLowerCase()
    .split(" ")
    .every(val => matchString.includes(val));
}

export function filterStatements(
  statements: AggregateStatistics[],
  filters: Filters,
  search: string,
  isTenant: boolean,
  nodeRegions: Record<string, string>,
): AggregateStatistics[] {
  const timeValue = getTimeValueInSeconds(filters);
  const sqlTypes =
    filters.sqlType.length > 0
      ? filters.sqlType.split(",").map(function(sqlType: string) {
          // Adding "Type" to match the value on the Statement
          // Possible values: TypeDDL, TypeDML, TypeDCL and TypeTCL
          return "Type" + sqlType;
        })
      : [];
  const databases =
    filters.database.length > 0 ? filters.database.split(",") : [];
  if (databases.includes("(unset)")) {
    databases.push("");
  }
  const regions = filters.regions.length > 0 ? filters.regions.split(",") : [];
  const nodes = filters.nodes.length > 0 ? filters.nodes.split(",") : [];

  // Return statements filtered by the values selected on the filter and
  // the search text. A statement must match all selected filters to be
  // displayed on the table.
  // Current filters: search text, database, fullScan, service latency,
  // SQL Type, nodes and regions.

  let filteredStatements = statements
    .filter(
      statement =>
        databases.length == 0 || databases.includes(statement.database),
    )
    .filter(statement => (filters.fullScan ? statement.fullScan : true))
    .filter(
      statement =>
        statement.stats.service_lat.mean >= timeValue || timeValue === "empty",
    )
    .filter(
      statement =>
        sqlTypes.length == 0 || sqlTypes.includes(statement.stats.sql_type),
    );

  if (!isTenant) {
    // These filters only apply to non-tenants.

    filteredStatements = filteredStatements
      .filter(
        // The statement must contain at least one value from the selected regions.
        statement =>
          regions.length === 0 ||
          (statement.stats.nodes &&
            containAny(
              statement.stats.nodes.map(node => nodeRegions[node.toString()]),
              regions,
            )),
      )
      .filter(
        // The statement must contain at least one value from the selected nodes.
        statement =>
          nodes.length === 0 ||
          (statement.stats.nodes &&
            containAny(
              statement.stats.nodes.map(node => "n" + node),
              nodes,
            )),
      );
  }

  // Finally, filter by the provided search query.
  // This is the last filter as it's a bit mmore expensive.
  if (search && search.length > 0) {
    filteredStatements = filteredStatements.filter(statement =>
      filterBySearchQuery(statement, search),
    );

    return filteredStatements;
  }

  return filteredStatements;
}

export const isColumnSelected = (
  c: ColumnDescriptor<AggregateStatistics>,
  userSelectedColumnsToShow: string[],
): boolean => {
  return (
    // show column if list of visible was never defined and can be show by default.
    (userSelectedColumnsToShow === null && c.showByDefault !== false) ||
    (userSelectedColumnsToShow !== null &&
      userSelectedColumnsToShow.includes(c.name)) || // show column if user changed its visibility.
    c.alwaysShow === true // show column if alwaysShow option is set explicitly.
  );
};
