// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Filters, getTimeValueInSeconds } from "../queryFilter";
import { AggregateStatistics } from "../statementsTable";
import { containAny, unset } from "../util";
import { filterBySearchQuery } from "../statementsPage";

export function filteredStatementsData(
  filters: Filters,
  search: string,
  statements: AggregateStatistics[],
  nodeRegions: { [key: string]: string },
  isTenant: boolean,
): AggregateStatistics[] {
  const timeValue = getTimeValueInSeconds(filters);
  const sqlTypes =
    filters.sqlType?.length > 0
      ? filters.sqlType.split(",").map(function (sqlType: string) {
          // Adding "Type" to match the value on the Statement
          // Possible values: TypeDDL, TypeDML, TypeDCL and TypeTCL
          return "Type" + sqlType;
        })
      : [];
  const databases =
    filters.database?.length > 0 ? filters.database.split(",") : [];
  if (databases.includes(unset)) {
    databases.push("");
  }
  const regions = filters.regions?.length > 0 ? filters.regions.split(",") : [];
  const nodes = filters.nodes?.length > 0 ? filters.nodes.split(",") : [];

  // Return statements filtered by the values selected on the filter and
  // the search text. A statement must match all selected filters to be
  // displayed on the table.
  // Current filters: search text, database, fullScan, service latency,
  // SQL Type, nodes and regions.
  return statements
    .filter(statement => {
      try {
        // Case where the database is returned as an array in a string form.
        const dbList = JSON.parse(statement.database);
        return (
          databases.length === 0 || databases.some(d => dbList.includes(d))
        );
      } catch (e) {
        // Case where the database is a single value as a string.
        return databases.length === 0 || databases.includes(statement.database);
      }
    })
    .filter(statement => (filters.fullScan ? statement.fullScan : true))
    .filter(
      statement =>
        statement.stats.service_lat.mean >= timeValue || timeValue === "empty",
    )
    .filter(
      statement =>
        sqlTypes.length == 0 || sqlTypes.includes(statement.stats.sql_type),
    )
    .filter(
      // The statement must contain at least one value from the selected regions
      // list if the list is not empty.
      statement =>
        regions.length == 0 ||
        statement.stats.regions?.some(region => regions.includes(region)),
    )
    .filter(
      // The statement must contain at least one value from the selected nodes
      // list if the list is not empty.
      // If the cluster is a tenant cluster we don't care
      // about nodes.
      statement =>
        isTenant ||
        nodes.length == 0 ||
        (statement.stats.nodes &&
          containAny(
            statement.stats.nodes.map(node => "n" + node),
            nodes,
          )),
    )
    .filter(statement =>
      search ? filterBySearchQuery(statement, search) : true,
    );
}
