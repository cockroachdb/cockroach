// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { indexUnusedDuration } from "src/util/constants";

import { SchemaInsightsViewProps } from "./schemaInsightsView";

export const SchemaInsightsPropsFixture: SchemaInsightsViewProps = {
  schemaInsights: [
    {
      type: "DropIndex",
      database: "db_name",
      indexDetails: {
        table: "table_name",
        indexID: 1,
        indexName: "index_name",
        schema: "public",
        lastUsed:
          "This index has not been used and can be removed for better write performance.",
      },
    },
    {
      type: "DropIndex",
      database: "db_name2",
      indexDetails: {
        table: "table_name2",
        indexID: 2,
        indexName: "index_name2",
        schema: "public",
        lastUsed:
          "This index has not been used in over 9 days, 5 hours, and 3 minutes and can be removed for better write performance.",
      },
    },
    {
      type: "CreateIndex",
      database: "db_name",
      query: "CREATE INDEX ON test_table (another_num) STORING (num);",
      execution: {
        statement: "SELECT * FROM test_table WHERE another_num > _",
        summary: "SELECT * FROM test_table",
        fingerprintID: "\\xc093e4523ab0bd3e",
        implicit: true,
      },
    },
    {
      type: "CreateIndex",
      database: "db_name",
      query: "CREATE INDEX ON test_table (yet_another_num) STORING (num);",
      execution: {
        statement: "SELECT * FROM test_table WHERE yet_another_num > _",
        summary: "SELECT * FROM test_table",
        fingerprintID: "\\xc093e4523ab0db9o",
        implicit: false,
      },
    },
  ],
  schemaInsightsDatabases: ["db_name", "db_name2"],
  schemaInsightsTypes: ["DropIndex", "CreateIndex"],
  schemaInsightsError: null,
  sortSetting: {
    ascending: false,
    columnTitle: "insights",
  },
  filters: {
    database: "",
    schemaInsightType: "",
  },
  hasAdminRole: true,
  csIndexUnusedDuration: indexUnusedDuration,
  refreshSchemaInsights: () => {},
  onSortChange: () => {},
  onFiltersChange: () => {},
  refreshUserSQLRoles: () => {},
};
