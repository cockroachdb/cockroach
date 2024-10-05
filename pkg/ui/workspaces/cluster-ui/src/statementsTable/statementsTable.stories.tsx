// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import { storiesOf } from "@storybook/react";
import { MemoryRouter } from "react-router-dom";
import {
  makeStatementsColumns,
  StatementsSortedTable,
} from "./statementsTable";
import statementsPagePropsFixture from "src/statementsPage/statementsPage.fixture";
import { calculateTotalWorkload } from "src/util";
import { convertRawStmtsToAggregateStatistics } from "../sqlActivity/util";

const statements =
  statementsPagePropsFixture.statementsResponse.data.statements;

storiesOf("StatementsSortedTable", module)
  .addDecorator(storyFn => <MemoryRouter>{storyFn()}</MemoryRouter>)
  .add("with data", () => (
    <StatementsSortedTable
      className="statements-table"
      data={convertRawStmtsToAggregateStatistics(statements)}
      columns={makeStatementsColumns(
        convertRawStmtsToAggregateStatistics(statements),
        ["$ internal"],
        calculateTotalWorkload(statements),
        "statement",
        false,
        false,
        null,
        React.createRef(),
      )}
      sortSetting={{
        ascending: false,
        columnTitle: "rowsRead",
      }}
      pagination={{
        pageSize: 20,
        current: 1,
      }}
    />
  ))
  .add("with data and VIEWACTIVITYREDACTED role", () => (
    <StatementsSortedTable
      className="statements-table"
      data={convertRawStmtsToAggregateStatistics(statements)}
      columns={makeStatementsColumns(
        convertRawStmtsToAggregateStatistics(statements),
        ["$ internal"],
        calculateTotalWorkload(statements),
        "statement",
        false,
        true,
        null,
        React.createRef(),
      )}
      sortSetting={{
        ascending: false,
        columnTitle: "rowsRead",
      }}
      pagination={{
        pageSize: 20,
        current: 1,
      }}
    />
  ))
  .add("empty table", () => <StatementsSortedTable empty />);
