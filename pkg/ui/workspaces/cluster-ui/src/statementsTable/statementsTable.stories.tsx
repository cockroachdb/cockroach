// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { storiesOf } from "@storybook/react";
import { MemoryRouter } from "react-router-dom";
import {
  makeStatementsColumns,
  StatementsSortedTable,
} from "./statementsTable";
import statementsPagePropsFixture from "src/statementsPage/statementsPage.fixture";
import { calculateTotalWorkload } from "src/util";

const { statements } = statementsPagePropsFixture;

storiesOf("StatementsSortedTable", module)
  .addDecorator(storyFn => <MemoryRouter>{storyFn()}</MemoryRouter>)
  .add("with data", () => (
    <StatementsSortedTable
      className="statements-table"
      data={statements}
      columns={makeStatementsColumns(
        statements,
        "(internal)",
        calculateTotalWorkload(statements),
        { "1": "gcp-europe-west1", "2": "gcp-us-east1", "3": "gcp-us-west1" },
        "statement",
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
