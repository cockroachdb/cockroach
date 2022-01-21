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
import { noop } from "lodash";
import {
  transactionDetails,
  routeProps,
  dateRange,
} from "./transactionDetails.fixture";

import { TransactionDetails } from ".";

const { data, nodeRegions, error } = transactionDetails;

storiesOf("Transactions Details", module)
  .addDecorator(storyFn => <MemoryRouter>{storyFn()}</MemoryRouter>)
  .addDecorator(storyFn => (
    <div style={{ backgroundColor: "#F5F7FA" }}>{storyFn()}</div>
  ))
  .add("with data", () => (
    <TransactionDetails
      {...routeProps}
      aggregatedTs={data.aggregatedTs}
      dateRange={dateRange}
      transactionFingerprintId={data.transactionFingerprintId}
      transaction={data.transaction}
      statements={data.statements as any}
      nodeRegions={nodeRegions}
      isTenant={false}
      hasViewActivityRedactedRole={false}
      refreshData={noop}
    />
  ))
  .add("with loading indicator", () => (
    <TransactionDetails
      {...routeProps}
      aggregatedTs={data.aggregatedTs}
      dateRange={dateRange}
      transactionFingerprintId={data.transactionFingerprintId}
      transaction={data.transaction}
      statements={undefined}
      nodeRegions={nodeRegions}
      isTenant={false}
      hasViewActivityRedactedRole={false}
      refreshData={noop}
    />
  ))
  .add("with error alert", () => (
    <TransactionDetails
      {...routeProps}
      aggregatedTs={undefined}
      dateRange={undefined}
      transactionFingerprintId={undefined}
      transaction={undefined}
      statements={undefined}
      nodeRegions={nodeRegions}
      error={error}
      isTenant={false}
      hasViewActivityRedactedRole={false}
      refreshData={noop}
    />
  ));
