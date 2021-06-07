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
import { transactionDetails } from "./transactionDetails.fixture";

import { TransactionDetails } from ".";

const { data, nodeRegions, error } = transactionDetails;

storiesOf("Transactions Details", module)
  .addDecorator(storyFn => <MemoryRouter>{storyFn()}</MemoryRouter>)
  .addDecorator(storyFn => (
    <div style={{ backgroundColor: "#F5F7FA" }}>{storyFn()}</div>
  ))
  .add("with data", () => (
    <TransactionDetails
      statements={data.statements as any}
      nodeRegions={nodeRegions}
      lastReset={Date().toString()}
      handleDetails={() => {}}
      resetSQLStats={() => {}}
    />
  ))
  .add("with loading indicator", () => (
    <TransactionDetails
      statements={undefined}
      nodeRegions={nodeRegions}
      lastReset={Date().toString()}
      handleDetails={() => {}}
      resetSQLStats={() => {}}
    />
  ))
  .add("with error alert", () => (
    <TransactionDetails
      statements={undefined}
      nodeRegions={nodeRegions}
      error={error}
      lastReset={Date().toString()}
      handleDetails={() => {}}
      resetSQLStats={() => {}}
    />
  ));
