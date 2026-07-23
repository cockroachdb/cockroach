// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { storiesOf } from "@storybook/react";
import noop from "lodash/noop";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import {
  requestTime,
  routeProps,
  timeScale,
  transactionFingerprintId,
} from "./transactionDetails.fixture";

import { TransactionDetails } from ".";

import StatsSortOptions = cockroach.server.serverpb.StatsSortOptions;

const defaultProps = {
  ...routeProps,
  timeScale,
  transactionFingerprintId: transactionFingerprintId.toString(),
  limit: 100,
  reqSortSetting: StatsSortOptions.SERVICE_LAT,
  requestTime,
  onTimeScaleChange: noop,
  onRequestTimeChange: noop,
};

storiesOf("Transactions Details", module)
  .addDecorator(storyFn => <MemoryRouter>{storyFn()}</MemoryRouter>)
  .addDecorator(storyFn => (
    <div style={{ backgroundColor: "#F5F7FA" }}>{storyFn()}</div>
  ))
  .add("with data", () => <TransactionDetails {...defaultProps} />)
  .add("with loading indicator", () => <TransactionDetails {...defaultProps} />)
  .add("with error alert", () => (
    <TransactionDetails
      {...defaultProps}
      transactionFingerprintId={undefined}
    />
  ))
  .add("No data for this time frame; no cached transaction text", () => {
    return <TransactionDetails {...defaultProps} />;
  });
