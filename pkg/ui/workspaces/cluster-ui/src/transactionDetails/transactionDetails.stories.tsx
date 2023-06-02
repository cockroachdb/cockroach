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
  nodeRegions,
  routeProps,
  timeScale,
  transactionDetailsData,
  transactionFingerprintId,
} from "./transactionDetails.fixture";

import { TransactionDetails } from ".";
import moment from "moment-timezone";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import StatsSortOptions = cockroach.server.serverpb.StatsSortOptions;

storiesOf("Transactions Details", module)
  .addDecorator(storyFn => <MemoryRouter>{storyFn()}</MemoryRouter>)
  .addDecorator(storyFn => (
    <div style={{ backgroundColor: "#F5F7FA" }}>{storyFn()}</div>
  ))
  .add("with data", () => (
    <TransactionDetails
      {...routeProps}
      timeScale={timeScale}
      transactionFingerprintId={transactionFingerprintId.toString()}
      nodeRegions={nodeRegions}
      isTenant={false}
      hasViewActivityRedactedRole={false}
      transactionInsights={undefined}
      refreshData={noop}
      refreshUserSQLRoles={noop}
      onTimeScaleChange={noop}
      refreshNodes={noop}
      refreshTransactionInsights={noop}
      limit={100}
      reqSortSetting={StatsSortOptions.SERVICE_LAT}
      txnStatsResp={{
        lastUpdated: moment(),
        error: null,
        inFlight: false,
        valid: true,
        data: transactionDetailsData,
      }}
    />
  ))
  .add("with loading indicator", () => (
    <TransactionDetails
      {...routeProps}
      timeScale={timeScale}
      transactionFingerprintId={transactionFingerprintId.toString()}
      nodeRegions={nodeRegions}
      isTenant={false}
      hasViewActivityRedactedRole={false}
      transactionInsights={undefined}
      refreshData={noop}
      refreshUserSQLRoles={noop}
      onTimeScaleChange={noop}
      refreshNodes={noop}
      refreshTransactionInsights={noop}
      limit={100}
      reqSortSetting={StatsSortOptions.SERVICE_LAT}
      txnStatsResp={{
        lastUpdated: moment(),
        error: null,
        inFlight: false,
        valid: true,
        data: transactionDetailsData,
      }}
    />
  ))
  .add("with error alert", () => (
    <TransactionDetails
      {...routeProps}
      timeScale={timeScale}
      transactionFingerprintId={undefined}
      nodeRegions={nodeRegions}
      isTenant={false}
      hasViewActivityRedactedRole={false}
      transactionInsights={undefined}
      refreshData={noop}
      refreshUserSQLRoles={noop}
      onTimeScaleChange={noop}
      refreshNodes={noop}
      refreshTransactionInsights={noop}
      limit={100}
      reqSortSetting={StatsSortOptions.SERVICE_LAT}
      txnStatsResp={{
        lastUpdated: moment(),
        error: null,
        inFlight: false,
        valid: true,
        data: transactionDetailsData,
      }}
    />
  ))
  .add("No data for this time frame; no cached transaction text", () => {
    return (
      <TransactionDetails
        {...routeProps}
        timeScale={timeScale}
        transactionFingerprintId={transactionFingerprintId.toString()}
        nodeRegions={nodeRegions}
        isTenant={false}
        hasViewActivityRedactedRole={false}
        transactionInsights={undefined}
        refreshData={noop}
        refreshUserSQLRoles={noop}
        onTimeScaleChange={noop}
        refreshNodes={noop}
        refreshTransactionInsights={noop}
        limit={100}
        reqSortSetting={StatsSortOptions.SERVICE_LAT}
        txnStatsResp={{
          lastUpdated: moment(),
          error: null,
          inFlight: false,
          valid: true,
          data: transactionDetailsData,
        }}
      />
    );
  });
