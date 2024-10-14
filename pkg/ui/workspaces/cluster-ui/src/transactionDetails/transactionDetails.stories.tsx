// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { storiesOf } from "@storybook/react";
import noop from "lodash/noop";
import moment from "moment-timezone";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import {
  nodeRegions,
  requestTime,
  routeProps,
  timeScale,
  transactionDetailsData,
  transactionFingerprintId,
} from "./transactionDetails.fixture";

import { TransactionDetails } from ".";

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
      requestTime={requestTime}
      onRequestTimeChange={noop}
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
      requestTime={requestTime}
      onRequestTimeChange={noop}
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
      requestTime={moment()}
      onRequestTimeChange={noop}
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
        requestTime={requestTime}
        onRequestTimeChange={noop}
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
