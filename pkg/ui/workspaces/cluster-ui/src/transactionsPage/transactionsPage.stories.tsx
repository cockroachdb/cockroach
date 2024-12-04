// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { storiesOf } from "@storybook/react";
import cloneDeep from "lodash/cloneDeep";
import extend from "lodash/extend";
import noop from "lodash/noop";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import { RequestState, SqlStatsResponse, SqlStatsSortOptions } from "../api";
import { RequestError } from "../util";

import {
  columns,
  data,
  filters,
  lastUpdated,
  nodeRegions,
  requestTime,
  routeProps,
  sortSetting,
  timeScale,
  timestamp,
} from "./transactions.fixture";

import { TransactionsPage } from ".";

const getEmptyData = () =>
  extend({}, data, { transactions: [], statements: [] });

const defaultLimitAndSortProps = {
  limit: 100,
  reqSortSetting: SqlStatsSortOptions.PCT_RUNTIME,
  onChangeLimit: noop,
  onChangeReqSort: noop,
  onApplySearchCriteria: noop,
};

storiesOf("Transactions Page", module)
  .addDecorator(storyFn => <MemoryRouter>{storyFn()}</MemoryRouter>)
  .addDecorator(storyFn => (
    <div style={{ backgroundColor: "#F5F7FA" }}>{storyFn()}</div>
  ))
  .add("with data", () => {
    const resp: RequestState<SqlStatsResponse> = {
      valid: true,
      inFlight: false,
      data,
      lastUpdated,
      error: null,
    };

    return (
      <TransactionsPage
        {...routeProps}
        txnsResp={resp}
        columns={columns}
        timeScale={timeScale}
        filters={filters}
        nodeRegions={nodeRegions}
        hasAdminRole={true}
        oldestDataAvailable={timestamp}
        onFilterChange={noop}
        onSortingChange={noop}
        refreshData={noop}
        refreshNodes={noop}
        refreshUserSQLRoles={noop}
        resetSQLStats={noop}
        search={""}
        sortSetting={sortSetting}
        requestTime={requestTime}
        onRequestTimeChange={noop}
        {...defaultLimitAndSortProps}
      />
    );
  })
  .add("without data", () => {
    const resp: RequestState<SqlStatsResponse> = {
      valid: true,
      inFlight: false,
      data: getEmptyData(),
      lastUpdated,
      error: null,
    };

    return (
      <TransactionsPage
        {...routeProps}
        columns={columns}
        txnsResp={resp}
        timeScale={timeScale}
        filters={filters}
        nodeRegions={nodeRegions}
        hasAdminRole={true}
        oldestDataAvailable={timestamp}
        onFilterChange={noop}
        onSortingChange={noop}
        refreshData={noop}
        refreshNodes={noop}
        refreshUserSQLRoles={noop}
        resetSQLStats={noop}
        search={""}
        sortSetting={sortSetting}
        requestTime={requestTime}
        onRequestTimeChange={noop}
        {...defaultLimitAndSortProps}
      />
    );
  })
  .add("with empty search result", () => {
    const route = cloneDeep(routeProps);
    const { history } = route;
    const searchParams = new URLSearchParams(history.location.search);
    searchParams.set("q", "aaaaaaa");
    history.location.search = searchParams.toString();

    const resp: RequestState<SqlStatsResponse> = {
      valid: true,
      inFlight: false,
      data: getEmptyData(),
      lastUpdated,
      error: null,
    };

    return (
      <TransactionsPage
        {...routeProps}
        columns={columns}
        txnsResp={resp}
        timeScale={timeScale}
        filters={filters}
        history={history}
        nodeRegions={nodeRegions}
        hasAdminRole={true}
        oldestDataAvailable={timestamp}
        onFilterChange={noop}
        onSortingChange={noop}
        refreshData={noop}
        refreshNodes={noop}
        refreshUserSQLRoles={noop}
        resetSQLStats={noop}
        search={""}
        sortSetting={sortSetting}
        requestTime={requestTime}
        onRequestTimeChange={noop}
        {...defaultLimitAndSortProps}
      />
    );
  })
  .add("with loading indicator", () => {
    const resp: RequestState<SqlStatsResponse> = {
      valid: true,
      inFlight: true,
      data: undefined,
      lastUpdated,
      error: null,
    };

    return (
      <TransactionsPage
        {...routeProps}
        columns={columns}
        txnsResp={resp}
        timeScale={timeScale}
        filters={filters}
        nodeRegions={nodeRegions}
        hasAdminRole={true}
        oldestDataAvailable={timestamp}
        onFilterChange={noop}
        onSortingChange={noop}
        refreshData={noop}
        refreshNodes={noop}
        refreshUserSQLRoles={noop}
        resetSQLStats={noop}
        search={""}
        sortSetting={sortSetting}
        requestTime={requestTime}
        onRequestTimeChange={noop}
        {...defaultLimitAndSortProps}
      />
    );
  })
  .add("with error alert", () => {
    const resp: RequestState<SqlStatsResponse> = {
      valid: true,
      inFlight: true,
      data: undefined,
      lastUpdated,
      error: new RequestError(403, "this operation requires admin privilege"),
    };

    return (
      <TransactionsPage
        {...routeProps}
        columns={columns}
        txnsResp={resp}
        timeScale={timeScale}
        filters={filters}
        nodeRegions={nodeRegions}
        hasAdminRole={true}
        oldestDataAvailable={timestamp}
        onFilterChange={noop}
        onSortingChange={noop}
        refreshData={noop}
        refreshNodes={noop}
        refreshUserSQLRoles={noop}
        resetSQLStats={noop}
        search={""}
        sortSetting={sortSetting}
        requestTime={requestTime}
        onRequestTimeChange={noop}
        {...defaultLimitAndSortProps}
      />
    );
  });
