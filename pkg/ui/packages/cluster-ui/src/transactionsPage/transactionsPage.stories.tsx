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
import { cloneDeep, noop, extend } from "lodash";
import {
  data,
  nodeRegions,
  columns,
  routeProps,
  timeScale,
  sortSetting,
  filters,
} from "./transactions.fixture";

import { TransactionsPage } from ".";
import { RequestError } from "../util";

const getEmptyData = () =>
  extend({}, data, { transactions: [], statements: [] });

storiesOf("Transactions Page", module)
  .addDecorator(storyFn => <MemoryRouter>{storyFn()}</MemoryRouter>)
  .addDecorator(storyFn => (
    <div style={{ backgroundColor: "#F5F7FA" }}>{storyFn()}</div>
  ))
  .add("with data", () => (
    <TransactionsPage
      {...routeProps}
      columns={columns}
      data={data}
      timeScale={timeScale}
      filters={filters}
      nodeRegions={nodeRegions}
      onFilterChange={noop}
      onSortingChange={noop}
      refreshData={noop}
      resetSQLStats={noop}
      search={""}
      sortSetting={sortSetting}
    />
  ))
  .add("without data", () => {
    return (
      <TransactionsPage
        {...routeProps}
        columns={columns}
        data={getEmptyData()}
        timeScale={timeScale}
        filters={filters}
        nodeRegions={nodeRegions}
        onFilterChange={noop}
        onSortingChange={noop}
        refreshData={noop}
        resetSQLStats={noop}
        search={""}
        sortSetting={sortSetting}
      />
    );
  })
  .add("with empty search result", () => {
    const route = cloneDeep(routeProps);
    const { history } = route;
    const searchParams = new URLSearchParams(history.location.search);
    searchParams.set("q", "aaaaaaa");
    history.location.search = searchParams.toString();

    return (
      <TransactionsPage
        {...routeProps}
        columns={columns}
        data={getEmptyData()}
        timeScale={timeScale}
        filters={filters}
        history={history}
        nodeRegions={nodeRegions}
        onFilterChange={noop}
        onSortingChange={noop}
        refreshData={noop}
        resetSQLStats={noop}
        search={""}
        sortSetting={sortSetting}
      />
    );
  })
  .add("with loading indicator", () => {
    return (
      <TransactionsPage
        {...routeProps}
        columns={columns}
        data={undefined}
        timeScale={timeScale}
        filters={filters}
        nodeRegions={nodeRegions}
        onFilterChange={noop}
        onSortingChange={noop}
        refreshData={noop}
        resetSQLStats={noop}
        search={""}
        sortSetting={sortSetting}
      />
    );
  })
  .add("with error alert", () => {
    return (
      <TransactionsPage
        {...routeProps}
        columns={columns}
        data={undefined}
        timeScale={timeScale}
        error={
          new RequestError(
            "Forbidden",
            403,
            "this operation requires admin privilege",
          )
        }
        filters={filters}
        nodeRegions={nodeRegions}
        onFilterChange={noop}
        onSortingChange={noop}
        refreshData={noop}
        resetSQLStats={noop}
        search={""}
        sortSetting={sortSetting}
      />
    );
  });
