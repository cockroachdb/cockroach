// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { storiesOf } from "@storybook/react";
import cloneDeep from "lodash/cloneDeep";
import noop from "lodash/noop";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import { SqlStatsSortOptions } from "../api";

import {
  columns,
  filters,
  requestTime,
  routeProps,
  sortSetting,
  timeScale,
} from "./transactions.fixture";

import { TransactionsPage } from ".";

const defaultProps = {
  ...routeProps,
  columns,
  timeScale,
  filters,
  search: "",
  sortSetting,
  requestTime,
  limit: 100,
  reqSortSetting: SqlStatsSortOptions.PCT_RUNTIME,
  onChangeLimit: noop,
  onChangeReqSort: noop,
  onApplySearchCriteria: noop,
  onFilterChange: noop,
  onSortingChange: noop,
  onRequestTimeChange: noop,
};

storiesOf("Transactions Page", module)
  .addDecorator(storyFn => <MemoryRouter>{storyFn()}</MemoryRouter>)
  .addDecorator(storyFn => (
    <div style={{ backgroundColor: "#F5F7FA" }}>{storyFn()}</div>
  ))
  .add("with data", () => {
    return <TransactionsPage {...defaultProps} />;
  })
  .add("without data", () => {
    return <TransactionsPage {...defaultProps} />;
  })
  .add("with empty search result", () => {
    const props = cloneDeep(defaultProps);
    const { history } = props;
    const searchParams = new URLSearchParams(history.location.search);
    searchParams.set("q", "aaaaaaa");
    history.location.search = searchParams.toString();

    return <TransactionsPage {...props} history={history} />;
  })
  .add("with loading indicator", () => {
    return <TransactionsPage {...defaultProps} />;
  })
  .add("with error alert", () => {
    return <TransactionsPage {...defaultProps} />;
  });
