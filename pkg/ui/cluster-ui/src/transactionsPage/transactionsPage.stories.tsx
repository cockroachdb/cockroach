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
import { data, routeProps } from "./transactions.fixture";

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
    <TransactionsPage {...routeProps} data={data} refreshData={noop} />
  ))
  .add("without data", () => {
    return (
      <TransactionsPage
        {...routeProps}
        data={getEmptyData()}
        refreshData={noop}
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
        data={getEmptyData()}
        refreshData={noop}
        history={history}
      />
    );
  })
  .add("with loading indicator", () => {
    return (
      <TransactionsPage {...routeProps} data={undefined} refreshData={noop} />
    );
  })
  .add("with error alert", () => {
    return (
      <TransactionsPage
        {...routeProps}
        data={undefined}
        error={
          new RequestError(
            "Forbidden",
            403,
            "this operation requires admin privilege",
          )
        }
        refreshData={noop}
      />
    );
  });
