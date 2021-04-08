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
    <TransactionsPage
      {...routeProps}
      data={data}
      refreshData={noop}
      resetSQLStats={noop}
    />
  ))
  .add("without data", () => {
    return (
      <TransactionsPage
        {...routeProps}
        data={getEmptyData()}
        refreshData={noop}
        resetSQLStats={noop}
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
        resetSQLStats={noop}
      />
    );
  })
  .add("with loading indicator", () => {
    return (
      <TransactionsPage
        {...routeProps}
        data={undefined}
        refreshData={noop}
        resetSQLStats={noop}
      />
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
        resetSQLStats={noop}
      />
    );
  });
