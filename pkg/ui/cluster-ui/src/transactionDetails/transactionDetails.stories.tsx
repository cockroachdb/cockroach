import React from "react";
import { storiesOf } from "@storybook/react";
import { MemoryRouter } from "react-router-dom";
import { transactionDetails } from "./transactionDetails.fixture";

import { TransactionDetails } from ".";

const { data, error } = transactionDetails;

storiesOf("Transactions Details", module)
  .addDecorator(storyFn => <MemoryRouter>{storyFn()}</MemoryRouter>)
  .addDecorator(storyFn => (
    <div style={{ backgroundColor: "#F5F7FA" }}>{storyFn()}</div>
  ))
  .add("with data", () => (
    <TransactionDetails
      statements={data.statements as any}
      lastReset={Date().toString()}
      handleDetails={() => {}}
      resetSQLStats={() => {}}
    />
  ))
  .add("with loading indicator", () => (
    <TransactionDetails
      statements={undefined}
      lastReset={Date().toString()}
      handleDetails={() => {}}
      resetSQLStats={() => {}}
    />
  ))
  .add("with error alert", () => (
    <TransactionDetails
      statements={undefined}
      error={error}
      lastReset={Date().toString()}
      handleDetails={() => {}}
      resetSQLStats={() => {}}
    />
  ));
