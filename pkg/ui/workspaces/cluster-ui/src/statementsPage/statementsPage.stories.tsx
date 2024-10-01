// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { storiesOf } from "@storybook/react";
import cloneDeep from "lodash/cloneDeep";
import React from "react";
import { MemoryRouter } from "react-router-dom";

import { StatementsPage } from "./statementsPage";
import statementsPagePropsFixture, {
  statementsPagePropsWithRequestError,
} from "./statementsPage.fixture";

storiesOf("StatementsPage", module)
  .addDecorator(storyFn => <MemoryRouter>{storyFn()}</MemoryRouter>)
  .addDecorator(storyFn => (
    <div style={{ backgroundColor: "#F5F7FA" }}>{storyFn()}</div>
  ))
  .add("with data", () => <StatementsPage {...statementsPagePropsFixture} />)
  .add("without data", () => <StatementsPage {...statementsPagePropsFixture} />)
  .add("with empty search result", () => {
    const props = cloneDeep(statementsPagePropsFixture);
    const { history } = props;
    const searchParams = new URLSearchParams(history.location.search);
    searchParams.set("q", "aaaaaaa");
    history.location.search = searchParams.toString();
    return (
      <StatementsPage
        {...props}
        {...statementsPagePropsFixture}
        history={history}
      />
    );
  })
  .add("with error", () => {
    return <StatementsPage {...statementsPagePropsWithRequestError} />;
  })
  .add("with loading state", () => {
    return <StatementsPage {...statementsPagePropsFixture} />;
  });
