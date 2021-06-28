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
import { cloneDeep } from "lodash";

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
  .add("without data", () => (
    <StatementsPage {...statementsPagePropsFixture} statements={[]} />
  ))
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
        statements={[]}
        history={history}
      />
    );
  })
  .add("with error", () => {
    return (
      <StatementsPage
        {...statementsPagePropsWithRequestError}
        statements={[]}
      />
    );
  })
  .add("with loading state", () => {
    return <StatementsPage {...statementsPagePropsFixture} statements={null} />;
  });
