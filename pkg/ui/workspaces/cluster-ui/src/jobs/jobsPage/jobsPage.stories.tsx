// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { storiesOf } from "@storybook/react";
import React from "react";
import { MemoryRouter, Route } from "react-router-dom";

import { withRouterProvider } from "src/storybook/decorators";

import { JobsPage } from "./jobsPage";

storiesOf("JobsPage", module)
  .addDecorator(withRouterProvider)
  .add("Default", () => (
    <MemoryRouter initialEntries={["/jobs"]}>
      <Route path="/jobs" component={JobsPage} />
    </MemoryRouter>
  ));
