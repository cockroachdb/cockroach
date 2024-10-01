// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { storiesOf } from "@storybook/react";
import React from "react";

import { withRouterProvider } from "src/storybook/decorators";

import { JobsPage } from "./jobsPage";
import { withData, empty, loading, error } from "./jobsPage.fixture";

storiesOf("JobsPage", module)
  .addDecorator(withRouterProvider)
  .add("With data", () => <JobsPage {...withData} />)
  .add("Empty", () => <JobsPage {...empty} />)
  .add("Loading; with delayed message", () => <JobsPage {...loading} />)
  .add("Timeout error", () => <JobsPage {...error} />);
