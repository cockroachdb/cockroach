// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { storiesOf } from "@storybook/react";
import React from "react";

import { withRouterProvider } from "src/storybook/decorators";

import { SchedulesPage } from "./schedulesPage";
import { withData, empty, loading, error } from "./schedulesPage.fixture";

storiesOf("SchedulesPage", module)
  .addDecorator(withRouterProvider)
  .add("With data", () => <SchedulesPage {...withData} />)
  .add("Empty", () => <SchedulesPage {...empty} />)
  .add("Loading; with delayed message", () => <SchedulesPage {...loading} />)
  .add("Timeout error", () => <SchedulesPage {...error} />);
