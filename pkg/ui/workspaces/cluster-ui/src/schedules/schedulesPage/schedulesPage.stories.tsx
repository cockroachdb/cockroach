// Copyright 2022 The Cockroach Authors.
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
import { withRouterProvider } from "src/storybook/decorators";

import { SchedulesPage } from "./schedulesPage";
import { withData, empty, loading, error } from "./schedulesPage.fixture";

storiesOf("SchedulesPage", module)
  .addDecorator(withRouterProvider)
  .add("With data", () => <SchedulesPage {...withData} />)
  .add("Empty", () => <SchedulesPage {...empty} />)
  .add("Loading; with delayed message", () => <SchedulesPage {...loading} />)
  .add("Timeout error", () => <SchedulesPage {...error} />);
