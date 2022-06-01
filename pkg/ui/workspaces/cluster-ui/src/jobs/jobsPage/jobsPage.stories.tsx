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

import { JobsPage } from "./jobsPage";
import { withData, empty, loading, error } from "./jobsPage.fixture";

storiesOf("JobsPage", module)
  .addDecorator(withRouterProvider)
  .add("With data", () => <JobsPage {...withData} />)
  .add("Empty", () => <JobsPage {...empty} />)
  .add("Loading; with delayed message", () => <JobsPage {...loading} />)
  .add("Timeout error", () => <JobsPage {...error} />);
