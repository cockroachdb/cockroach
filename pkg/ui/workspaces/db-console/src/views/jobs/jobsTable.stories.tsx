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
import { withRouterDecorator } from "src/util/decorators";

import { JobsTable } from "./index";
import { withData, empty, loading, error } from "./jobsTable.fixture";

storiesOf("JobsTable", module)
  .addDecorator(withRouterDecorator)
  .add("With data", () => <JobsTable {...withData} />)
  .add("Empty", () => <JobsTable {...empty} />)
  .add("Loading; with delayed message", () => <JobsTable {...loading} />)
  .add("Timeout error", () => <JobsTable {...error} />);
