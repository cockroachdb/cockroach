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
import { withRouterDecorator } from "src/util/decorators";

import { JobTable } from "./jobTable";
import { withData, empty } from "./jobTable.fixture";

storiesOf("JobTable", module)
  .addDecorator(withRouterDecorator)
  .add("with data", () => <JobTable {...withData} />)
  .add("empty", () => <JobTable {...empty} />);
