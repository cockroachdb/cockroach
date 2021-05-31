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
import { PlanView } from "./planView";
import { logicalPlan } from "./planView.fixtures";

storiesOf("PlanView", module).add("default", () => (
  <PlanView title="Logical Plan" plan={logicalPlan} />
));
