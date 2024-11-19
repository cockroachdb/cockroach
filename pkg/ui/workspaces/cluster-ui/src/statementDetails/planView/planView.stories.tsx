// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { storiesOf } from "@storybook/react";
import React from "react";

import { PlanView } from "./planView";
import { logicalPlan, globalProperties } from "./planView.fixtures";

storiesOf("PlanView", module).add("default", () => (
  <PlanView
    title="Explain Plan"
    globalProperties={globalProperties}
    plan={logicalPlan}
  />
));
