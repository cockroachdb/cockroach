import React from "react";
import { storiesOf } from "@storybook/react";
import { PlanView } from "./planView";
import { logicalPlan } from "./planView.fixtures";

storiesOf("PlanView", module).add("default", () => (
  <PlanView title="Logical Plan" plan={logicalPlan} />
));
