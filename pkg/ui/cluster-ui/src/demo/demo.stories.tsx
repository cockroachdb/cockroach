import { storiesOf } from "@storybook/react";
import React from "react";
import { DemoFetch } from "./demoFetch";

storiesOf("demoFetch", module).add("fetch data from server", () => (
  <DemoFetch />
));
