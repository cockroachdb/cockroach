import React from "react";
import { storiesOf } from "@storybook/react";
import { Badge } from "./index";

storiesOf("Badge", module)
  .add("with small size", () => <Badge size="small" text="Small size badge" />)
  .add("with medium (default) size", () => (
    <Badge size="small" text="Medium (default) size badge" />
  ))
  .add("with large size", () => <Badge size="large" text="Large size badge" />);
