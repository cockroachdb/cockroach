import React from "react";
import { storiesOf } from "@storybook/react";
import { Tooltip2 as Tooltip } from "./index";

storiesOf("Tooltip", module).add("simple", () => (
  <Tooltip
    placement="bottom"
    title={
      <div>
        <p>
          For statements not in explicit transactions, CockroachDB wraps each
          statement in individual implicit transactions.
        </p>
      </div>
    }
  >
    TXN Type
  </Tooltip>
));
