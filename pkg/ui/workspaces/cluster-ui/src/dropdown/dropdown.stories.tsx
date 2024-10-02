// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Download } from "@cockroachlabs/icons";
import { storiesOf } from "@storybook/react";
import noop from "lodash/noop";
import React from "react";

import { Button } from "src/button";

import { Dropdown, DropdownOption } from "./dropdown";

const items: DropdownOption[] = [
  { name: "A", value: "a" },
  { name: "B", value: "b" },
  { name: "C", value: "c" },
];

storiesOf("Dropdown", module)
  .addDecorator(renderChild => (
    <div style={{ padding: "12px", display: "flex" }}>{renderChild()}</div>
  ))
  .add("default", () => (
    <Dropdown onChange={noop} items={items}>
      Select
    </Dropdown>
  ))
  .add("with custom toggle icon", () => (
    <Dropdown
      onChange={noop}
      items={items}
      customToggleButton={
        <Button type="primary" textAlign="center">
          <Download />
        </Button>
      }
    />
  ))
  .add("with custom toggle button options", () => (
    <Dropdown
      onChange={noop}
      items={items}
      customToggleButtonOptions={{
        iconPosition: "left",
        size: "small",
        type: "unstyled-link",
      }}
    >
      Select options
    </Dropdown>
  ));
