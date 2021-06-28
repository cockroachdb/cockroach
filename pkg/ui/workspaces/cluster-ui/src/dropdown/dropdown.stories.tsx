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
import { noop } from "lodash";

import { Dropdown, DropdownOption } from "./dropdown";
import { Button } from "src/button";
import { Download } from "@cockroachlabs/icons";

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
