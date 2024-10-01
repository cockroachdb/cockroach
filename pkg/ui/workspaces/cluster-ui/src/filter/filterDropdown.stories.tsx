// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { storiesOf } from "@storybook/react";
import noop from "lodash/noop";
import React from "react";

import { FilterCheckboxOption } from "./filterCheckboxOption";
import { FilterDropdown } from "./filterDropdown";
import { FilterSearchOption } from "./filterSearchOption";

storiesOf("FilterDropdown", module)
  .addDecorator(renderChild => (
    <div style={{ padding: "12px", display: "flex" }}>{renderChild()}</div>
  ))
  .add("default", () => (
    <FilterDropdown label="Filters" onSubmit={noop}>
      <FilterCheckboxOption
        label="Node ID"
        value={[{ label: "1", value: "1" }]}
        options={[
          { label: "1", value: "1" },
          { label: "2", value: "2" },
          { label: "3", value: "3" },
          { label: "4", value: "4" },
        ]}
        placeholder="Select"
      />
      <FilterSearchOption label="Store ID" onSubmit={noop} />
    </FilterDropdown>
  ));
