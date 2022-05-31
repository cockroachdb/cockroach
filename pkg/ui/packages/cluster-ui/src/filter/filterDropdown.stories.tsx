// Copyright 2022 The Cockroach Authors.
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
import { FilterDropdown } from "./filterDropdown";
import { FilterCheckboxOption } from "./filterCheckboxOption";
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
      <FilterSearchOption label="Store ID" />
    </FilterDropdown>
  ));
