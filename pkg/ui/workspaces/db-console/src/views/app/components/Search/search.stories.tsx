// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { storiesOf } from "@storybook/react";
import React from "react";

import { Search } from "./index";

storiesOf("Search", module)
  .add("empty", () => <Search defaultValue="" onSubmit={() => {}} value="" />)
  .add("with search text", () => (
    <Search
      defaultValue="select * from"
      onSubmit={() => {}}
      value="select * from"
    />
  ));
