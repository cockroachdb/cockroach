// Copyright 2020 The Cockroach Authors.
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
