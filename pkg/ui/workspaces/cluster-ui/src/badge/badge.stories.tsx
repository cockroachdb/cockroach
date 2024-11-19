// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { storiesOf } from "@storybook/react";
import React from "react";

import { Badge } from "./index";

storiesOf("Badge", module)
  .add("with small size", () => <Badge size="small" text="Small size badge" />)
  .add("with medium (default) size", () => (
    <Badge size="small" text="Medium (default) size badge" />
  ))
  .add("with large size", () => <Badge size="large" text="Large size badge" />);
