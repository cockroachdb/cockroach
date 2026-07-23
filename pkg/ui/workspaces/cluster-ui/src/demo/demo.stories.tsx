// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { storiesOf } from "@storybook/react";
import React from "react";

import { DemoFetch } from "./demoFetch";

storiesOf("demoFetch", module).add("fetch data from server", () => (
  <DemoFetch />
));
