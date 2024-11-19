// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { PartialStoryFn, StoryContext } from "@storybook/addons";
import React from "react";

export const withBackground = (
  storyFn: PartialStoryFn,
  context: StoryContext,
): React.ReactElement => (
  <div style={{ backgroundColor: "#F5F7FA" }}>{storyFn(context)}</div>
);
