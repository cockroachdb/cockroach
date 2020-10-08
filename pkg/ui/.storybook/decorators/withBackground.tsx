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
import {RenderFunction} from "storybook__react";

export const withBackgroundFactory = (backgroundColor = "#F5F7FA") => (storyFn: RenderFunction) => (
  <div style={{backgroundColor}}>
    {storyFn()}
  </div>
);
