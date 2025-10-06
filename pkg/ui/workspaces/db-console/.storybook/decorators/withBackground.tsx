// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import {RenderFunction} from "storybook__react";
import "nvd3/build/nv.d3.min.css";
import "react-select/dist/react-select.css";
import "styl/app.styl";
import "../styles.css";
import "src/views/app/containers/layout/layout.styl";

export const withBackgroundFactory = (backgroundColor = "#F5F7FA") => (storyFn: RenderFunction) => (
  <div style={{backgroundColor}}>
    {storyFn()}
  </div>
);
