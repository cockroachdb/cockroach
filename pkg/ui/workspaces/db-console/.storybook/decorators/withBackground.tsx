// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";
import {RenderFunction} from "storybook__react";
import "react-select/dist/react-select.css";
import "styl/app.scss";
import "../styles.css";
import "src/views/app/containers/layout/layout.scss";

export const withBackgroundFactory = (backgroundColor = "#F5F7FA") => (storyFn: RenderFunction) => (
  <div style={{backgroundColor}}>
    {storyFn()}
  </div>
);
