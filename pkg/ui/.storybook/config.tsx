// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { configure } from "@storybook/react";

// Import global styles here
import "nvd3/build/nv.d3.min.css";
import "react-select/dist/react-select.css";
import "antd/es/tooltip/style/css";
import "styl/app.styl";
import "./styles.css";
import "src/views/app/containers/layout/layout.styl";

const req = require.context("../src/", true, /.stories.tsx$/);

function loadStories() {
  req.keys().forEach(filename => req(filename));
}

configure(loadStories, module);
