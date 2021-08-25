// Copyright 2021 The Cockroach Authors.
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
import { Badge } from "./index";

storiesOf("Badge", module)
  .add("with small size", () => <Badge size="small" text="Small size badge" />)
  .add("with medium (default) size", () => (
    <Badge size="small" text="Medium (default) size badge" />
  ))
  .add("with large size", () => <Badge size="large" text="Large size badge" />);
