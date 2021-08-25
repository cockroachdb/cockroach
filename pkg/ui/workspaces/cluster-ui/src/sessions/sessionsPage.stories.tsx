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

import { withBackground, withRouterProvider } from "src/storybook/decorators";
import { SessionsPage } from "./sessionsPage";
import {
  sessionsPagePropsEmptyFixture,
  sessionsPagePropsFixture,
} from "./sessionsPage.fixture";

storiesOf("Sessions Page", module)
  .addDecorator(withRouterProvider)
  .addDecorator(withBackground)
  .add("Overview Page", () => <SessionsPage {...sessionsPagePropsFixture} />)
  .add("Empty Overview Page", () => (
    <SessionsPage {...sessionsPagePropsEmptyFixture} />
  ));
