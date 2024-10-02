// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { storiesOf } from "@storybook/react";
import React from "react";

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
