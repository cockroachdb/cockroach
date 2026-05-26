// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { storiesOf } from "@storybook/react";
import React from "react";

import { withRouterDecorator } from "src/util/decorators";

import { LoginPage } from "./loginPage";
import {
  loginPagePropsFixture,
  loginPagePropsLoadingFixture,
  loginPagePropsErrorFixture,
} from "./loginPage.fixture";

storiesOf("LoginPage", module)
  .addDecorator(withRouterDecorator)
  .add("Default state", () => <LoginPage {...loginPagePropsFixture} />)
  .add("Loading state", () => <LoginPage {...loginPagePropsLoadingFixture} />)
  .add("Error state", () => <LoginPage {...loginPagePropsErrorFixture} />);
