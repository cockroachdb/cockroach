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
