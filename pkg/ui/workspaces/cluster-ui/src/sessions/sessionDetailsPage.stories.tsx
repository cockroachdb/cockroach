// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { storiesOf } from "@storybook/react";
import React from "react";

import { withBackground, withRouterProvider } from "src/storybook/decorators";

import { SessionDetails } from "./sessionDetails";
import {
  sessionDetailsActiveStmtPropsFixture,
  sessionDetailsActiveTxnPropsFixture,
  sessionDetailsClosedPropsFixture,
  sessionDetailsIdlePropsFixture,
  sessionDetailsNotFound,
} from "./sessionDetailsPage.fixture";

storiesOf("Session Details Page", module)
  .addDecorator(withRouterProvider)
  .addDecorator(withBackground)
  .add("Idle Session", () => (
    <SessionDetails {...sessionDetailsIdlePropsFixture} />
  ))
  .add("Idle Txn", () => (
    <SessionDetails {...sessionDetailsActiveTxnPropsFixture} />
  ))
  .add("Session", () => (
    <SessionDetails {...sessionDetailsActiveStmtPropsFixture} />
  ))
  .add("Closed Session", () => (
    <SessionDetails {...sessionDetailsClosedPropsFixture} />
  ))
  .add("Session Not Found", () => (
    <SessionDetails {...sessionDetailsNotFound} />
  ));
