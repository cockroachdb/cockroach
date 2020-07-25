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

import {withBackgroundFactory, withRouterProvider} from ".storybook/decorators";
import {SessionDetails} from "src/views/sessions/sessionDetails";
import {
  sessionDetailsActiveStmtPropsFixture,
  sessionDetailsActiveTxnPropsFixture,
  sessionDetailsIdlePropsFixture, sessionDetailsNotFound,
} from "src/views/sessions/sessionDetailsPage.fixture";

storiesOf("Session Details Page", module)
  .addDecorator(withRouterProvider)
  .addDecorator(withBackgroundFactory())
  .add("Idle Session", () => (
    <SessionDetails
      {...sessionDetailsIdlePropsFixture}
    />
  ))
  .add("Idle Txn", () => (
    <SessionDetails
      {...sessionDetailsActiveTxnPropsFixture}
    />
  ))
  .add("Active Session", () => (
    <SessionDetails
      {...sessionDetailsActiveStmtPropsFixture}
    />
  ))
  .add("Session Not Found", () => (
    <SessionDetails
      {...sessionDetailsNotFound}
    />
  ))
;
