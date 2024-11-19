// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { storiesOf } from "@storybook/react";
import noop from "lodash/noop";
import React from "react";

import { AlertLevel } from "src/redux/alerts";
import { styledWrapper } from "src/util/decorators";

import { AlertMessage } from "./alertMessage";

storiesOf("AlertMessage", module)
  .addDecorator(styledWrapper({ padding: "24px" }))
  .add("Success alert without text", () => (
    <AlertMessage
      title="You successfully signed up for CockroachDB release notes"
      level={AlertLevel.SUCCESS}
      dismiss={noop}
      autoClose={false}
    />
  ))
  .add("NOTIFICATION", () => (
    <AlertMessage
      title="title"
      text="text"
      level={AlertLevel.NOTIFICATION}
      dismiss={noop}
      autoClose={false}
    />
  ))
  .add("WARNING", () => (
    <AlertMessage
      title="title"
      text="text"
      level={AlertLevel.WARNING}
      dismiss={noop}
      autoClose={false}
    />
  ))
  .add("Critical alert without text", () => (
    <AlertMessage
      title="We're currently having some trouble fetching updated data. If this persists, it might be a good idea to check your network connection to the CockroachDB cluster."
      text="text"
      level={AlertLevel.CRITICAL}
      dismiss={noop}
      autoClose={false}
    />
  ))
  .add("Critical alert with text", () => (
    <AlertMessage
      title="There was an error activating statement diagnostics"
      text="Please try activating again. If the problem continues please reach out to customer support."
      level={AlertLevel.CRITICAL}
      dismiss={noop}
      autoClose={false}
    />
  ));
