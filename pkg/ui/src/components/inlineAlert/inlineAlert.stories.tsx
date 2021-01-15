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

import { InlineAlert } from "./inlineAlert";
import { styledWrapper } from "src/util/decorators";
import { Anchor } from "src/components";

storiesOf("InlineAlert", module)
  .addDecorator(styledWrapper({ padding: "24px" }))
  .add("with text title", () => (
    <InlineAlert title="Hello world!" message="blah-blah-blah" />
  ))
  .add("with Error intent", () => (
    <InlineAlert title="Hello world!" message="blah-blah-blah" intent="error" />
  ))
  .add("with link in title", () => (
    <InlineAlert
      title={
        <span>
          You do not have permission to view this information.{" "}
          <Anchor href="#">Learn more.</Anchor>
        </span>
      }
    />
  ))
  .add("with multiline message", () => (
    <InlineAlert
      title="Hello world!"
      message={
        <div>
          <div>Message 1</div>
          <div>Message 2</div>
          <div>Message 3</div>
        </div>
      }
    />
  ));
