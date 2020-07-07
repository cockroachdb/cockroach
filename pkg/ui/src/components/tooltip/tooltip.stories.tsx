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
import { withKnobs, text, boolean, select } from "@storybook/addon-knobs";

import { Tooltip } from "./index";
import { styledWrapper } from "src/util/decorators";

storiesOf("Tooltip", module)
  .addDecorator(styledWrapper({ padding: "150px", textAlign: "center" }))
  .addDecorator(withKnobs)
  .add(
    "withKnobs",
    () => (
      <Tooltip
        visible={boolean("Visible", true)}
        title={
          <div
            dangerouslySetInnerHTML={{
              __html: text(
                "Text",
                "Lorem ipsum lorem ipsum  lorem ipsum ipsum <code>some code</code>  ipsum lorem lorem  <br/><br/> <a>read more</a>",
              ),
            }}
          />
        }
        placement={select(
          "Position",
          ["bottom", "top", "left", "right"],
          "bottom",
        )}
      >
        hoverme
      </Tooltip>
    ),
    {
      knobs: {
        escapeHTML: false,
      },
    },
  );
