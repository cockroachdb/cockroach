// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/* eslint-disable react/jsx-key */
import { CaretDown } from "@cockroachlabs/icons";
import { storiesOf } from "@storybook/react";
import React from "react";

import { Button, ButtonProps } from "src/button";

import { Text, TextTypes } from "../text";

const sizes: ButtonProps["size"][] = ["default", "small"];
const types: ButtonProps["type"][] = [
  "primary",
  "secondary",
  "flat",
  "unstyled-link",
];
const icons: ButtonProps["icon"][] = [<CaretDown />, undefined];
const iconPositions: ButtonProps["iconPosition"][] = ["right", "left"];

storiesOf("Button", module)
  .addDecorator(renderChild => (
    <div style={{ padding: "12px", display: "flex" }}>{renderChild()}</div>
  ))
  .add("default", () => <Button>Caption</Button>)
  .add("examples", () => {
    const buttons = types.map(buttonType => {
      const buttonsPerSize = sizes.map(size => {
        const items = icons
          .map(buttonIcon => {
            return iconPositions.map(iconPosition => {
              return (
                <Button
                  type={buttonType}
                  size={size}
                  icon={buttonIcon}
                  iconPosition={iconPosition}
                >
                  Sample text
                </Button>
              );
            });
          })
          .reduce((ac, el) => [...ac, ...el], []);

        return (
          <div
            style={{
              display: "flex",
              flexDirection: "row",
              justifyContent: "space-around",
              margin: "24px 0",
            }}
          >
            {React.Children.toArray(items)}
          </div>
        );
      });

      return (
        <div>
          <Text textType={TextTypes.Heading3}>{buttonType} type</Text>
          <div style={{ display: "flex", flexDirection: "column" }}>
            {React.Children.toArray(buttonsPerSize)}
          </div>
        </div>
      );
    });

    return (
      <div style={{ display: "flex", flexDirection: "column", width: "100%" }}>
        {React.Children.toArray(buttons)}
      </div>
    );
  });
