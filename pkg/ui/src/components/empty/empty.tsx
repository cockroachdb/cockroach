// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import heroBannerLp from "assets/heroBannerLp.png";
import React from "react";
import { Anchor, Button, Text, TextTypes } from "src/components";
import "./empty.styl";

interface IMainEmptyProps {
  title?: string;
  description?: string;
  label?: React.ReactNode;
  link?: string;
  anchor?: string;
  backgroundImage?: string;
}

type OnClickXORHref = {
  onClick?: () => void;
  buttonHref?: never;
} | {
    onClick?: never;
    buttonHref?: string;
  };

export type EmptyProps = OnClickXORHref & IMainEmptyProps;

export const Empty = ({
  title,
  description,
  anchor,
  label,
  link,
  backgroundImage,
  onClick,
  buttonHref,
}: EmptyProps) => (
  <div className="cl-empty-view" style={{ backgroundImage: `url(${backgroundImage})` }}>
    <Text
      className="cl-empty-view__title"
      textType={TextTypes.Heading3}
    >
      {title}
    </Text>
    <div className="cl-empty-view__content">
      <main className="cl-empty-view__main">
        <Text
          textType={TextTypes.Body}
        >
          {description}
          {link && <Anchor href={link}>{anchor}</Anchor>}
        </Text>
      </main>
      <footer className="cl-empty-view__footer">
           <Button
              type="primary"
              onClick={() => buttonHref ? window.open(buttonHref) :  onClick && onClick()}
            >
              {label}
            </Button>
      </footer>
    </div>
  </div>
);

Empty.defaultProps = {
  backgroundImage: heroBannerLp,
  anchor: "Learn more",
  label: "Learn more",
  title: "No results",
};
