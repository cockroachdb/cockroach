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
import classnames from "classnames/bind";
import { Anchor, Button, Text, TextTypes } from "src/components";
import styles from "./empty.module.styl";

const cx = classnames.bind(styles);

export interface IEmptyProps {
  title?: string;
  description?: string;
  label?: React.ReactNode;
  link?: string;
  anchor?: string;
  backgroundImage?: string;
  onClick?: () => void;
  btnType?: "button" | "anchor";
}

export const Empty = ({
  title,
  description,
  anchor,
  label,
  onClick,
  link,
  backgroundImage,
}: IEmptyProps) => (
  <div
    className={cx("cl-empty-view")}
    style={{ backgroundImage: `url(${backgroundImage})` }}
  >
    <Text
      className={cx("cl-empty-view__title")}
      textType={TextTypes.Heading3}
    >
      {title}
    </Text>
    <div
      className={cx("cl-empty-view__content")}
    >
      <main className={cx("cl-empty-view__main")}>
        <Text
          className={cx("cl-empty-view__main--text")}
          textType={TextTypes.Body}
        >
          {description}
          {link && (
            <Anchor
              href={link}
              className={cx("cl-empty-view__main--anchor")}
            >
              {anchor}
            </Anchor>
          )}
        </Text>
      </main>
      <footer className={cx("cl-empty-view__footer")}>
        <Button
          type="primary"
          onClick={onClick}
        >
          {label}
        </Button>
      </footer>
    </div>
  </div>
);

Empty.defaultProps = {
  onClick: () => {},
  backgroundImage: heroBannerLp,
  anchor: "Learn more",
  label: "Learn more",
  title: "No results",
};
