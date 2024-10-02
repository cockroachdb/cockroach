// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Heading, Text, Button } from "@cockroachlabs/ui-components";
import classnames from "classnames/bind";
import React from "react";

import { Anchor } from "../../anchor";
import heroBannerLp from "../../assets/heroBannerLp.png";

import styles from "./emptyPanel.module.scss";

const cx = classnames.bind(styles);

interface IMainEmptyProps {
  title?: string;
  description?: string;
  label?: React.ReactNode;
  link?: string;
  anchor?: string;
  backgroundImage?: string;
}

type OnClickXORHref =
  | {
      onClick?: () => void;
      buttonHref?: never;
    }
  | {
      onClick?: never;
      buttonHref?: string;
    };

export type EmptyPanelProps = OnClickXORHref & IMainEmptyProps;

export const EmptyPanel: React.FC<EmptyPanelProps> = ({
  title = "No results",
  description,
  anchor = "Learn more",
  label = "Learn more",
  link,
  backgroundImage = heroBannerLp,
  onClick,
  buttonHref,
}) => (
  <div
    className={cx("cl-empty-view")}
    style={{ backgroundImage: `url(${backgroundImage})` }}
  >
    <Heading type="h2">{title}</Heading>
    <div className={cx("cl-empty-view__content")}>
      <main className={cx("cl-empty-view__main")}>
        <Text>
          {description}
          {link && (
            <Anchor href={link} className={cx("cl-empty-view__main--anchor")}>
              {anchor}
            </Anchor>
          )}
        </Text>
      </main>
      <footer className={cx("cl-empty-view__footer")}>
        <Button
          intent="primary"
          onClick={() =>
            buttonHref ? window.open(buttonHref) : onClick && onClick()
          }
        >
          {label}
        </Button>
      </footer>
    </div>
  </div>
);
