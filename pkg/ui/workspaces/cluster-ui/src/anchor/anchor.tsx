// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classnames from "classnames/bind";
import React from "react";

import styles from "./anchor.module.scss";

interface AnchorProps {
  onClick?: () => void;
  href?: string;
  target?: "_blank" | "_parent" | "_self";
  className?: string;
}

const cx = classnames.bind(styles);

export function Anchor(
  props: React.PropsWithChildren<AnchorProps>,
): React.ReactElement {
  const { href, target, children, onClick, className } = props;
  return (
    <a
      className={cx("crl-anchor", className)}
      href={href}
      target={target}
      onClick={onClick}
    >
      {children}
    </a>
  );
}

Anchor.defaultProps = {
  target: "_blank",
  className: "",
};
