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
import cn from "classnames";
import styles from "./anchor.module.styl";

interface AnchorProps {
  onClick?: () => void;
  href?: string;
  target?: "_blank" | "_parent" | "_self";
  className?: string;
}
export function Anchor(props: React.PropsWithChildren<AnchorProps>) {
  const { href, target, children, onClick, className } = props;
  return (
    <a
      className={cn(styles[`crl-anchor`], className)}
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
