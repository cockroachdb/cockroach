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
import "./anchor.styl";

interface AnchorProps {
  onClick?: () => void;
  href?: string;
  target?: "_blank" | "_parent" | "_self";
}
export function Anchor(props: React.PropsWithChildren<AnchorProps>) {
  const { href, target, children, onClick } = props;
  return (
    <a
      className="crl-anchor"
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
};
