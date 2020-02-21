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
import "./link.styl";

interface LinkProps {
  href: string;
  target?: "_blank" | "_parent" | "_self";
}
export function Link(props: React.PropsWithChildren<LinkProps>) {
  const { href, target, children } = props;
  return (
    <a className="crl-link" href={href} target={target}>{children}</a>
  );
}

Link.defaultProps = {
  target: "_blank",
};
