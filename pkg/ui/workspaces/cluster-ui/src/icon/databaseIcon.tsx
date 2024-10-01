// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as React from "react";

interface IconProps {
  className?: string;
}

export const DatabaseIcon = ({
  className,
  ...props
}: IconProps): React.ReactElement => (
  <svg viewBox="0 0 14 14" className={className} {...props}>
    <path
      fillRule="evenodd"
      clipRule="evenodd"
      d="M12.25 1.167H1.75a.583.583 0 00-.583.583v10.5c0 .322.26.583.583.583h10.5a.583.583 0 00.583-.583V1.75a.583.583 0 00-.583-.583zM1.75 0A1.75 1.75 0 000 1.75v10.5C0 13.216.784 14 1.75 14h10.5A1.75 1.75 0 0014 12.25V1.75A1.75 1.75 0 0012.25 0H1.75z"
    />
    <path
      fillRule="evenodd"
      clipRule="evenodd"
      d="M3.662 13.417V1h1.239v3.292H13V5.49H4.9V8.5H13v1.25H4.9v3.667H3.663z"
    />
  </svg>
);
