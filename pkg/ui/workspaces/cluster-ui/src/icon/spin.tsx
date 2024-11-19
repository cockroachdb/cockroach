// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as React from "react";

const SpinIcon = (props: React.SVGProps<SVGSVGElement>): React.ReactElement => (
  <svg
    width={18}
    height={18}
    viewBox="0 0 18 18"
    fill="none"
    style={{ animation: "loadingCircle 1s infinite linear" }}
    {...props}
  >
    <path
      fillRule="evenodd"
      clipRule="evenodd"
      d="M0 9a9 9 0 109-9v2.25A6.75 6.75 0 112.25 9H0z"
      fill="#7E89A9"
    />
  </svg>
);

export default SpinIcon;
