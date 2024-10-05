// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as React from "react";

interface IconProps {
  className: string;
  viewBox?: string;
}

export function CircleFilled(props: IconProps): React.ReactElement {
  return (
    <svg {...props}>
      <circle cx="5" cy="5" r="5" />
    </svg>
  );
}
