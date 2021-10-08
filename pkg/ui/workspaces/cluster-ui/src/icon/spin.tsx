// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as React from "react";

const SpinIcon = (props: React.SVGProps<SVGSVGElement>) => (
  <svg width={18} height={18} viewBox="0 0 18 18" fill="none" {...props}>
    <path
      fillRule="evenodd"
      clipRule="evenodd"
      d="M0 9a9 9 0 109-9v2.25A6.75 6.75 0 112.25 9H0z"
      fill="#7E89A9"
    />
  </svg>
);

export default SpinIcon;
