// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as React from "react";

interface IconProps {
  className?: string;
}

export const CaretRight = ({ className, ...props }: IconProps) => (
  <svg viewBox="0 0 11 17" className={className} {...props}>
    <path
      fillRule="evenodd"
      d="M.512 14.371a1.5 1.5 0 1 0 1.976 2.258l8-7a1.5 1.5 0 0 0 0-2.258l-8-7A1.5 1.5 0 0 0 .512 2.63L7.222 8.5l-6.71 5.871z"
      clipRule="evenodd"
    />
  </svg>
);
