// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as React from "react";

interface IconProps {
  className?: string;
}

export const StackIcon = ({ className, ...props }: IconProps) => (
  <svg viewBox="0 0 16 16" className={className} {...props}>
    <path
      fillRule="evenodd"
      clipRule="evenodd"
      d="M.503 12.592a.727.727 0 0 1 .93-.44L8 14.5l6.567-2.348a.727.727 0 1 1 .49 1.37l-6.812 2.435a.726.726 0 0 1-.49 0L.943 13.522a.727.727 0 0 1-.44-.93zM7.715.058a.727.727 0 0 1 .57 0l6.851 2.916c1.191.507 1.135 2.203-.081 2.635L8.243 8.024a.727.727 0 0 1-.486 0L.944 5.609C-.272 5.177-.329 3.48.862 2.974L7.715.058zM1.526 4.272L8 6.567l6.473-2.295L8 1.518 1.526 4.272zM.502 8.653a.727.727 0 0 1 .928-.443L8 10.532l6.57-2.322a.727.727 0 0 1 .484 1.371L8.243 11.99a.728.728 0 0 1-.485 0L.946 9.581a.727.727 0 0 1-.444-.928z"
    />
  </svg>
);
