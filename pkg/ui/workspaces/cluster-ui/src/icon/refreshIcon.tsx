// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as React from "react";

interface IconProps {
  className?: string;
  color?: string;
}

const RefreshIcon = ({ className, color }: IconProps) => (
  <svg
    className={className}
    width="18"
    height="18"
    viewBox="0 0 24 24"
    fill={color || "#394455"}
    xmlns="http://www.w3.org/2000/svg"
  >
    <g clipPath="url(#clip0_11029_22359)">
      <path
        d="M1.77601 11.9912C1.77601 8.58278 3.43606 5.47461 6.17336 3.58499V7.04636H7.93937V0H0.893012V1.766H5.78484C2.19985 3.92053 0.0100098 7.7351 0.0100098 11.9912C0.0100098 17.5364 3.93054 22.4636 9.33451 23.6821L9.72303 21.9514C5.11376 20.9095 1.77601 16.7064 1.77601 11.9735V11.9912Z"
        fill={color || "#394455"}
      />
      <path
        d="M23.9924 11.9912C23.9924 6.35762 20.1601 1.53642 14.6678 0.300221L14.2793 2.03091C18.8886 3.07285 22.2263 7.27594 22.2263 12.0088C22.2263 15.4172 20.5663 18.5254 17.829 20.415V16.9536H16.063V24H23.1094V22.234H18.2175C21.8025 20.0795 23.9924 16.2649 23.9924 12.0088V11.9912Z"
        fill={color || "#394455"}
      />
    </g>
    <defs>
      <clipPath id="clip0_11029_22359">
        <rect width="24" height="24" fill="white" />
      </clipPath>
    </defs>
  </svg>
);

export default RefreshIcon;
