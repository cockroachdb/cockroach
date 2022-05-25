// Copyright 2022 The Cockroach Authors.
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
  fill?: string;
  size?: number;
  className?: string;
}

export const FilterIcon = ({ fill, size, className, ...props }: IconProps) => (
  <svg
    width={size}
    height={size}
    viewBox={`0 0 ${size} ${size}`}
    fill={fill}
    className={className}
    {...props}
  >
    <path
      d="M8.67698 14C8.53676 14 8.39891 13.9452 8.29612 13.8422L6.1423 11.6884C6.04126 11.5875 5.98454 11.4506 5.98454 11.3076V6.46147C5.98454 6.30822 5.86986 6.0314 5.76156 5.92297L0.757693 0.919106C0.603696 0.765109 0.557622 0.533624 0.641007 0.332405C0.724517 0.131209 0.920581 0 1.13832 0H14.0614C14.2793 0 14.4757 0.131212 14.5589 0.332405C14.6424 0.533602 14.5962 0.765099 14.4422 0.919106L9.43833 5.92297C9.33003 6.0314 9.21535 6.30823 9.21535 6.46147V13.4615C9.21535 13.6793 9.08427 13.8758 8.88294 13.9589C8.81646 13.9867 8.74635 14 8.67711 14L8.67698 14ZM7.0614 11.0848L8.13839 12.1618V6.46143C8.13839 6.02021 8.36488 5.47374 8.67663 5.16159L12.7616 1.07664H2.43847L6.5231 5.16159C6.8351 5.47359 7.06159 6.02021 7.06159 6.46143L7.0614 11.0848Z"
      fill={fill}
    />
  </svg>
);

FilterIcon.defaultProps = {
  fill: "#475872",
  size: 14,
};
