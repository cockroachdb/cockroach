// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { useState, useLayoutEffect } from "react";

export const useDelay = (delay: number) => {
  const [delayLimitReached, setDelayLimitReached] = useState(false);

  useLayoutEffect(() => {
    const timeout = setTimeout(() => {
      setDelayLimitReached(true);
    }, delay);
    return () => clearTimeout(timeout);
  }, [delay]);

  return delayLimitReached;
};
