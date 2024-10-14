// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";
import React, { useState, useEffect } from "react";

type Props = {
  children: React.ReactElement;
  delay?: moment.Duration;
};

export const Delayed = ({
  children,
  delay = moment.duration(10, "s"),
}: Props): React.ReactElement => {
  const [isShown, setIsShown] = useState(false);

  useEffect(() => {
    const timer = setTimeout(() => {
      setIsShown(true);
    }, delay.asMilliseconds());
    return () => clearTimeout(timer);
  }, [delay]);

  return isShown ? children : null;
};
