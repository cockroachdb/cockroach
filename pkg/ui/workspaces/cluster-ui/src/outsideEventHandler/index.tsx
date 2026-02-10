// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import React, { useRef, useEffect, useCallback } from "react";

import styles from "./outsideEventHandler.module.scss";

const cx = classNames.bind(styles);

export interface OutsideEventHandlerProps {
  onOutsideClick: () => void;
  children: React.ReactNode;
  mountNodePosition?: "fixed" | "initial";
  ignoreClickOnRefs?: React.RefObject<HTMLDivElement>[];
}

export function OutsideEventHandler({
  onOutsideClick,
  children,
  mountNodePosition = "initial",
  ignoreClickOnRefs = [],
}: OutsideEventHandlerProps): React.ReactElement {
  const nodeRef = useRef<HTMLDivElement>(null);

  const onClick = useCallback(
    (event: MouseEvent): void => {
      if (!(event.target instanceof Node)) {
        return;
      }
      const target = event.target;

      const isChildEl = nodeRef.current && nodeRef.current.contains(target);

      const isOutsideIgnoredEl = ignoreClickOnRefs.some(outsideIgnoredRef => {
        if (!outsideIgnoredRef || !outsideIgnoredRef.current) {
          return false;
        }
        return outsideIgnoredRef.current.contains(target);
      });

      if (!isChildEl && !isOutsideIgnoredEl) {
        onOutsideClick();
      }
    },
    [onOutsideClick, ignoreClickOnRefs],
  );

  useEffect(() => {
    document.addEventListener("click", onClick);
    return () => {
      document.removeEventListener("click", onClick);
    };
  }, [onClick]);

  const classes = cx(
    "outside-event-handler",
    `outside-event-handler--position-${mountNodePosition}`,
  );

  return (
    <div ref={nodeRef} className={classes}>
      {children}
    </div>
  );
}
