// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Button, Icon } from "@cockroachlabs/ui-components";
import classnames from "classnames/bind";
import React, { useCallback } from "react";

import styles from "./jobProfilerView.module.scss";

const cx = classnames.bind(styles);

interface ExecutionDetailViewerProps {
  filename: string;
  blobUrl: string;
  onClose: () => void;
}

export const ExecutionDetailViewer: React.FC<ExecutionDetailViewerProps> = ({
  filename,
  blobUrl,
  onClose,
}) => {
  const openInNewTab = useCallback(() => {
    // Blob URL already exists so this is synchronous and won't
    // trigger popup blockers.
    window.open(blobUrl, "_blank");
  }, [blobUrl]);

  return (
    <div className={cx("viewer-overlay")} onClick={onClose}>
      <div className={cx("viewer-panel")} onClick={e => e.stopPropagation()}>
        <div className={cx("viewer-header")}>
          <span className={cx("viewer-filename")}>{filename}</span>
          <span className={cx("viewer-actions")}>
            <Button
              as="a"
              size="small"
              intent="tertiary"
              onClick={openInNewTab}
              aria-label="Open in new tab"
            >
              <Icon iconName="Open" />
            </Button>
            <Button
              as="a"
              size="small"
              intent="tertiary"
              onClick={onClose}
              aria-label="Close"
            >
              <Icon iconName="Cancel" />
            </Button>
          </span>
        </div>
        <iframe
          src={blobUrl}
          className={cx("viewer-iframe")}
          sandbox="allow-same-origin allow-scripts"
          title={filename}
        />
      </div>
    </div>
  );
};
