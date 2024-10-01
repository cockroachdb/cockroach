// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, {
  useRef,
  useEffect,
  forwardRef,
  useImperativeHandle,
} from "react";

export interface DownloadAsFileProps {
  fileName?: string;
  content?: Blob;
}

export interface DownloadFileRef {
  download: (name: string, body: Blob) => void;
}

/*
 * DownloadFile can download file in two modes `default` and `imperative`.
 * `Default` mode - when DownloadFile wraps component which should trigger
 * downloading and can work only if content of file is already available.
 *
 * For example:
 * ```
 * <DownloadFile fileName="example.txt" fileType="text/plain" content="Some text">
 *   <button>Download</download>
 * </DownloadFile>
 * ```
 *
 * `Imperative` mode allows initiate file download in async way, and trigger
 * download manually.
 *
 * For example:
 * ```
 * downloadRef = React.createRef<DownloadFileRef>();
 *
 * fetchData = () => {
 *   Promise.resolve().then((someText) =>
 *     this.downloadRef.current.download("example.txt", "text/plain", someText))
 * }
 *
 * <DownloadFile ref={downloadRef} />
 * <button onClick={fetchData}>Download</button>
 * ```
 * */
// tslint:disable-next-line:variable-name
export const DownloadFile = forwardRef<DownloadFileRef, DownloadAsFileProps>(
  (props, ref) => {
    const { children, fileName, content } = props;
    const anchorRef = useRef<HTMLAnchorElement>();

    const bootstrapFile = (name: string, body: Blob) => {
      const anchorElement = anchorRef.current;
      anchorElement.href = URL.createObjectURL(body);
      anchorElement.download = name;
    };

    useEffect(() => {
      if (content === undefined) {
        return;
      }
      bootstrapFile(fileName, content);
    }, [fileName, content]);

    useImperativeHandle(ref, () => ({
      download: (name: string, body: Blob) => {
        bootstrapFile(name, body);
        anchorRef.current.click();
      },
    }));

    return <a ref={anchorRef}>{children}</a>;
  },
);
