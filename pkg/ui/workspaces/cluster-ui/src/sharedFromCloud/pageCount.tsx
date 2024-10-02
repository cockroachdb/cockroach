// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";

interface PageCountProps {
  page: number;
  pageSize: number;
  total: number;
  entity: string;
}

export default function formPageCount(props: PageCountProps) {
  const { page, pageSize, total, entity } = props;

  const start = Math.min((page - 1) * pageSize + 1, total);
  const end = Math.min(start + pageSize - 1, total);
  return (
    <span>
      {start}-{end} of {total} {entity}
    </span>
  );
}
