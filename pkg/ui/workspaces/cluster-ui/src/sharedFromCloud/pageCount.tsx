// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
