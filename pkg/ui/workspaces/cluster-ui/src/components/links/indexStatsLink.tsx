// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React, { useContext } from "react";
import { Link } from "react-router-dom";

import { CockroachCloudContext } from "../../contexts";
import { EncodeUriName } from "../../util";

type Props = {
  dbName: string;
  escSchemaQualifiedTableName: string;
  indexName: string;
};
export const IndexStatsLink: React.FC<Props> = ({
  dbName,
  escSchemaQualifiedTableName,
  indexName,
}) => {
  const isCockroachCloud = useContext(CockroachCloudContext);

  const linkUrl = isCockroachCloud
    ? `/databases/${encodeURIComponent(dbName)}/null/${encodeURIComponent(escSchemaQualifiedTableName)}/${EncodeUriName(indexName)}`
    : `/database/${encodeURIComponent(dbName)}/table/${encodeURIComponent(escSchemaQualifiedTableName)}/index/${EncodeUriName(indexName)}`;
  return <Link to={linkUrl}>{indexName}</Link>;
};
