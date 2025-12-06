// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

import { fetchDataJSON } from "src/api/fetchData";

import { useSwrWithClusterId } from "../util";

import { PaginationRequest, ResultsWithPagination } from "./types";

const FINGERPRINTS_API_V2 = "api/v2/fingerprints/";

export type StatementFingerprint = {
  fingerprint: string;
  query: string;
  summary: string;
  implicitTxn: boolean;
  database: string;
  createdAt: moment.Moment;
};

export type StatementFingerprintRequest = {
  search?: string;
  pagination: PaginationRequest;
};

type PaginatedStatementFingerprints = ResultsWithPagination<
  StatementFingerprint[]
>;

type StatementFingerprintServer = {
  fingerprint: string;
  query: string;
  summary: string;
  implicit_txn: boolean;
  database: string;
  created_at: string;
};

type StatementFingerprintsResponseServer = {
  results: StatementFingerprintServer[];
  pagination_info: {
    total_results: number;
    page_size: number;
    page_num: number;
  };
};

const convertStatementFingerprintFromServer = (
  fp: StatementFingerprintServer,
): StatementFingerprint => {
  return {
    fingerprint: fp.fingerprint,
    query: fp.query,
    summary: fp.summary,
    implicitTxn: fp.implicit_txn,
    database: fp.database,
    createdAt: moment(fp.created_at),
  };
};

export const getStatementFingerprints = async (
  req: StatementFingerprintRequest,
): Promise<PaginatedStatementFingerprints> => {
  const urlParams = new URLSearchParams();
  if (req.search) {
    urlParams.append("search", req.search);
  }
  if (req.pagination.pageSize) {
    urlParams.append("pageSize", req.pagination.pageSize.toString());
  }
  if (req.pagination.pageNum) {
    urlParams.append("pageNum", req.pagination.pageNum.toString());
  }

  return fetchDataJSON<StatementFingerprintsResponseServer, null>(
    FINGERPRINTS_API_V2 + "?" + urlParams.toString(),
  ).then(resp => ({
    results: resp.results?.map(convertStatementFingerprintFromServer) ?? [],
    pagination: {
      pageSize: resp.pagination_info.page_size,
      pageNum: resp.pagination_info.page_num,
      totalResults: resp.pagination_info.total_results,
    },
  }));
};

const createKey = (req: StatementFingerprintRequest) => {
  const {
    search,
    pagination: { pageSize, pageNum },
  } = req;
  return {
    name: "statementFingerprints",
    search,
    pageSize,
    pageNum,
  };
};

export const useStatementFingerprints = (req: StatementFingerprintRequest) => {
  const { data, error, isLoading, mutate } = useSwrWithClusterId(
    createKey(req),
    () => getStatementFingerprints(req),
    {
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
    },
  );

  return {
    data,
    error,
    isLoading,
    refreshFingerprints: mutate,
  };
};
