#!/usr/bin/env bash

# Copyright 2024 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.

set -o errexit
set -x

while getopts t:m: option
do
    case "${option}"
    in
        t) TAG=${OPTARG};;
        m) MESSAGE=${OPTARG};;
    esac
done

if [ -z "${TAG}" ];
then
    printf "Tag is unset\n"
    exit 1
fi
if [ -z "${MESSAGE}" ];
then
    printf "Message is empty\n"
    exit 1
fi

TOKEN_FILE="/etc/grafana_token"

if [ ! -f "${TOKEN_FILE}" ];
then
    printf "%s doesn't exist. Cannot add '%s' Grafana annotation.\n" "${TOKEN_FILE}" "${MESSAGE}"
    exit 1
fi

if [ "${ENVIRONMENT}" != "PROD" ];
then
    printf "Non-production environment, skipping annotation.\n"
    exit 2
fi


. "${TOKEN_FILE}"
GRAFANA_API="https://grafana.testeng.crdb.io/api/annotations"
EPOCH=$(($(gdate +%s%N)/1000000))

curl -H "Authorization: Bearer ${TOKEN}" \
     -H "Content-Type: application/json" \
     -X POST ${GRAFANA_API} \
     --data "{\"time\":${EPOCH},\"isRegion\":true,\"tags\":[\"${TAG}\"],\"text\":\"${MESSAGE}\",\"timeEnd\":${EPOCH}}"


if [ $? != 0 ];
then
    printf "Annotation failed\n"
fi
