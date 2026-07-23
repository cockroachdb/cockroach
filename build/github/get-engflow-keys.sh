#!/usr/bin/env bash

# Copyright 2023 The Cockroach Authors.
#
# Use of this software is governed by the CockroachDB Software License
# included in the /LICENSE file.


set -euxo pipefail

gcloud secrets versions access 2 --secret=engflow-mesolite-key --project=crl-github-actions > /home/agent/engflow.key
gcloud secrets versions access 2 --secret=engflow-mesolite-crt --project=crl-github-actions > /home/agent/engflow.crt
