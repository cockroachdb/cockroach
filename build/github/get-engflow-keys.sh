#!/usr/bin/env bash

set -euxo pipefail

gcloud secrets versions access 1 --secret=engflow-mesolite-key > /home/agent/engflow.key
gcloud secrets versions access 1 --secret=engflow-mesolite-crt > /home/agent/engflow.crt
