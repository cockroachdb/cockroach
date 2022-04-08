#!/bin/bash

roachprod destroy local || true
roachprod create -n 5 local
roachprod put local cockroach
roachprod start local
tail -F ~/local/*/logs/cockroach.log


