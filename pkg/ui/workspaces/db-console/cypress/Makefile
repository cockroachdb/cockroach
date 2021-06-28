
.PHONY: default teardown decommission-node stop-node start-node

PROJECT_ROOT := ../../..
ROACHPROD_EXEC := $(PROJECT_ROOT)/bin/roachprod
COCKROACH_EXEC := $(PROJECT_ROOT)/cockroach

export PATH := $(PROJECT_ROOT):$(PATH)

default:
	$(ROACHPROD_EXEC) create local --nodes 4
	$(ROACHPROD_EXEC) start local
	# Decrease delay for recognizing unavailable node as dead (unfortunately 1m15s is min allowed value)
	$(ROACHPROD_EXEC) sql local:1 -- -e "SET CLUSTER SETTING server.time_until_store_dead = '1m15s';"
	$(ROACHPROD_EXEC) sql local:1 -- -e "CREATE USER craig WITH PASSWORD 'cockroach';"

decommission-node:
	$(COCKROACH_EXEC) node decommission $(NODE_ID) --insecure

stop-node:
	$(ROACHPROD_EXEC) stop local:$(NODE_ID)

decommission:
	$(COCKROACH_EXEC) node decommission $(NODE_ID) --insecure

teardown:
	$(ROACHPROD_EXEC) destroy local
