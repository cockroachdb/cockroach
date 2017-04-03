.PHONY: bigtest
bigtest:
	go test -bigtest -timeout 24h $(TESTFLAGS) -run '^TestLogic$$'

.PHONY: bigtest-distsql
bigtest-distsql:
	go test -bigtest -config distsql -timeout 24h $(TESTFLAGS) -run '^TestLogic$$'
