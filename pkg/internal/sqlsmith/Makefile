# Use FORCE here because we want to regenerate when this makefile changes.
gen-sampler-gen.go: FORCE
	genny -in=sampler.go -out=gen-sampler.go gen "element=statement,tableExpr,selectStatement,scalarExpr"
	../../../bin/crlfmt -w gen-sampler.go >/dev/null
FORCE:
