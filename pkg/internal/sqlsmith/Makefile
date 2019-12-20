# Use FORCE here because we want to regenerate when this makefile changes.
gen-sampler-gen.go: FORCE
	genny -in=sampler.tmpl -out=sampler.go gen "element=statement,tableExpr,selectStatement,scalarExpr"
	../../../bin/crlfmt -w sampler.go >/dev/null
FORCE:
