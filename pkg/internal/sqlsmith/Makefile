# Use FORCE here because we want to regenerate when this makefile changes.
gen-sampler-gen.go: FORCE
	genny -in=sampler-gen.go -out=gen-sampler-gen.go gen "Element=statement,tableExpr"
FORCE:
