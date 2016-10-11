all: sql.go keywords.go reserved_keywords.go

.PHONY: sql.go
sql.go: sql.y
	@./sql_union.sh

.PHONY: reserved_keywords.go
reserved_keywords.go: sql.y reserved_keywords.awk
	awk -f reserved_keywords.awk < sql.y > reserved_keywords.go
	gofmt -s -w reserved_keywords.go

.PHONY: keywords.go
keywords.go: sql.y all_keywords.awk
	awk -f all_keywords.awk < sql.y > keywords.go
	gofmt -s -w keywords.go

# This target will print unreserved_keywords which are not actually
# used in the grammar.
.PHONY: unused_unreserved_keywords
unused_unreserved_keywords:
	@for kw in $$(awk -f unreserved_keywords.awk < sql.y); do \
	  if [ $$(grep -c $${kw} sql.y) -le 2 ]; then \
	    echo $${kw}; \
	  fi \
	done
