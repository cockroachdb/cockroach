# This Makefile derives the HTML for the SQL work item overview,
# and ensure the HTML code embeds all the SVG diagrams with the
# proper cross-references.

SOURCE = ../sql_work_items.rst
TARGET = ../sql_work_items.html

# Path to docutil's rst2html. On macOS this may need to be
# overridden:
#    make RST2HTML=rst2html5.py
RST2HTML = rst2html5

all: $(TARGET)

clean:
	rm -f *.dot *.dot.p *.svg
	rm -f items.rst $(TARGET)

DEPS := items $(shell grep 'workitem::' <$(SOURCE)|cut -d: -f3)
.SUFFIXES: .dot .dot.p .svg .embed.svg .rst .html

items.rst: $(SOURCE) extract.awk
	awk -f extract.awk $< >$@.tmp
	mv -f $@.tmp $@

$(DEPS:%=%.dot): items.rst

%.dot: %.dot.p
	cpp -P $< >$@.tmp
	mv -f $@.tmp $@

%.svg: %.dot
	dot -Tsvg -o $@.tmp $<
	mv -f $@.tmp $@

%.embed.svg: %.svg
	echo "<div id='svg-container-$*'>" >$@.tmp
	tail -n +7 $< | sed -e "s/<svg/<svg id='svg-$*'/g" >>$@.tmp
	echo "</div>" >>$@.tmp
	mv -f $@.tmp $@

$(TARGET): items.rst $(DEPS:%=%.embed.svg) narrow.css scrollsvg.awk
	$(RST2HTML) --table-style=borderless --stylesheet-path=minimal.css,plain.css,narrow.css $< | \
	  awk -f scrollsvg.awk >$@.tmp
	mv -f $@.tmp $@
