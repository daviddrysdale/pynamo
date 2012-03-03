# Ideally, want Pygments installed (sudo easy_install Pygments)

# Python files that are included in the doc
INCLUDED_PY_FILES=hash_simple.py hash_multiple.py vectorclock.py vectorclockt.py
# Python files that run as tests
TEST_FILES=hash_simple.py hash_multiple.py vectorclock.py vectorclockt.py merkle.py test_dynamo.py
COVERAGE_FILES=$(TEST_FILES)
# All files
ALL_PY_FILES=$(wildcard *.py)
ALL_FILES=makefile preprocess pynamo_src.html pygments.css $(ALL_PY_FILES)
# Coverage; requires coverage module
COVERAGE=$(shell hash python-coverage 2>&- && echo python-coverage || echo coverage)

all: pynamo.html tar

pynamo.html: preprocess pynamo_src.html $(INCLUDED_PY_FILES) pynamo.tgz
	preprocess pynamo_src.html > $@

test:
	@list='$(TEST_FILES)'; for pyfile in $$list; do \
	  python $$pyfile; \
	done

coverage: coverage_clean coverage_generate coverage_report
coverage_clean:
	$(COVERAGE) -e
coverage_generate:
	@list='$(TEST_FILES)'; for pyfile in $$list; do \
	  $(COVERAGE) -x $$pyfile; \
	done
coverage_report:
	$(COVERAGE) -m -r $(COVERAGE_FILES)
coverage_annotate:
	$(COVERAGE) annotate $(COVERAGE_FILES)

tar: pynamo.tgz
pynamo.tgz: $(ALL_FILES)
	tar czf $@ $^

clean: 
	rm -f *.pyc *,cover .coverage* pynamo.log* pynamo.html

lint:
	pyflakes *.py
	pep8 --repeat --ignore E501 *.py