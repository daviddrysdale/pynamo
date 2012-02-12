# Ideally, want Pygments installed (sudo easy_install Pygments)

# Python files that are included in the doc
INCLUDED_PY_FILES=hash_simple.py hash_multiple.py vectorclock.py vectorclockt.py
# Python files that run as tests
TEST_FILES=hash_simple.py hash_multiple.py vectorclock.py vectorclockt.py merkle.py test_dynamo.py
# All files
ALL_PY_FILES=$(wildcard *.py)
ALL_FILES=makefile preprocess pynamo_src.html pygments.css $(ALL_PY_FILES)


all: pynamo.html tar

pynamo.html: preprocess pynamo_src.html $(INCLUDED_PY_FILES) pynamo.tgz
	preprocess pynamo_src.html > $@

test:
	@list='$(TEST_FILES)'; for pyfile in $$list; do \
	  python $$pyfile; \
	done

coverage: 
	python-coverage erase
	@list='$(TEST_FILES)'; for pyfile in $$list; do \
	  python-coverage run -p $$pyfile; \
	done
	python-coverage combine
	python-coverage report -m $(COVERAGE_FILES)

tar: pynamo.tgz
pynamo.tgz: $(ALL_FILES)
	tar czf $@ $^

clean: 
	rm -f *.pyc *,cover .coverage pynamo.log* pynamo.html

lint:
	pyflakes *.py
	pep8 --repeat --ignore E501 *.py