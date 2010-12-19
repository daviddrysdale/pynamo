test:
	./hash_simple.py
	./hash_multiple.py
	./vectorclock.py

COVERAGE_FILES=$(wildcard *.py)
coverage: 
	python-coverage erase
	@list='$(COVERAGE_FILES)'; for pyfile in $$list; do \
	  python-coverage run -p $$pyfile; \
	done
	python-coverage combine
	python-coverage report -m $(COVERAGE_FILES)

slap:
	slap *.py

clean: 
	rm -f *.pyc *,cover .coverage dynamo.log
