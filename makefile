test:
	./hash_simple.py
	./hash_multiple.py
	./vectorclock.py

COVERAGE_FILES=$(wildcard *.py)
coverage: 
	python-coverage -e
	@list='$(COVERAGE_FILES)'; for pyfile in $$list; do \
	  python-coverage -c $$pyfile; \
	done
	python-coverage -m -r $(COVERAGE_FILES)

slap:
	slap *.py

clean: 
	find . -name \*.pyc | xargs rm -f
	find . -name \*,cover | xargs rm -f
