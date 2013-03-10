SHELL := /bin/bash

init:
	python setup.py develop

html:
	(cd docs && $(MAKE) html)

integration:
	nosetests -s tests/integration.py

test:
	nosetests -s tests

clean:
	git clean -Xfd
