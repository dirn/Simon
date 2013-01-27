SHELL := /bin/bash

init:
	python setup.py develop

integration:
	nosetests -s tests/integration.py

test:
	nosetests -s tests

clean:
	git clean -Xfd
