.DEFAULT_GOAL := help

define PRINT_HELP_PYSCRIPT
import re, sys


HEADER = '\033[95m'
OKBLUE = '\033[94m'
OKGREEN = '\033[92m'
WARNING = '\033[93m'
FAIL = '\033[91m'
ENDCOLOR = '\033[0m'
BOLD = '\033[1m'
UNDERLINE = '\033[4m'

print(OKBLUE + "Welcome! You can run these make commands:\n" + ENDCOLOR)

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print(OKGREEN + "%-30s " % target + ENDCOLOR + "%s" % (help))
endef
export PRINT_HELP_PYSCRIPT

help: ## Get help messages
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)
.PHONY: help

github: ## Open the GitHub repo for this project
	open https://github.com/superconductive/cooper-pair
.PHONY: github

install: ## update your python environment
	pip install -r requirements.txt || echo 'No requirements.txt found'
	pip install -r dev-requirements.txt || echo 'No dev-requirements.txt found'
	pip install -e .
	pip list
.PHONY: install-requirements

notebooks: ## Run juypter lab for your notebooks
	jupyter lab
.PHONY: notebooks

clean: ## Clean out python cache files
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -type d | xargs rm -fr
	rm -fr docs/_build/
	rm -fr dist/*
.PHONY: clean

dist: clean ## builds source and wheel package
	python setup.py sdist
	python setup.py bdist_wheel
	ls -l dist
.PHONY: dist

release: dist ## package and upload a release
# TODO using TEST pypi for now
#	twine upload dist/*
	twine upload --repository-url https://test.pypi.org/legacy/ dist/*
.PHONY: release

venv: ## Create a new virtualenv in the repo root
	rm -rf venv
	virtualenv venv
.PHONY: venv

test: ## Run tests
	pytest -s tests
.PHONY: test
