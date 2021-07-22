# short commit hash
GIT_COMMIT=`git rev-parse --short HEAD`

.DEFAULT_GOAL := help

help:
	@echo "Build targets:"
	@echo "- clean:                     cleans the build directory"
	@echo "- build_wheel:          		builds wheel package locally"

clean:
	rm -rf build/*

build_local:
	python setup.py sdist bdist_wheel

