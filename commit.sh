#!/bin/bash

set -ex

git config --global user.name 'github action'

if [ -n "$(git status --porcelain)" ]; then
	echo "there are changes";
	git add -f dist/*.whl
	git commit -m 'publish wheel by github action'
	git push
else
	echo "no changes";
fi

