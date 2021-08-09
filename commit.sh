#!/bin/bash

set -ex

git config --global user.name 'github action'
if [[ $(git log -n 1 --format=format:%an) == "github action" ]]; then
    echo "previous commit is by github action, skipping commit"
    exit 0
fi
if [ -n "$(git status --porcelain)" ]; then
	echo "there are changes";
	git add package/*
	git commit -m 'publish wheel by github action'
	git push
else
	echo "no changes";
fi
