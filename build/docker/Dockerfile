FROM 977887051160.dkr.ecr.ap-southeast-2.amazonaws.com/python:3.8-20201217 AS base
WORKDIR /opt/utils

ADD setup.py setup.py
ADD README.md README.md
ADD dbricks_utils dbricks_utils
RUN pip install .

FROM base AS snyk-cli

ADD https://github.com/snyk/snyk/releases/download/v1.367.0/snyk-linux /usr/local/bin/snyk
RUN chmod a+x /usr/local/bin/snyk

FROM base AS whl-cli
RUN python setup.py sdist bdist_wheel
