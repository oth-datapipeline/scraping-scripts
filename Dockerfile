# syntax=docker/dockerfile:1

# first stage:
# enable venv and install dependencies into venv
# this is done to keep the final docker image size as small as possible

FROM python:3.9-slim-bullseye AS base

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY requirements.txt ./requirements.txt

RUN pip install -r requirements.txt

# next stage:
# setup of the into actual runner image

FROM python:3.9-slim-bullseye AS runner
ENV VIRTUAL_ENV=/opt/venv
ARG scraper_type
ARG base_url
WORKDIR /scraping_scripts/

# copy venv and its dependencies from the base image and enable venv
COPY --from=base $VIRTUAL_ENV $VIRTUAL_ENV
ENV PATH="/opt/venv/bin:$PATH"

# the entire repository is not necessary
# -» selectively copy config and sourcecode
COPY config.json ./config.json
COPY src/ ./src/

# check for entirety of required files
RUN ls -a
RUN ls -a src/

# check for running vevn
RUN which python
