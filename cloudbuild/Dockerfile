# This Dockerfile creates an image for running presubmit tests.
FROM openjdk:8
RUN apt-get update && apt-get install -y gcc python-dev python-setuptools libffi-dev
RUN apt-get install -y python3-pip
RUN pip install gsutil
