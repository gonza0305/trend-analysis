FROM --platform=linux/amd64 public.ecr.aws/amazonlinux/amazonlinux:2 AS base

RUN yum install -y python3.7

ENV VIRTUAL_ENV=/opt/venv
RUN python3.7 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

COPY requirements.txt /
WORKDIR /

RUN python3.7 -m pip install --upgrade pip
RUN python3.7 -m pip install -r requirements.txt
RUN python3.7 -m pip install venv-pack

RUN mkdir /output && venv-pack -o /output/pyspark_venv.tar.gz

FROM scratch AS export
COPY --from=base /output/pyspark_venv.tar.gz /