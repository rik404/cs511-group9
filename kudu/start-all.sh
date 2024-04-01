#!/bin/bash
export KUDU_QUICKSTART_IP=$(ifconfig | grep "inet " | grep -Fv 127.0.0.1 |  awk '{print $2}' | tail -1) && \

docker build -t cs511p1-common -f cs511p1-common.Dockerfile . && \
    docker build -t cs511p1-main -f cs511p1-main.Dockerfile . && \
    docker build -t cs511p1-worker -f cs511p1-worker.Dockerfile .
    # docker build -t apache-kudu -f kudu.Dockerfile . && \
    docker compose -f docker/quickstart.yml up
