export KUDU_QUICKSTART_IP=$(ifconfig | grep "inet " | grep -Fv 127.0.0.1 |  awk '{print $2}' | tail -1) && \
docker compose -f docker/quickstart.yml cp flat_line_item.csv main:/flat_line_item.csv && \
docker-compose -f docker/quickstart.yml exec main bash -x -c 'spark-shell --packages org.apache.kudu:kudu-spark3_2.12:1.17.0 --driver-memory 1G'