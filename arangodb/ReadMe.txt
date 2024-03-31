Copy the file to the container by mentioning in the common dockerfile
Make the changes to file name in docker-entrypoint.sh
Start the containers using start-all.sh
RUN: docker-compose -f cs511p1-compose.yaml exec main bash
On main run: bash ./code/docker-entrypoint.sh 