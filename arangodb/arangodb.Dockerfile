FROM arangodb

ENV ARANGO_ROOT_PASSWORD=cs511grp9

RUN mkdir -p /data/db 

EXPOSE 8529

