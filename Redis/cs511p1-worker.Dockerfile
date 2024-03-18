####################################################################################
# DO NOT MODIFY THE BELOW ##########################################################

FROM cs511p1-common

# DO NOT MODIFY THE ABOVE ##########################################################
####################################################################################

RUN mkdir /datanode
COPY ./hdfs-site-worker.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
COPY ./core-site-master.xml $HADOOP_HOME/etc/hadoop/core-site.xml

COPY ./spark-defaults.conf /opt/spark/conf/spark-defaults.conf

COPY ./setup-worker.sh ./setup-worker.sh
RUN /bin/bash setup-worker.sh

COPY ./start-worker.sh ./start-worker.sh
CMD ["/bin/bash", "start-worker.sh"]
