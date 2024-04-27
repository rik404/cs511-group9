####################################################################################
# DO NOT MODIFY THE BELOW ##########################################################

FROM cs511p1-common

# DO NOT MODIFY THE ABOVE ##########################################################
####################################################################################

RUN mkdir /namenode
RUN mkdir /datanode
COPY ./hdfs-site-master.xml $HADOOP_HOME/etc/hadoop/hdfs-site.xml
COPY ./core-site-master.xml $HADOOP_HOME/etc/hadoop/core-site.xml

COPY ./spark-env.sh /opt/spark/conf/spark-env.sh
COPY ./spark-defaults.conf /opt/spark/conf/spark-defaults.conf

COPY ./setup-main.sh ./setup-main.sh
RUN /bin/bash setup-main.sh

COPY ./start-main.sh ./start-main.sh
CMD ["/bin/bash", "start-main.sh"]