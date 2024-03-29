####################################################################################
# DO NOT MODIFY THE BELOW ##########################################################

FROM openjdk:8

RUN apt update && \
    apt upgrade --yes && \
    apt install ssh openssh-server --yes

# Setup common SSH key.
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/shared_rsa -C common && \
    cat ~/.ssh/shared_rsa.pub >> ~/.ssh/authorized_keys && \
    chmod 0600 ~/.ssh/authorized_keys

# DO NOT MODIFY THE ABOVE ##########################################################
####################################################################################

# Setup HDFS/Spark resources here
ENV HADOOP_VERSION 3.3.6
ENV HADOOP_HOME /usr/local/hadoop
ENV HADOOP_OPTS -Djava.library.path=/usr/local/hadoop/lib/native

ENV SPARK_VERSION 3.4.1
ENV SPARK_HOME /usr/local/spark

ENV PATH $PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin

#REMOVE THEEEEEEEEEEEEEEEESEEEEEEEEEE
COPY hadoop-3.3.6.tar.gz /hadoop-3.3.6.tar.gz

COPY spark-3.4.1-bin-hadoop3.tgz /spark-3.4.1-bin-hadoop3.tgz

#COPY flat_line_item.csv /flat_line_item.csv

RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean && \
    apt-get update && \
    apt-get install -y wget && \
    # wget https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz && \
    tar -zxf /hadoop-$HADOOP_VERSION.tar.gz && \
    rm /hadoop-$HADOOP_VERSION.tar.gz && \
    mv hadoop-$HADOOP_VERSION $HADOOP_HOME && \
    mkdir -p $HADOOP_HOME/logs

RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean && \
    apt-get update && \
    # wget https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz && \
    tar -zxf /spark-$SPARK_VERSION-bin-hadoop3.tgz && \
    rm /spark-$SPARK_VERSION-bin-hadoop3.tgz && \
    mv spark-$SPARK_VERSION-bin-hadoop3 $SPARK_HOME && \
    mkdir -p $SPARK_HOME/logs

COPY hadoop-resources $HADOOP_HOME/etc/hadoop/

RUN mkdir -p /mongo-jars

# COPY mongo-resources /mongo-jars/

RUN hdfs namenode -format

EXPOSE 50010 50020 50070 50075 50090 8020 9000 9001

RUN --mount=target=/var/lib/apt/lists,type=cache,sharing=locked \
    --mount=target=/var/cache/apt,type=cache,sharing=locked \
    rm -f /etc/apt/apt.conf.d/docker-clean && \
    apt-get install -y python3 && \
    apt-get install -y python3-pip

COPY pyspark.tar.gz pyspark.tar.gz

RUN pip install pyspark.tar.gz

# RUN pip install pandas