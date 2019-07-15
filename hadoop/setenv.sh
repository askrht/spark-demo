#!/bin/bash
cd ${HADOOP_PREFIX} && \
  bin/hdfs dfs -mkdir /datain && \
  bin/hdfs dfs -chmod 777 /datain
  bin/hdfs dfs -mkdir /dataout && \
  bin/hdfs dfs -chmod 777 /dataout && \
  bin/hdfs dfs -put /work/datain/customers.dat /datain/customers.dat && \
  bin/hdfs dfs -put /work/datain/sales.dat /datain/sales.dat
