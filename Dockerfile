# Use a base image with Python
FROM apache/spark-py

# Install PySpark
# RUN pip3 install pyspark
ENV PYTHONUSERBASE /tmp/.local
RUN pip3 install --user pyspark
RUN pip3 install --user findspark
RUN pip3 install --user numpy

# Install Hadoop AWS dependencies
USER root
RUN wget -O /opt/spark/jars/hadoop-aws-3.3.4.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
RUN wget -O /opt/spark/jars/aws-java-sdk-1.12.558.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.12.558/aws-java-sdk-1.12.558.jar
RUN wget -O /opt/spark/jars/aws-java-sdk-core-1.12.558.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-core/1.12.558/aws-java-sdk-core-1.12.558.jar
RUN wget -O /opt/spark/jars/aws-java-sdk-bundle-1.12.558.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.558/aws-java-sdk-bundle-1.12.558.jar

# Specify S3A as the file system
ENV SPARK_CONF_DIR=/conf
RUN mkdir -p $SPARK_CONF_DIR
RUN echo "spark.hadoop.fs.s3a.access.key $AWS_ACCESS_KEY_ID" > $SPARK_CONF_DIR/spark-defaults.conf
RUN echo "spark.hadoop.fs.s3a.secret.key $AWS_SECRET_ACCESS_KEY" >> $SPARK_CONF_DIR/spark-defaults.conf


COPY trend-analysis/main.py .
COPY trend-analysis/main.py /opt/spark/work-dir/
CMD [ "python", "trend-analysis/main.py" ]
