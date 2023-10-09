FROM python:3.10

RUN apt-get update
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64
RUN wget https://apache.mirror.digionline.de/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz
RUN tar -xzvf spark-3.1.2-bin-hadoop3.2.tgz
RUN mv spark-3.1.2-bin-hadoop3.2 /opt/spark
ENV SPARK_HOME /opt/spark
ENV PATH $SPARK_HOME/bin:$PATH

RUN pip install pyspark

WORKDIR /src

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

CMD [ "python", "trend-analysis/main.py" ]