FROM gcr.io/datamechanics/spark:platform-3.1-dm14

ENV PYSPARK_MAJOR_PYTHON_VERSION=3
WORKDIR /opt/application/

COPY requirements.txt .
RUN pip3 install -r requirements.txt

COPY main.py .

CMD [ "python", "trend-analysis/main.py" ]