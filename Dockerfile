FROM apache/spark-py:v3.4.0

WORKDIR /src

COPY . .

CMD [ "python", "trend-analysis/main.py" ]