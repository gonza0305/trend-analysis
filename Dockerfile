FROM apache/spark-py:latest

COPY . .

CMD [ "python", "trend-analysis/main.py" ]