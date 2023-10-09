FROM apache/spark-py:v3.4.0

WORKDIR /src

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

CMD [ "python", "trend-analysis/main.py" ]