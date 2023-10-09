FROM bitnami/spark

WORKDIR /src

COPY . .

CMD [ "python", "trend-analysis/main.py" ]