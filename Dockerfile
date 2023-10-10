# Use a base image with Python
FROM gcr.io/datamechanics/spark:platform-3.1-dm14

# Install PySpark
RUN pip3 install pyspark
RUN pip3 install findspark

COPY main.py .
ENV AWS_ACCESS_KEY_ID=AKIAVP5IO2RVCXZKULAT
ENV AWS_SECRET_ACCESS_KEY=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
CMD [ "python", "main.py" ]
