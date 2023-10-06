FROM python:3.10

RUN apt-get update

WORKDIR /src

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

CMD [ "python", "trend-analysis/main.py" ]