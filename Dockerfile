FROM python:3.7-slim

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY app.py ./app.py
ENTRYPOINT [ "python", "-u", "./app.py" ]
