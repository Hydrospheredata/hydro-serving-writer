FROM python:3.7-slim

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY hydro_serving_writer ./hydro_serving_writer
COPY tests ./tests
ENTRYPOINT [ "python", "-m", "hydro_serving_writer.grpc_.server" ]