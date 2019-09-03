import pytest
import grpc 
import random
import datetime
import itertools
from hydro_serving_grpc import writer
from hydro_serving_grpc import monitoring


features = ["age", "sex", "salary", "confidence", "weight", "height"]
description = ["< max", "> max", "custom threshold > 0.5", "custom model < 0.5"]


def one_message_iter():
    while True:
        yield writer.WriteOneRequest(
            model_version=random.randint(1, 10),
            trace_data=monitoring.TraceData(
                ts=int(datetime.datetime.utcnow().timestamp()),
                uid=random.randint(1, 500),
            ),
            feature=random.choice(features),            
            value=random.random() * random.randint(1, 25),
            description=random.choice(description),
            check=True,
        )


def many_message_iter():
    i = one_message_iter()
    while True:
        k = random.randint(3, 15)
        yield writer.WriteManyRequest(requests=[next(i) for _ in range(k)])

@pytest.fixture()
def stub():
    channel = grpc.insecure_channel('localhost:50051')
    return writer.WriterStub(channel)


@pytest.fixture()
def one_messages():
    return iter(one_message_iter())


@pytest.fixture()
def many_messages():
    return iter(many_message_iter())


def test_one_message(stub, one_messages):
    stub.WriteOne(next(one_messages))

def test_one_messages(stub, one_messages):
    for _ in range(1000):
        stub.WriteOne(next(one_messages))

def test_many_message(stub, many_messages):
    stub.WriteMany(next(many_messages))

def test_many_messages(stub, many_messages):
    for _ in range(1000):
        stub.WriteMany(next(many_messages))
