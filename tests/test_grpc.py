import pytest
import grpc 
import random
import datetime
import itertools
from hydro_serving_grpc import writer


features = ["age", "sex", "salary", "confidence", "weight", "height"]
description = ["< max", "> max", "custom threshold > 0.5", "custom model < 0.5"]


@pytest.fixture()
def stub():
    channel = grpc.insecure_channel('localhost:50051')
    return writer.WriterStub(channel)


@pytest.fixture()
def one_messages():
    def iterator():
        while True:
            yield writer.WriteOneRequest(
                model_version=random.randint(1, 10),
                feature=random.choice(features),
                check=True,
                timestamp=int(datetime.datetime.utcnow().timestamp()),
                uid=random.randint(1, 500),
                value=random.random() * random.randint(1, 25),
                description=random.choice(description),
            )
    return iter(iterator())


@pytest.fixture()
def many_messages():
    def iterator():
        while True: 
            k = random.randint(3, 15)
            yield writer.WriteManyRequest(
                model_version=random.choices(range(1, 10), k=k),
                feature=random.choices(features, k=k),
                check=itertools.repeat(True, k),
                timestamp=itertools.repeat(1566388674598, k),
                uid=random.choices(range(100), k=k),
                value=random.choices(range(100), k=k),
                description=random.choices(description, k=k),
            )
    return iter(iterator())


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
