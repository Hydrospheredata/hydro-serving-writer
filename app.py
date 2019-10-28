import pandas as pd
import pymongo
from pymongo import MongoClient
import pyarrow as pa
import pyarrow.parquet as pq
import bson
import s3fs
import os

MONGO_HOST = os.environ.get('MONGO_HOST', 'localhost')
MONGO_PORT = os.environ.get('MONGO_PORT', 27017)
MONGO_DB = os.environ.get('MONGO_DB', 'sonar-profiles')
MONGO_USER = os.environ.get('MONGO_USER')
MONGO_PASS = os.environ.get('MONGO_PASS')
MONGO_AUTH_DB = os.environ.get('MONGO_AUTH_DB')

print("and here we go")

def _connect_mongo():
    """ A util for making a connection to mongo """

    if MONGO_USER and MONGO_PASS:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (MONGO_USER, MONGO_PASS, MONGO_HOST, MONGO_PORT, MONGO_DB)
        if MONGO_AUTH_DB is not None:
            mongo_uri = '%s?authSource=%s' % (mongo_uri, MONGO_AUTH_DB)
        print("Connecting to %s" % (mongo_uri,))
        conn = MongoClient(mongo_uri)
    else:
        print("Connecting to %s:%s" % (MONGO_HOST, MONGO_PORT))
        conn = MongoClient(MONGO_HOST, MONGO_PORT)


    return conn[MONGO_DB]


def read_mongo(collection, query={}):
    """ Read from Mongo and Store into DataFrame """

    # Connect to MongoDB
    db = _connect_mongo()

    # Make a query to the specific DB and Collection
    cursor = db[collection].find(query)

    # Expand the cursor and construct the DataFrame
    df =  pd.DataFrame(list(cursor))

    return df.astype({'_id': 'str'}) if len(df) > 0 else df

try:
    db = _connect_mongo()
#    cursor = db.aggregated_check.changes([{'$match': {'operationType': 'insert'}}])
#    for change in cursor:
#        print(change)
    fs = s3fs.S3FileSystem()
    bucket = 'hydroserving-dev-feature-lake'
    path = ''
    bucket_uri = f's3://{bucket}'
    with db.aggregated_check.watch([{'$match': {'operationType': 'insert'}}]) as stream:
        print("Waiting for changes...")
        for insert_change in stream:
            print(insert_change)
            model_version_id = insert_change['fullDocument']['_hs_model_version_id']
            model_name = insert_change['fullDocument']['_hs_model_name']
            from_id = insert_change['fullDocument']['_hs_first_id']
            #print({'_id': {'$lt': bson.objectid.ObjectId(from_id)}, '_hs_model_version_id': model_version_id})
            df = read_mongo('checks', {'_id': {'$lt': bson.objectid.ObjectId(from_id)}, '_hs_model_version_id': model_version_id})
            if len(df) == 0:
                print("empty collection")
                continue
            df1 = df.astype({'_hs_model_incremental_version': 'int64'})._hs_raw_checks.apply(pd.Series)
            df1.rename(columns=lambda x: "_hs_raw_checks_" + x, inplace=True)
            def f(field):
                def f1(x):
                    y = list(map(lambda m: (int(m[field]) if type(m[field]) is bool else m[field]) if field in m else None, x))
                    return y
                return f1
            for k in df1.keys():
                if k.startswith("_hs_raw_checks"):
                    for key in ["check", "description", "threshold", "value", "metricSpecId"]:
                       df1[k + "_" + key] = df1[k].map(f(key))
                    df1 = df1.drop(k, axis=1)
            df2 = pd.concat([df1, df.drop('_hs_raw_checks', axis=1)], axis=1)
            table = pa.Table.from_pandas(df2)
            pq.write_to_dataset(table, root_path=bucket_uri + '/' + model_name, partition_cols=['_hs_model_incremental_version'], filesystem=fs)
            db = _connect_mongo()
            db.checks.delete_many({'_id': { '$in':list( map(lambda x: bson.objectid.ObjectId(x), df2['_id'].tolist()))}})

except pymongo.errors.PyMongoError as e:
    # The ChangeStream encountered an unrecoverable error or the
    # resume attempt failed to recreate the cursor.
    print(e)
