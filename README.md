# trino-s3-reader
trino catalog plugin that allows querying parquet file directly from s3 without creating a table or using metastore

## setup
copy trino-s3-parquet-1.0-SNAPSHOT.jar from /target folder into the /plugins/ReadS3File, copy all the jars inside the dependecies folder to the same folder.
add new catalog with the following configuration:

```
connector.name=s3parquet
```

restart yout trino cluster.

## how to use
assuming we setup catalog read_s3_parquet:
```
select * from table(read_s3_parquet.system.read_s3_parquet(endpoint => 's3 url', access_key => 's3 access key', secret_key => 's3 secert key', bucket => 's3 bucket', key => 'parquet file path'))
```

### notes
-  tested on trino version 477 and minio
-  java version 24
