package S3Parquet;

import io.trino.spi.connector.ConnectorTransactionHandle;

public enum S3ParquetTransactionHandle
        implements ConnectorTransactionHandle
{
    INSTANCE
}
