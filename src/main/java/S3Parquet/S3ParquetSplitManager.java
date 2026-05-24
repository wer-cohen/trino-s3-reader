package S3Parquet;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;

public class S3ParquetSplitManager
        implements ConnectorSplitManager
{
    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableFunctionHandle function)
    {
        if (function instanceof ReadS3ParquetFunctionHandle handle) {
            S3ParquetSplit split = new S3ParquetSplit(
                    handle.getEndpoint(),
                    handle.getAccessKey(),
                    handle.getSecretKey(),
                    handle.getBucket(),
                    handle.getKey()
            );
            return new S3ParquetSplitSource(split);
        }
        throw new IllegalArgumentException("Unknown table function handle: " + function.getClass().getName());
    }
}
