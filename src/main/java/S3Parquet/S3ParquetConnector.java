package S3Parquet;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunction;
import io.trino.spi.transaction.IsolationLevel;

import java.util.Optional;
import java.util.Set;

public class S3ParquetConnector
        implements Connector
{
    @Override
    public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly, boolean autoCommit)
    {
        return S3ParquetTransactionHandle.INSTANCE;
    }

    @Override
    public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transactionHandle)
    {
        return new ConnectorMetadata() {};
    }

    @Override
    public io.trino.spi.connector.ConnectorSplitManager getSplitManager()
    {
        return new S3ParquetSplitManager();
    }

    @Override
    public Set<ConnectorTableFunction> getTableFunctions()
    {
        return Set.of(new ReadS3ParquetFunction());
    }

    @Override
    public Optional<FunctionProvider> getFunctionProvider()
    {
        return Optional.of(new S3ParquetFunctionProvider());
    }
}
