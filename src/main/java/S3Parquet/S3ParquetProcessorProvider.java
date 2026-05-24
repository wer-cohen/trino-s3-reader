package S3Parquet;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;
import io.trino.spi.function.table.TableFunctionSplitProcessor;

public class S3ParquetProcessorProvider
        implements TableFunctionProcessorProvider
{
    @Override
    public TableFunctionSplitProcessor getSplitProcessor(
            ConnectorSession session,
            ConnectorTableFunctionHandle handle,
            ConnectorSplit split)
    {
        S3ParquetSplit s3Split = (S3ParquetSplit) split;
        return new S3ParquetSplitProcessor(s3Split);
    }
}
