package S3Parquet;

import io.trino.spi.function.FunctionProvider;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.function.table.TableFunctionProcessorProvider;

public class S3ParquetFunctionProvider
        implements FunctionProvider
{
    @Override
    public TableFunctionProcessorProvider getTableFunctionProcessorProvider(ConnectorTableFunctionHandle functionHandle)
    {
        if (functionHandle instanceof ReadS3ParquetFunctionHandle) {
            return new S3ParquetProcessorProvider();
        }
        throw new IllegalArgumentException("Unknown function handle: " + functionHandle);
    }
}
