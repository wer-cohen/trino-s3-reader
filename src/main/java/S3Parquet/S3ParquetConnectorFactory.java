package S3Parquet;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class S3ParquetConnectorFactory
        implements ConnectorFactory
{
    @Override
    public String getName()
    {
        return "s3parquet";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> requiredConfig, ConnectorContext context)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        return new S3ParquetConnector();
    }
}
