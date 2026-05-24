package S3Parquet;

import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.function.table.AbstractConnectorTableFunction;
import io.trino.spi.function.table.Argument;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.function.table.ScalarArgument;
import io.trino.spi.function.table.ScalarArgumentSpecification;
import io.trino.spi.function.table.TableFunctionAnalysis;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class ReadS3ParquetFunction
        extends AbstractConnectorTableFunction
{
    public ReadS3ParquetFunction()
    {
        super(
                "system",
                "read_s3_parquet",
                List.of(
                        ScalarArgumentSpecification.builder()
                                .name("ENDPOINT")
                                .type(VARCHAR)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name("ACCESS_KEY")
                                .type(VARCHAR)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name("SECRET_KEY")
                                .type(VARCHAR)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name("BUCKET")
                                .type(VARCHAR)
                                .build(),
                        ScalarArgumentSpecification.builder()
                                .name("KEY")
                                .type(VARCHAR)
                                .build()),
                io.trino.spi.function.table.ReturnTypeSpecification.GenericTable.GENERIC_TABLE);
    }

    @Override
    public TableFunctionAnalysis analyze(
            ConnectorSession session,
            ConnectorTransactionHandle transaction,
            Map<String, Argument> arguments,
            ConnectorAccessControl accessControl)
    {
        String endpoint = ((io.airlift.slice.Slice) ((ScalarArgument) arguments.get("ENDPOINT")).getValue()).toStringUtf8();
        String accessKey = ((io.airlift.slice.Slice) ((ScalarArgument) arguments.get("ACCESS_KEY")).getValue()).toStringUtf8();
        String secretKey = ((io.airlift.slice.Slice) ((ScalarArgument) arguments.get("SECRET_KEY")).getValue()).toStringUtf8();
        String bucket = ((io.airlift.slice.Slice) ((ScalarArgument) arguments.get("BUCKET")).getValue()).toStringUtf8();
        String key = ((io.airlift.slice.Slice) ((ScalarArgument) arguments.get("KEY")).getValue()).toStringUtf8();

        Descriptor returnedType = new Descriptor(List.of(
                new Descriptor.Field("id", Optional.of(BIGINT)),
                new Descriptor.Field("session_id", Optional.of(BIGINT))));

        return TableFunctionAnalysis.builder()
                .returnedType(returnedType)
                .handle(new ReadS3ParquetFunctionHandle(endpoint, accessKey, secretKey, bucket, key))
                .build();
    }
}
