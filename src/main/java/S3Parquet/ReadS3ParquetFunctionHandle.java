package S3Parquet;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class ReadS3ParquetFunctionHandle
        implements ConnectorTableFunctionHandle
{
    private final String endpoint;
    private final String accessKey;
    private final String secretKey;
    private final String bucket;
    private final String key;

    @JsonCreator
    public ReadS3ParquetFunctionHandle(
            @JsonProperty("endpoint") String endpoint,
            @JsonProperty("accessKey") String accessKey,
            @JsonProperty("secretKey") String secretKey,
            @JsonProperty("bucket") String bucket,
            @JsonProperty("key") String key)
    {
        this.endpoint = requireNonNull(endpoint, "endpoint is null");
        this.accessKey = requireNonNull(accessKey, "accessKey is null");
        this.secretKey = requireNonNull(secretKey, "secretKey is null");
        this.bucket = requireNonNull(bucket, "bucket is null");
        this.key = requireNonNull(key, "key is null");
    }

    @JsonProperty
    public String getEndpoint()
    {
        return endpoint;
    }

    @JsonProperty
    public String getAccessKey()
    {
        return accessKey;
    }

    @JsonProperty
    public String getSecretKey()
    {
        return secretKey;
    }

    @JsonProperty
    public String getBucket()
    {
        return bucket;
    }

    @JsonProperty
    public String getKey()
    {
        return key;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ReadS3ParquetFunctionHandle that = (ReadS3ParquetFunctionHandle) o;
        return Objects.equals(endpoint, that.endpoint)
                && Objects.equals(accessKey, that.accessKey)
                && Objects.equals(secretKey, that.secretKey)
                && Objects.equals(bucket, that.bucket)
                && Objects.equals(key, that.key);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(endpoint, accessKey, secretKey, bucket, key);
    }
}
