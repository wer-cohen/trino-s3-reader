package S3Parquet;

import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;

public class S3ParquetSplitSource
        implements ConnectorSplitSource
{
    private final S3ParquetSplit split;
    private boolean done;

    public S3ParquetSplitSource(S3ParquetSplit split)
    {
        this.split = requireNonNull(split, "split is null");
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(int maxSize)
    {
        if (done) {
            return CompletableFuture.completedFuture(new ConnectorSplitBatch(List.of(), true));
        }
        done = true;
        List<ConnectorSplit> splits = new ArrayList<>();
        splits.add(split);
        return CompletableFuture.completedFuture(new ConnectorSplitBatch(splits, true));
    }

    @Override
    public void close()
    {
        // nothing to clean up
    }

    @Override
    public boolean isFinished()
    {
        return done;
    }
}
