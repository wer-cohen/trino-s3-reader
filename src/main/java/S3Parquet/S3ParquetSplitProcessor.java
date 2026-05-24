package S3Parquet;

import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;

import static io.trino.spi.type.BigintType.BIGINT;

public class S3ParquetSplitProcessor
        implements TableFunctionSplitProcessor
{
    private final S3ParquetSplit split;
    private File tempFile;
    private ParquetReader<GenericRecord> reader;
    private boolean finished;

    public S3ParquetSplitProcessor(S3ParquetSplit split)
    {
        this.split = split;
    }

    @Override
    public TableFunctionProcessorState process()
    {
        if (finished) {
            return TableFunctionProcessorState.Finished.FINISHED;
        }

        try {
            if (reader == null) {
                // First call: Download the parquet file from MinIO to a temp file
                tempFile = downloadFromMinio();

                // Open the parquet reader
                org.apache.parquet.io.InputFile inputFile = new LocalInputFile(tempFile);
                org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
                reader = AvroParquetReader.<GenericRecord>builder(inputFile)
                        .withConf(conf)
                        .build();
            }

            // Stream next batch
            Page page = readNextBatch();
            if (page == null) {
                cleanup();
                finished = true;
                return TableFunctionProcessorState.Finished.FINISHED;
            }

            return TableFunctionProcessorState.Processed.produced(page);
        }
        catch (Exception e) {
            cleanup();
            finished = true;
            throw new UncheckedIOException("Failed to read parquet file from MinIO: " + e.getMessage(), new IOException(e));
        }
    }

    private File downloadFromMinio()
            throws IOException
    {
        S3Client s3Client = S3Client.builder()
                .endpointOverride(URI.create(split.getEndpoint()))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(split.getAccessKey(), split.getSecretKey())))
                .forcePathStyle(true)
                .build();

        File temp = File.createTempFile("trino-s3parquet-", ".parquet");
        temp.delete(); // AWS SDK requires the file to NOT exist

        try {
            GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(split.getBucket())
                    .key(split.getKey())
                    .build();

            s3Client.getObject(request, temp.toPath());
        }
        catch (Exception e) {
            Files.deleteIfExists(temp.toPath());
            throw new IOException("Failed to download from MinIO: " + e.getMessage(), e);
        }
        finally {
            s3Client.close();
        }

        return temp;
    }

    private Page readNextBatch()
            throws IOException
    {
        int batchSize = 4096;
        BlockBuilder idBlockBuilder = BIGINT.createBlockBuilder(null, batchSize);
        BlockBuilder sessionIdBlockBuilder = BIGINT.createBlockBuilder(null, batchSize);

        GenericRecord record;
        int count = 0;
        while (count < batchSize && (record = reader.read()) != null) {
            Object idValue = record.get("id");
            Object sessionIdValue = record.get("session_id");

            Long id = toLong(idValue);
            if (id == null) {
                idBlockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(idBlockBuilder, id);
            }

            Long sessionId = toLong(sessionIdValue);
            if (sessionId == null) {
                sessionIdBlockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(sessionIdBlockBuilder, sessionId);
            }
            count++;
        }

        if (count == 0) {
            return null;
        }

        return new Page(count, idBlockBuilder.build(), sessionIdBlockBuilder.build());
    }

    private void cleanup()
    {
        if (reader != null) {
            try {
                reader.close();
            }
            catch (IOException e) {
                // ignore
            }
            reader = null;
        }
        if (tempFile != null) {
            try {
                Files.deleteIfExists(tempFile.toPath());
            }
            catch (IOException e) {
                // ignore
            }
            tempFile = null;
        }
    }

    private static Long toLong(Object value)
    {
        if (value == null) {
            return null;
        }
        if (value instanceof Long) {
            return (Long) value;
        }
        if (value instanceof Integer) {
            return ((Integer) value).longValue();
        }
        if (value instanceof Number) {
            return value != null ? ((Number) value).longValue() : null;
        }
        return Long.parseLong(value.toString());
    }

    private static class LocalInputFile implements org.apache.parquet.io.InputFile
    {
        private final File file;

        public LocalInputFile(File file)
        {
            this.file = file;
        }

        @Override
        public long getLength()
        {
            return file.length();
        }

        @Override
        public org.apache.parquet.io.SeekableInputStream newStream() throws IOException
        {
            return new LocalSeekableInputStream(file);
        }
    }

    private static class LocalSeekableInputStream extends org.apache.parquet.io.SeekableInputStream
    {
        private final java.io.RandomAccessFile raf;

        public LocalSeekableInputStream(File file) throws IOException
        {
            this.raf = new java.io.RandomAccessFile(file, "r");
        }

        @Override
        public long getPos() throws IOException
        {
            return raf.getFilePointer();
        }

        @Override
        public void seek(long newPos) throws IOException
        {
            raf.seek(newPos);
        }

        @Override
        public void readFully(byte[] bytes) throws IOException
        {
            raf.readFully(bytes);
        }

        @Override
        public void readFully(byte[] bytes, int start, int len) throws IOException
        {
            raf.readFully(bytes, start, len);
        }

        @Override
        public int read(java.nio.ByteBuffer buf) throws IOException
        {
            byte[] b = new byte[buf.remaining()];
            int read = raf.read(b);
            if (read > 0) {
                buf.put(b, 0, read);
            }
            return read;
        }

        @Override
        public void readFully(java.nio.ByteBuffer buf) throws IOException
        {
            byte[] b = new byte[buf.remaining()];
            raf.readFully(b);
            buf.put(b);
        }

        @Override
        public int read() throws IOException
        {
            return raf.read();
        }

        @Override
        public void close() throws IOException
        {
            raf.close();
        }
    }
}
