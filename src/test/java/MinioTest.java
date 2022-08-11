import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.CloseableIterable;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MinioTest {

    @Test
    public void testMinIO() throws IOException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.access.key", "admin1234");
        conf.set("fs.s3a.secret.key", "admin1234");
        conf.set("fs.s3a.endpoint", "http://10.201.0.212:32000");
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        String warehouse = "s3a://faas-ethan/iceberg/warehouse";

        S3AFileSystem s3AFileSystem= (S3AFileSystem) S3AFileSystem.get(new URI(warehouse), conf);
        s3AFileSystem.delete(new Path(warehouse),true);
    }
}
