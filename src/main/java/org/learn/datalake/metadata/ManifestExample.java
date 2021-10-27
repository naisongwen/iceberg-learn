package org.learn.datalake.metadata;

import org.apache.commons.io.FileUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class ManifestExample extends TableTestBase {
    public ManifestExample() {
        super(1);
    }

    public static void main(String[] args) throws Exception {
        File tabDir = new File("warehouse/test_manifest");
        if (tabDir.exists())
            FileUtils.cleanDirectory(tabDir);
        tabDir.mkdirs();
        ManifestExample manifestExample = new ManifestExample();
        manifestExample.setupTable(tabDir);
        manifestExample.testWriteManifest();
    }

    public void testWriteManifest() throws IOException {
        File manifestFile = new File("manifest.avro");
        ManifestFile manifest = writeManifest(1000L,manifestFile, FILE_A, FILE_B, FILE_C);
        try (ManifestReader<DataFile> reader = ManifestFiles.read(manifest, FILE_IO)
                .filterRows(Expressions.equal("id", 0))) {
            List<String> files = Streams.stream(reader)
                    .map(file -> file.path().toString())
                    .collect(Collectors.toList());

            // note that all files are returned because the reader returns data files that may match, and the partition is
            // bucketing by data, which doesn't help filter files
            Assert.assertEquals("Should read the expected files",
                    Lists.newArrayList(FILE_A.path(), FILE_B.path(), FILE_C.path()), files);
        }
    }
}
