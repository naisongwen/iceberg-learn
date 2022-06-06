package flink.source;

import com.google.common.collect.Maps;
import org.apache.flink.util.Preconditions;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedInputFile;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

public class InputFilesDecryptor {
    private final Map<String, InputFile> decryptedInputFiles;

    public InputFilesDecryptor(CombinedScanTask combinedTask, FileIO io, EncryptionManager encryption) {
        Map<String, ByteBuffer> keyMetadata = Maps.newHashMap();
        combinedTask.files().stream()
                .flatMap(fileScanTask -> Stream.concat(Stream.of(fileScanTask.file()), fileScanTask.deletes().stream()))
                .forEach(file -> keyMetadata.put(file.path().toString(), file.keyMetadata()));
        Stream<EncryptedInputFile> encrypted = keyMetadata.entrySet().stream()
                .map(entry -> EncryptedFiles.encryptedInput(io.newInputFile(entry.getKey()), entry.getValue()));

        // decrypt with the batch call to avoid multiple RPCs to a key server, if possible
        Iterable<InputFile> decryptedFiles = encryption.decrypt(encrypted::iterator);

        Map<String, InputFile> files = Maps.newHashMapWithExpectedSize(keyMetadata.size());
        decryptedFiles.forEach(decrypted -> files.putIfAbsent(decrypted.location(), decrypted));
        this.decryptedInputFiles = Collections.unmodifiableMap(files);
    }

    public InputFile getInputFile(FileScanTask task) {
        Preconditions.checkArgument(!task.isDataTask(), "Invalid task type");
        return decryptedInputFiles.get(task.file().path().toString());
    }

    public InputFile getInputFile(String location) {
        return decryptedInputFiles.get(location);
    }
}
