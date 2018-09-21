import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public class S3Utils {

    private static final Logger logger = Logger.getGlobal();
    private static final AmazonS3 S3 = AmazonS3ClientBuilder.defaultClient();

    private S3Utils() {
    }

    public static S3Object getObject(String bucketName, String key) {
        S3Object s3Object;
        try {
            s3Object = S3.getObject(bucketName, key);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error during downloading " + bucketName + "/" + key);
            throw e;
        }
        return s3Object;
    }

    public static void putObject(String bucketName, String key, String filePath) {
        File file = new File(filePath);
        try {
            S3.putObject(bucketName, key, file);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error during uploading " + filePath + "into" + bucketName + "/" + key);
            throw e;
        }
    }

    public static void saveToFile(S3Object s3Object, String fileName) throws IOException {
        try (InputStream in = s3Object.getObjectContent()) {
            Objects.requireNonNull(s3Object);
            Files.copy(in, Paths.get(fileName), StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error during saving to file: " + fileName);
            throw e;
        }
    }
}
