import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

public class Handler {

    private static final String FOLDER_PATH = "/tmp/";
    private static final String BUCKET_KEY = "bucket";

    public long handleRequest(Request request, Context context) throws Exception {

        Instant instant1 = Instant.now();

        LambdaLogger logger = context.getLogger();

        logger.log("executable: " + request.getExecutable());
        logger.log("args:       " + request.getArgs());
        logger.log("inputs:     " + request.getInputs());
        logger.log("outputs:    " + request.getOutputs());
        logger.log("bucket:     " + request.getOptions().get(BUCKET_KEY));
        logger.log("prefix:     " + request.getOptions().get("prefix"));

        downloadData(request, logger);
        execute(request.getExecutable(), String.join(" ", request.getArgs()), logger);
        uploadData(request, logger);

        Instant instant2 = Instant.now();

        return Duration.between(instant1, instant2).getSeconds();
    }

    private void downloadData(Request request, LambdaLogger logger) throws Exception {
        for (Map<String, Object> input : request.getInputs()) {
            String fileName = input.get("name").toString();
            String key = request.getOptions().get("prefix") + "/" + fileName;
            logger.log("Downloading " + request.getOptions().get(BUCKET_KEY) + "/" + key);
            S3Object s3Object = S3Utils.getObject(request.getOptions().get(BUCKET_KEY), key);
            S3Utils.saveToFile(s3Object, FOLDER_PATH + fileName);
        }
    }

    private void execute(String executable, String args, LambdaLogger logger) throws Exception {
        logger.log("Executing " + executable);

        Process chmod = Runtime.getRuntime().exec("chmod -R 755 " + FOLDER_PATH);
        chmod.waitFor();
        Process process = Runtime.getRuntime().exec(FOLDER_PATH + executable + " " + args);
        process.waitFor();

        String outputMsg = IOUtils.toString(process.getInputStream());
        String errorMsg = IOUtils.toString(process.getErrorStream());

        logger.log("Stdout: " + outputMsg);
        logger.log("Stderr: " + errorMsg);
    }

    private void uploadData(Request request, LambdaLogger logger) {
        for (Map<String, String> input : request.getOutputs()) {
            String fileName = input.get("name");
            String filePath = FOLDER_PATH + fileName;
            String key = request.getOptions().get("prefix") + "/" + fileName;
            //String key = "test" + "/" + fileName;
            logger.log("Uploading " + request.getOptions().get(BUCKET_KEY) + "/" + key);
            S3Utils.putObject(request.getOptions().get(BUCKET_KEY), key, filePath);
        }
    }

}
