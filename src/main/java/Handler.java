import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import com.google.gson.Gson;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

public class Handler {

    private static final String FOLDER_PATH = "/tmp/";
    private static final String BUCKET_KEY = "bucket";
    private static final String PREFIX_KEY = "prefix";

    public Response handleRequest(Event event, Context context) {

        Instant instant1 = Instant.now();
        LambdaLogger logger = context.getLogger();
        logger.log("body:       " + event.getBody());

        Gson gson = new Gson();
        Body body = gson.fromJson(event.getBody(), Body.class);

        try {
            logger.log("executable: " + body.getExecutable());
            logger.log("args:       " + body.getArgs());
            logger.log("inputs:     " + body.getInputs());
            logger.log("outputs:    " + body.getOutputs());
            logger.log("bucket:     " + body.getOptions().get(BUCKET_KEY));
            logger.log("prefix:     " + body.getOptions().get(PREFIX_KEY));

            downloadData(body, logger);
            downloadExecutable(body, logger);
            execute(body.getExecutable(), String.join(" ", body.getArgs()), logger);
            uploadData(body, logger);
        } catch (Exception e) {
            return handleException(e, body);
        }
        Response response = new Response();
        response.setStatusCode(HttpURLConnection.HTTP_OK);
        response.setMessage("Execution of " + body.getExecutable() + " successful, duration: " + Duration.between(instant1, Instant.now()).getSeconds() + " seconds");
        return response;
    }

    private void downloadData(Body request, LambdaLogger logger) throws IOException {
        for (Map<String, Object> input : request.getInputs()) {
            String fileName = input.get("name").toString();
            String key = request.getOptions().get(PREFIX_KEY) + "/" + fileName;
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

    private void uploadData(Body body, LambdaLogger logger) {
        for (Map<String, String> input : body.getOutputs()) {
            String fileName = input.get("name");
            String filePath = FOLDER_PATH + fileName;
            String key = body.getOptions().get(PREFIX_KEY) + "/" + fileName;
            logger.log("Uploading " + body.getOptions().get(BUCKET_KEY) + "/" + key);
            S3Utils.putObject(body.getOptions().get(BUCKET_KEY), key, filePath);
        }
    }

    private Response handleException(Exception e, Body body) {
        Throwable cause;
        if (e.getCause() != null) {
            cause = e.getCause();
        } else {
            cause = e;
        }
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        Response response = new Response();
        response.setStatusCode(HttpURLConnection.HTTP_INTERNAL_ERROR);
        response.setMessage("Execution of " + body.getExecutable() + " failed, cause: " + cause.getMessage());
        return response;
    }

    private void downloadExecutable(Body body, LambdaLogger logger) throws IOException {
        logger.log("Downloading executable" + body.getExecutable());
        String key = body.getOptions().get(PREFIX_KEY) + "/" + body.getExecutable();
        S3Object s3Object = S3Utils.getObject(body.getOptions().get(BUCKET_KEY), key);
        S3Utils.saveToFile(s3Object, FOLDER_PATH + body.getExecutable());
    }

}
