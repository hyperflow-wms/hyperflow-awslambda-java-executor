This handler is created for hyperflow java tasks executed on aws lambda.
To use it, build jar with "mvn clean package" command and upload it into lambda function.
Function runtime and handler should be set to "Java 8" and "Handler::handleRequest" respectively.