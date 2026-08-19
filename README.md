Trying out what is involved in getting DynamoDB to support a conditional upsert.

 - If a record with the specified id does not already exist, then insert
 - If a record with the specified id already exists
   - If the specified version is greater than the existing version then overwrite the record with the newer, later version.
   - If the specified version is less than or the same as the existing version then leave the existing record as it is.

Running the tests

The tests need a localstack instance providing DynamoDB. They obtain one in either of two ways:

 - If the `LOCALSTACK_ENDPOINT` environment variable is set, they use the already running localstack at that
   endpoint. This is what CI does, where localstack is started by the `localstack/setup-localstack` action.
 - Otherwise they start localstack themselves via TestContainers, which requires a working Docker setup.

To run against a localstack that you started yourself:

    docker run -d --name localstack -p 4566:4566 -e SERVICES=dynamodb localstack/localstack:4.9.1
    LOCALSTACK_ENDPOINT=http://localhost:4566 mvn test

Docker
 - To make TestContainers work with Docker I needed to apply a configuration change, as per:
   https://github.com/testcontainers/testcontainers-java/issues/11212#issuecomment-3516573631
 -
