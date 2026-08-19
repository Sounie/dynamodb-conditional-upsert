import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.testcontainers.containers.localstack.LocalStackContainer;
import org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.URI;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

public class DynamoDbTests {
    /**
     * When set, tests talk to an already running localstack at this endpoint rather than starting
     * one via TestContainers. CI sets this, as localstack is started there by the
     * localstack/setup-localstack action.
     */
    private static final String ENDPOINT_ENV_VARIABLE = "LOCALSTACK_ENDPOINT";

    /** Only populated when we start localstack ourselves. */
    private static LocalStackContainer localstack;

    private static URI endpoint;
    private static String accessKey;
    private static String secretKey;
    private static String region;

    @BeforeAll
    static void setup() {
        String externalEndpoint = System.getenv(ENDPOINT_ENV_VARIABLE);

        if (externalEndpoint != null && !externalEndpoint.isBlank()) {
            System.out.println("Using already running localstack at " + externalEndpoint);
            endpoint = URI.create(externalEndpoint);
            // Localstack accepts any credentials, but the SDK requires that some are supplied.
            accessKey = "test";
            secretKey = "test";
            region = "us-east-1";
        } else {
            System.out.println("Starting localstack via TestContainers");
            localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:4.9.1"))
                    .withServices(LocalStackContainer.Service.DYNAMODB);
            localstack.start();

            Awaitility.waitAtMost(Duration.of(30, ChronoUnit.SECONDS))
                    .until(() -> localstack.isCreated() && localstack.isRunning() && localstack.isHealthy());

            endpoint = localstack.getEndpointOverride(LocalStackContainer.Service.DYNAMODB);
            accessKey = localstack.getAccessKey();
            secretKey = localstack.getSecretKey();
            region = localstack.getRegion();
        }

        // Setting up client to talk to localstack
        try (DynamoDbClient dbClient = createClient()) {
            createEventTable(dbClient);
        }
    }

    @AfterAll
    static void teardown() {
        if (localstack != null) {
            localstack.stop();
        }
    }

    private static DynamoDbClient createClient() {
        return DynamoDbClient.builder()
                .endpointOverride(endpoint) // Localstack endpoint
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .region(Region.of(region))
                .build();
    }

    private static void createEventTable(DynamoDbClient dbClient) {
        CreateTableRequest createTableRequest = CreateTableRequest.builder()
                .tableName("event")
                .keySchema(KeySchemaElement.builder()
                        .attributeName("id")
                        .keyType(KeyType.HASH)
                        .build())
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName("id")
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(200L)
                        .writeCapacityUnits(200L)
                        .build())
                .build();

        try {
            CreateTableResponse createTableResponse = dbClient.createTable(createTableRequest);

            SdkHttpResponse sdkHttpResponse = createTableResponse.sdkHttpResponse();

            System.out.println("Create table response: " + sdkHttpResponse.statusCode());
        } catch (ResourceInUseException e) {
            // A localstack instance that we did not start may already hold the table from an earlier run
            System.out.println("Table 'event' already exists, reusing it");
        }
    }

    @Test
    void testWriteDataThenUpsert() {
        AtomicInteger completed = new AtomicInteger(0);

        DynamoDbClient dbClient = createClient();

        String idAsString =  UUID.randomUUID().toString();
        String initialVersion = "0";
        Map<String, AttributeValue> eventDataStarting = Map.of("id", AttributeValue.builder().s(idAsString).build(),
                "version", AttributeValue.builder().n(initialVersion).build());
        System.out.println("DEBUG eventDataStarting"+eventDataStarting);
        dbClient.putItem(
                PutItemRequest.builder().tableName("event")
                        .item(eventDataStarting)
                        .conditionExpression("attribute_not_exists(id) OR (version < :version)")
                        .expressionAttributeValues(Map.of(":version", AttributeValue.builder().n(initialVersion).build()))
                        .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
                        .build());

        Awaitility.waitAtMost(Duration.of(30, ChronoUnit.SECONDS))
                .until(() -> {
                    GetItemResponse getItemResponseAfterInitialInsert = dbClient.getItem(GetItemRequest.builder().tableName("event")
                            .key(Map.of("id", AttributeValue.builder().s(idAsString).build())).
                            build());
                    System.out.println("Waiting for initial insert to be visible");
                    return getItemResponseAfterInitialInsert.hasItem() && !getItemResponseAfterInitialInsert.item().isEmpty();
                });

        // Setting up multiple concurrent updates to each have an attempt to update the version.
        // If we have a "safe" mechanism for updating then we should always end up with the highest versiop (101) in place
        List<Integer> versionValues = new ArrayList<>(100);
        for (int i = 2; i <= 101; i++) {
            versionValues.add(i); // Starting from 2
        }

        Collections.shuffle(versionValues);

        // Set up a concurrent executor to perform the upsert calls concurrently
        ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
        System.out.println("Set up executor service");

        for (Integer versionValue : versionValues) {
            String version = versionValue.toString();

            executorService.submit(() -> {
                Map<String, AttributeValue> eventDataUpdating = Map.of("id", AttributeValue.builder().s(idAsString).build(),
                        "version", AttributeValue.builder().n(version).build());
                try {
                    PutItemResponse putItemResponse2 = dbClient.putItem(
                            PutItemRequest.builder().tableName("event")
                                    .item(eventDataUpdating)
                                    .conditionExpression("attribute_not_exists(id) OR (version < :version)")
                                    .expressionAttributeValues(Map.of(":version", AttributeValue.builder().n(version).build()))
                                    .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
                                    .build());

                    completed.incrementAndGet();
                    // Can be a mix of exceptions and successful updates
                } catch (ConditionalCheckFailedException e) {
                    System.out.println("DEBUG - update failed as existing version " + e.item().get("version").n() + " already higher than " + version);
                    assertThat(e.isThrottlingException()).isFalse();
                    assertThat(e.isRetryableException()).isFalse();
                    assertThat(e.isClockSkewException()).isFalse();

                    assertThat(e.item()).isNotEmpty();
                    // Expect updatingVersion is lower value than existing version
                    int versionAsInt = Integer.parseInt(version);
                    int existingVersionAsInt = Integer.parseInt(e.item().get("version").n());
                    assertThat(existingVersionAsInt).isGreaterThan(versionAsInt);
                    completed.incrementAndGet();
                }
            });
        }

        try {
            executorService.shutdown();
            boolean terminated = executorService.awaitTermination(80L, TimeUnit.SECONDS);

            assertThat(completed.get()).isEqualTo(100);
            assertThat(terminated).withFailMessage("Executor service not terminated cleanly?").isTrue();
        }  catch (InterruptedException e) {
            System.err.println("Interrupted while waiting for executor service to terminate");
        }

        GetItemResponse getItemResponseAfterUpdate = dbClient.getItem(GetItemRequest.builder().tableName("event")
                            .key(Map.of("id", AttributeValue.builder().s(idAsString).build())).
                            build());

        dbClient.close();
        assertThat(getItemResponseAfterUpdate.item().get("version").n()).isEqualTo("101");
    }
}
