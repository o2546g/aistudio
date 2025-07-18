# Question:

I want to do this in Java: read all rows in a database table (there are a lot of them), for each row, call a REST endpoint to enrich the row data, then write the enriched data in another table. Is that a good use case for Spring Batch? If yes, how to do it?

## Answer:

Of course. This is an **excellent and classic use case for Spring Batch**.

Your scenario fits the standard Extract-Transform-Load (ETL) pattern that Spring Batch is designed for:

*   **Extract:** Read rows from the source database table.
*   **Transform:** Call a REST endpoint to enrich the data.
*   **Load:** Write the transformed, split data into two destination tables.

Spring Batch provides key benefits for this task:
*   **Chunk Processing:** It reads and processes data in manageable chunks, which is crucial for handling "a lot of rows" without running out of memory.
*   **Restartability:** If the job fails (e.g., the REST service is down, or the database connection is lost), Spring Batch can resume from where it left off, preventing reprocessing of already completed items.
*   **Transaction Management:** Each chunk is processed within a single transaction. If writing to the second table fails, the write to the first table for that entire chunk is also rolled back, ensuring data consistency.
*   **Scalability:** You can scale the job using multi-threaded steps or partitioning for even better performance.

---

### How to Implement this with Spring Batch 5.2 and JPA

Here is a step-by-step guide on how to build this job.

#### 1. Project Dependencies (pom.xml)

You'll need the following starters in your Spring Boot project:

```xml
<dependencies>
    <!-- Core Spring Batch -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-batch</artifactId>
    </dependency>

    <!-- For JPA Reader/Writer -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-data-jpa</artifactId>
    </dependency>

    <!-- For calling the REST endpoint -->
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>

    <!-- Your Database Driver -->
    <dependency>
        <groupId>com.h2database</groupId>
        <artifactId>h2</artifactId>
        <scope>runtime</scope>
    </dependency>
    <!-- Or for PostgreSQL, MySQL, etc. -->
    <!--
    <dependency>
        <groupId>org.postgresql</groupId>
        <artifactId>postgresql</artifactId>
        <scope>runtime</scope>
    </dependency>
    -->
</dependencies>
```

#### 2. Domain Models (Entities and DTOs)

First, define the JPA entities for your tables and a DTO to hold the enriched data.

```java
// --- Source Data ---
@Entity
@Table(name = "source_data")
public class SourceEntity {
    @Id
    private Long id;
    private String someData;
    // getters and setters
}

// --- Enriched Data (Transient, not an entity) ---
// This object holds the result from the processor
public class EnrichedData {
    // Data for the first table
    private Long id;
    private String enrichedInfoPartOne;
    private String originalData;

    // Data for the second table
    private String enrichedInfoPartTwo;
    private LocalDateTime timestamp;
    // getters and setters
}

// --- Destination Data ---
@Entity
@Table(name = "destination_one")
public class DestinationEntityOne {
    @Id
    private Long id;
    private String enrichedInfoPartOne;
    private String originalData;
    // getters and setters
}

@Entity
@Table(name = "destination_two")
public class DestinationEntityTwo {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY) // Or link by source ID
    private Long id;
    private String enrichedInfoPartTwo;
    private LocalDateTime timestamp;
    // getters and setters
}
```

#### 3. The Batch Job Configuration

Now, let's create the Spring Batch configuration. This is where you define the `Job`, `Step`, `Reader`, `Processor`, and `Writer`.

**With Spring Boot 3 / Spring Batch 5, you don't need `@EnableBatchProcessing` anymore; it's auto-configured.**

```java
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.web.client.RestTemplate;
import jakarta.persistence.EntityManagerFactory;

import java.util.Arrays;
import java.util.stream.Collectors;

@Configuration
public class DataEnrichmentJobConfig {

    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final EntityManagerFactory entityManagerFactory;

    public DataEnrichmentJobConfig(JobRepository jobRepository,
                                   PlatformTransactionManager transactionManager,
                                   EntityManagerFactory entityManagerFactory) {
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
        this.entityManagerFactory = entityManagerFactory;
    }

    // 1. The ItemReader
    @Bean
    public JpaPagingItemReader<SourceEntity> sourceItemReader() {
        return new JpaPagingItemReaderBuilder<SourceEntity>()
                .name("sourceItemReader")
                .entityManagerFactory(entityManagerFactory)
                .queryString("select s from SourceEntity s order by s.id")
                .pageSize(100) // Adjust chunk size based on your memory and performance needs
                .build();
    }

    // 2. The ItemProcessor
    @Bean
    public ItemProcessor<SourceEntity, EnrichedData> enrichmentProcessor() {
        return new EnrichmentProcessor(new RestTemplate());
    }

    // 3. The ItemWriters (one for each destination)
    // We will combine them using a CompositeItemWriter

    @Bean
    public ItemWriter<EnrichedData> destinationOneWriter() {
        // This is a custom writer. It takes EnrichedData, maps it to DestinationEntityOne, and saves it.
        return chunk -> {
            // Your repository for DestinationEntityOne
            // @Autowired private DestinationOneRepository repo;
            System.out.println("Writing to destination ONE for chunk of size: " + chunk.getItems().size());
            // In a real scenario, you'd inject the repository instead of the EntityManager
            // but for this example, we'll just show the mapping.
            // repo.saveAll(mappedItems);
        };
    }

    @Bean
    public ItemWriter<EnrichedData> destinationTwoWriter() {
        // This custom writer handles the second destination
        return chunk -> {
             // Your repository for DestinationEntityTwo
            // @Autowired private DestinationTwoRepository repo;
            System.out.println("Writing to destination TWO for chunk of size: " + chunk.getItems().size());
        };
    }
    
    // For a real implementation, it's better to create dedicated writer classes
    // instead of lambdas to inject repositories. See the section below for this.

    @Bean
    @Qualifier("compositeEnrichmentWriter")
    public CompositeItemWriter<EnrichedData> compositeItemWriter() {
        CompositeItemWriter<EnrichedData> writer = new CompositeItemWriter<>();
        writer.setDelegates(Arrays.asList(destinationOneWriter(), destinationTwoWriter()));
        return writer;
    }

    // 4. The Step
    @Bean
    public Step enrichmentStep(@Qualifier("compositeEnrichmentWriter") CompositeItemWriter<EnrichedData> writer) {
        return new StepBuilder("enrichmentStep", jobRepository)
                .<SourceEntity, EnrichedData>chunk(100, transactionManager) // Chunk size matches reader's page size
                .reader(sourceItemReader())
                .processor(enrichmentProcessor())
                .writer(writer)
                .build();
    }

    // 5. The Job
    @Bean
    public Job dataEnrichmentJob(Step enrichmentStep) {
        return new JobBuilder("dataEnrichmentJob", jobRepository)
                .start(enrichmentStep)
                .build();
    }
}
```

#### 4. Implementing the Processor and Writers

##### The ItemProcessor

This class contains the logic to call the REST endpoint.

```java
import org.springframework.batch.item.ItemProcessor;
import org.springframework.web.client.RestTemplate;
// Assume a DTO for the REST API response
// import com.example.RestApiResponseDto; 

public class EnrichmentProcessor implements ItemProcessor<SourceEntity, EnrichedData> {

    private final RestTemplate restTemplate;
    private final String apiUrl = "https://api.example.com/enrich/{id}";

    public EnrichmentProcessor(RestTemplate restTemplate) {
        this.restTemplate = restTemplate;
    }

    @Override
    public EnrichedData process(SourceEntity item) throws Exception {
        System.out.println("Processing item with ID: " + item.getId());

        // Call the REST endpoint
        // In a real app, handle exceptions (e.g., RestClientException)
        RestApiResponseDto response = restTemplate.getForObject(apiUrl, RestApiResponseDto.class, item.getId());
        
        if (response == null) {
            // You can choose to skip this item by returning null
            // The item will not be passed to the ItemWriter
            return null;
        }

        // Map the source data and REST response to our EnrichedData object
        EnrichedData enrichedData = new EnrichedData();
        
        // Data for table one
        enrichedData.setId(item.getId());
        enrichedData.setOriginalData(item.getSomeData());
        enrichedData.setEnrichedInfoPartOne(response.getInfoPartOne());

        // Data for table two
        enrichedData.setEnrichedInfoPartTwo(response.getInfoPartTwo());
        enrichedData.setTimestamp(LocalDateTime.now());
        
        return enrichedData;
    }
}
```

##### Better Implementation for ItemWriters

Using lambdas is fine for simple examples, but for a real application, create dedicated writer classes that can have repositories injected.

```java
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

// --- Writer for Destination One ---
@Component
public class DestinationOneItemWriter implements ItemWriter<EnrichedData> {

    private final DestinationOneRepository repository;

    public DestinationOneItemWriter(DestinationOneRepository repository) {
        this.repository = repository;
    }

    @Override
    public void write(Chunk<? extends EnrichedData> chunk) throws Exception {
        List<DestinationEntityOne> itemsToWrite = chunk.getItems().stream()
            .map(enriched -> {
                DestinationEntityOne entity = new DestinationEntityOne();
                entity.setId(enriched.getId());
                entity.setOriginalData(enriched.getOriginalData());
                entity.setEnrichedInfoPartOne(enriched.getEnrichedInfoPartOne());
                return entity;
            })
            .collect(Collectors.toList());
        
        repository.saveAll(itemsToWrite);
    }
}

// --- Writer for Destination Two ---
@Component
public class DestinationTwoItemWriter implements ItemWriter<EnrichedData> {

    private final DestinationTwoRepository repository;

    public DestinationTwoItemWriter(DestinationTwoRepository repository) {
        this.repository = repository;
    }

    @Override
    public void write(Chunk<? extends EnrichedData> chunk) throws Exception {
        List<DestinationEntityTwo> itemsToWrite = chunk.getItems().stream()
            .map(enriched -> {
                DestinationEntityTwo entity = new DestinationEntityTwo();
                // Assume ID is generated or map from source if needed
                entity.setEnrichedInfoPartTwo(enriched.getEnrichedInfoPartTwo());
                entity.setTimestamp(enriched.getTimestamp());
                return entity;
            })
            .collect(Collectors.toList());
        
        repository.saveAll(itemsToWrite);
    }
}
```

You would then update your `CompositeItemWriter` bean to use these components:

```java
@Bean
@Qualifier("compositeEnrichmentWriter")
public CompositeItemWriter<EnrichedData> compositeItemWriter(
    DestinationOneItemWriter writerOne,
    DestinationTwoItemWriter writerTwo) {
        
    CompositeItemWriter<EnrichedData> writer = new CompositeItemWriter<>();
    writer.setDelegates(Arrays.asList(writerOne, writerTwo));
    return writer;
}
```

---

### Best Practices and Considerations

1.  **Chunk Size:** The `pageSize()` of the reader and the `chunk()` size of the step should generally be the same. A size between 100 and 1000 is a good starting point. Tune this number based on the complexity of your processing, memory available, and network latency of the REST calls.
2.  **Error Handling:**
    *   **REST API Failures:** In the `ItemProcessor`, wrap the `restTemplate` call in a `try-catch` block. If an API call fails, you can either log the error and return `null` (to skip the item) or throw an exception to fail the entire chunk (and potentially the job).
    *   **Fault Tolerance:** Spring Batch has powerful `skip()` and `retry()` policies you can configure on the step for handling transient errors (like a temporary network blip).
3.  **Performance:**
    *   **Non-Blocking HTTP Client:** For very high throughput, consider replacing `RestTemplate` (blocking) with `WebClient` (non-blocking). This is more complex to integrate into a standard `ItemProcessor` but can significantly improve performance by not having worker threads wait for I/O.
    *   **Parallel Processing:** If the enrichment of one row is independent of others, you can configure a multi-threaded step using a `TaskExecutor` to make REST calls in parallel, drastically speeding up the job.
4.  **Transactionality:** The `CompositeItemWriter` is a key part of the solution. Both delegate writers (`DestinationOneItemWriter` and `DestinationTwoItemWriter`) operate within the **same transaction** for the chunk. If the `saveAll()` call in `DestinationTwoItemWriter` fails, the `saveAll()` from `DestinationOneItemWriter` for that same chunk will be rolled back. This guarantees consistency between your two destination tables.

# Question:

A few questions about the above code:
1. If I use a regular (not in-memory) database (i.e. the tables to store the batch job information are still there when the app restarts), do I need to change anything in this configuration `spring.batch.jdbc.initialize-schema=always`? How to specify a database schema in the configuration?
2. How to trigger that job in your code?
3. I want to trigger the job from a REST endpoint. The job should start in the background and the REST endpoint returns some id for the job so that we can check the job status later. How to do that?

## Answer:

Of course! These are excellent follow-up questions that get into the practical, real-world usage of Spring Batch. Let's tackle them one by one.

### 1. Database Configuration (`initialize-schema` and custom schema)

#### `spring.batch.jdbc.initialize-schema=always`

You should **absolutely change this for a regular, persistent database** (like PostgreSQL, MySQL, Oracle, etc.).

*   **`always`**: This setting tells Spring Boot to run the `schema-*.sql` script every time the application starts. This is fine for an in-memory database like H2 which is destroyed on shutdown. For a real database, this will cause an error on the second startup because the tables (`BATCH_JOB_INSTANCE`, etc.) will already exist, and the `CREATE TABLE` statements will fail.

*   **The Correct Approach for Production:**
    1.  Set the property to **`never`**. This is the safest option. It tells Spring Batch not to touch the schema at all, assuming it's already there.
        ```properties
        spring.batch.jdbc.initialize-schema=never
        ```
    2.  **How do you create the tables then?** You manage the database schema yourself, which is standard practice.
        *   **Option A (Manual):** Find the DDL script for your specific database inside the `spring-batch-core.jar` (under `org/springframework/batch/core/schema-*.sql`). Run this script once on your database to create the necessary tables.
        *   **Option B (Recommended):** Use a database migration tool like **Flyway** or **Liquibase**. This is the best practice. You would add the Spring Batch DDL script as your first migration, and the tool would ensure it's only ever run once.

#### How to specify a database schema?

If your batch tables need to live in a specific schema (e.g., `batch_jobs.BATCH_JOB_INSTANCE` instead of `public.BATCH_JOB_INSTANCE`), you use the `table-prefix` property.

```properties
# Example for PostgreSQL where tables should be in the 'batch_schema'
spring.batch.jdbc.table-prefix=batch_schema.
```

Spring Batch will then prepend `batch_schema.` to all of its table names in its SQL queries. For example, it will query `batch_schema.BATCH_JOB_INSTANCE` instead of just `BATCH_JOB_INSTANCE`.

**Example `application.properties` for production PostgreSQL:**

```properties
# --- PostgreSQL Database Configuration ---
spring.datasource.url=jdbc:postgresql://localhost:5432/mydatabase
spring.datasource.username=user
spring.datasource.password=secret

# --- Production Spring Batch Configuration ---
# Let a migration tool like Flyway/Liquibase handle schema creation
spring.batch.jdbc.initialize-schema=never

# (Optional) If you need to place batch tables in a specific schema
spring.batch.jdbc.table-prefix=batch_schema.
```

---

### 2. How to Trigger the Job Programmatically

To run a job, you use the `JobLauncher` interface. Spring Boot automatically configures a bean of this type for you, so you can just inject it.

The key method is `jobLauncher.run(Job job, JobParameters jobParameters)`.

*   **`Job`**: The job bean you want to run (e.g., your `enrichDataJob` bean).
*   **`JobParameters`**: This is a crucial concept. It's a set of parameters that uniquely identifies a **job instance**. A job instance (a combination of a Job name and its parameters) that has completed successfully **cannot be run again**. This is what enables restartability. To run the "same" job again, you must provide different parameters. A common technique is to add a timestamp.

Here is a simple example using `CommandLineRunner` to trigger the job on application startup.

```java
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class JobTrigger implements CommandLineRunner {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    private Job enrichDataJob; // Spring injects the Job bean by its name

    @Override
    public void run(String... args) throws Exception {
        System.out.println("TRIGGERING THE JOB");
        
        // To re-run the job, we need to provide unique parameters
        JobParameters jobParameters = new JobParametersBuilder()
                .addLong("time", System.currentTimeMillis())
                .toJobParameters();

        jobLauncher.run(enrichDataJob, jobParameters);
    }
}
```
*Note: The `RunIdIncrementer` in your job definition also helps in making parameters unique, but it's good practice to understand how `JobParameters` work.*

---

### 3. Triggering the Job from a REST Endpoint (Asynchronously)

This is a very common requirement. The trick is to make the `JobLauncher` **asynchronous**, so your REST endpoint can return immediately without waiting for the entire (potentially hours-long) batch job to finish.

#### Step 1: Configure an Asynchronous JobLauncher

By default, `jobLauncher.run()` is **synchronous** (blocking). You need to configure it with a `TaskExecutor`.

Add this bean to your `BatchConfiguration` or another `@Configuration` class.

```java
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;

@Configuration
public class AsyncJobLauncherConfig {

    @Autowired
    private JobRepository jobRepository;

    @Bean(name = "asyncJobLauncher")
    public JobLauncher asyncJobLauncher() {
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();
        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.setTaskExecutor(new SimpleAsyncTaskExecutor()); // This makes it async!
        return jobLauncher;
    }
}
```

#### Step 2: Create the REST Controller

The controller will inject the `asyncJobLauncher` and the `Job`. It will also inject `JobExplorer`, which is a read-only component for fetching metadata about past job executions.

```java
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/jobs")
public class JobController {

    @Autowired
    @Qualifier("asyncJobLauncher") // Make sure to inject our async launcher
    private JobLauncher jobLauncher;

    @Autowired
    private Job enrichDataJob;

    @Autowired
    private JobExplorer jobExplorer; // To check job status

    // Endpoint to launch the job
    @PostMapping("/launch")
    public ResponseEntity<Map<String, Object>> launchJob() {
        try {
            // Each run needs unique parameters. A timestamp is a good way to do this.
            JobParameters jobParameters = new JobParametersBuilder()
                    .addLong("startTime", System.currentTimeMillis())
                    .toJobParameters();
            
            // The asyncJobLauncher returns immediately with a JobExecution
            JobExecution jobExecution = jobLauncher.run(enrichDataJob, jobParameters);

            Map<String, Object> response = new HashMap<>();
            response.put("message", "Job launched successfully.");
            response.put("executionId", jobExecution.getId()); // The ID to track the job

            return ResponseEntity.ok(response);
        } catch (Exception e) {
            // Handle exceptions (e.g., JobInstanceAlreadyCompleteException)
            Map<String, Object> response = new HashMap<>();
            response.put("message", "Error launching job: " + e.getMessage());
            return ResponseEntity.status(500).body(response);
        }
    }

    // Endpoint to check the job's status
    @GetMapping("/status/{executionId}")
    public ResponseEntity<Map<String, Object>> getJobStatus(@PathVariable Long executionId) {
        JobExecution jobExecution = jobExplorer.getJobExecution(executionId);
        
        Map<String, Object> response = new HashMap<>();
        if (jobExecution != null) {
            response.put("executionId", jobExecution.getId());
            response.put("status", jobExecution.getStatus());
            response.put("exitStatus", jobExecution.getExitStatus().getExitCode());
            response.put("startTime", jobExecution.getStartTime());
            response.put("endTime", jobExecution.getEndTime());
        } else {
            response.put("message", "Job execution not found with ID: " + executionId);
            return ResponseEntity.status(404).body(response);
        }
        
        return ResponseEntity.ok(response);
    }
}
```

Now you have a complete solution:
1.  **`POST /api/jobs/launch`**: Starts the job in the background and immediately returns a JSON response with the `executionId`.
2.  **`GET /api/jobs/status/{executionId}`**: Use the ID from the previous call to check the status of the running or completed job.

# Question:

Could you give me some idea of how to create a spring boot app which uses spring batch to do these things:
- able to start a spring batch job from a REST controller or a scheduled method
- that spring batch job must be able to runs multiple times with different parameters.

The spring batch job is given the following information:
- a very big csv file where each line is independent from others; the 1st field of each line will tell the meanings of the next fields in that line and from there we can validate the whole line.
- a very big xml file with a lot of <transaction> elements inside.

This is what the spring batch job should do:
- read each line in the csv file, validate it. If the line is invalid, write that line to another file. If the line is valid, write a record in some database table. If reading the csv file or writing to the db fail, the spring batch job should wait for a bit then try again for a few times before giving up. Keep doing this step until the whole csv file is processed.
- read each <transaction> element in the xml file then write to some other database table. After the whole xml file is processed, read from the database and write to a file.

## Answer:

Okay, this is a great use case for Spring Batch! Let's break down how you can structure this.

**Core Spring Batch Concepts You'll Use:**

1.  **Job:** The overall batch process. It's composed of one or more `Step`s.
2.  **Step:** A domain object that encapsulates an independent, sequential phase of a batch job. Every `Job` has at least one `Step`.
    *   **Chunk-Oriented Processing:** For your "very big files," this is key. A step reads data in chunks, processes each item in the chunk, and then writes the chunk. This is memory-efficient.
3.  **ItemReader:** Reads items one at a time from a data source (CSV, XML, database).
4.  **ItemProcessor:** Processes an item after it's read and before it's written. This is where your validation logic will go. It can also transform the item.
5.  **ItemWriter:** Writes items in chunks to a data source (file, database).
6.  **JobLauncher:** Used to run a `Job` with specific `JobParameters`.
7.  **JobParameters:** Used to pass parameters to a job (e.g., file paths, run dates). Crucial for running the same job multiple times with different inputs.
8.  **JobRepository:** Stores metadata about job executions (status, start/end times, etc.). Requires a `DataSource`.
9.  **@EnableBatchProcessing:** Annotation to enable Spring Batch features.
10. **@StepScope:** Allows beans (Readers, Processors, Writers) to be created for each step execution and to access `JobParameters`.

**Project Structure (Maven Dependencies):**

```xml
<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-batch</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-data-jpa</artifactId> <!-- Or spring-boot-starter-jdbc -->
    </dependency>
    <dependency> <!-- For StaxEventItemReader (XML) -->
        <groupId>org.springframework</groupId>
        <artifactId>spring-oxm</artifactId>
    </dependency>
    <dependency> <!-- Example JAXB implementation for XML marshalling/unmarshalling -->
        <groupId>com.sun.xml.bind</groupId>
        <artifactId>jaxb-impl</artifactId>
        <version>2.3.3</version> <!-- or a more recent version -->
    </dependency>
    <dependency>
        <groupId>javax.xml.bind</groupId>
        <artifactId>jaxb-api</artifactId>
        <version>2.3.1</version>
    </dependency>


    <!-- Your chosen database driver -->
    <dependency>
        <groupId>com.h2database</groupId>
        <artifactId>h2</artifactId>
        <scope>runtime</scope> <!-- Example: H2 in-memory DB -->
    </dependency>
    <!-- Lombok (Optional, for boilerplate reduction) -->
    <dependency>
        <groupId>org.projectlombok</groupId>
        <artifactId>lombok</artifactId>
        <optional>true</optional>
    </dependency>
</dependencies>
```

**Application Properties (`application.properties`):**

```properties
# Spring Batch
spring.batch.job.enabled=false # We'll launch jobs manually or via scheduler, not on startup
spring.batch.initialize-schema=always # Creates batch meta-tables on startup (use 'embedded' for testing, 'never' for prod if managed externally)

# Datasource (Example for H2)
spring.datasource.url=jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1
spring.datasource.driverClassName=org.h2.Driver
spring.datasource.username=sa
spring.datasource.password=
spring.jpa.hibernate.ddl-auto=update # For JPA entities, or manage schema manually
```

---

**1. Define Your Data Objects (POJOs):**

```java
// For CSV data
// Assuming a generic structure, you'll need to adapt this
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CsvLineRecord {
    private String field1; // The determinant field
    private String field2;
    private String field3;
    // ... more fields as needed based on the type
    private String originalLine; // Store the original line for error reporting
}

// For XML <transaction> data
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlElement;
import lombok.Data;

@Data
@XmlRootElement(name = "transaction")
public class Transaction {
    @XmlElement // Assuming direct mapping; adjust JAXB annotations as needed
    private String id;
    @XmlElement
    private double amount;
    @XmlElement
    private String currency;
    // ... other transaction fields
}

// For the database record written from valid CSV lines
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.GeneratedValue;
import lombok.Data;

@Data
@Entity // If using JPA
public class ValidCsvData {
    @Id
    @GeneratedValue
    private Long dbId;
    private String identifier;
    private String dataPayload; // Example fields
    // ... other fields based on validated CSV data
}

// For the database record written from XML transactions
import javax.persistence.Entity;
import javax.persistence.Id;
import lombok.Data;

@Data
@Entity // If using JPA
public class TransactionData {
    @Id
    private String transactionId; // Assuming XML id is unique
    private double amount;
    private String currency;
    // ...
}
```

---

**2. Batch Configuration (`BatchConfig.java`):**

```java
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.batch.item.xml.builder.StaxEventItemReaderBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;
import org.springframework.retry.backoff.FixedBackOffPolicy;

import javax.sql.DataSource;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private DataSource dataSource; // Autowired for JobRepository and DB writers/readers

    // --- CSV Processing Step Components ---

    @Bean
    @StepScope // Needed to inject jobParameters
    public FlatFileItemReader<String> csvFileReader(@Value("#{jobParameters['csvInputFile']}") String csvInputFile) {
        return new FlatFileItemReaderBuilder<String>()
                .name("csvFileReader")
                .resource(new FileSystemResource(csvInputFile))
                .lineMapper(new PassThroughLineMapper()) // Reads each line as a String
                .build();
    }

    @Bean
    @StepScope
    public ItemProcessor<String, ValidCsvData> csvLineProcessor() {
        return line -> {
            // Your validation logic:
            // 1. Split the line
            String[] fields = line.split(","); // Assuming comma-separated
            if (fields.length == 0) {
                throw new InvalidCsvLineException("Empty line: " + line);
            }

            String type = fields[0];
            // 2. Validate based on 'type'
            // Example:
            if ("USER_DATA".equals(type)) {
                if (fields.length < 3) throw new InvalidCsvLineException("USER_DATA requires at least 3 fields: " + line);
                // ... more specific validation
                ValidCsvData validData = new ValidCsvData();
                validData.setIdentifier(fields[1]);
                validData.setDataPayload(fields[2]);
                return validData;
            } else if ("PRODUCT_DATA".equals(type)) {
                if (fields.length < 2) throw new InvalidCsvLineException("PRODUCT_DATA requires at least 2 fields: " + line);
                // ...
                ValidCsvData validData = new ValidCsvData();
                validData.setIdentifier(fields[0]); // Or however you map it
                validData.setDataPayload(fields[1]);
                return validData;
            } else {
                throw new InvalidCsvLineException("Unknown line type: " + line);
            }
            // If valid, return a ValidCsvData object.
            // If invalid, throw a custom exception (e.g., InvalidCsvLineException).
            // Spring Batch will catch this and (if configured) skip the item.
        };
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<ValidCsvData> csvDbWriter() {
        return new JdbcBatchItemWriterBuilder<ValidCsvData>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO valid_csv_data (identifier, data_payload) VALUES (:identifier, :dataPayload)") // Adjust table/columns
                .dataSource(dataSource)
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<String> csvErrorWriter(@Value("#{jobParameters['csvErrorFile']}") String csvErrorFile) {
        return new FlatFileItemWriterBuilder<String>()
                .name("csvErrorWriter")
                .resource(new FileSystemResource(csvErrorFile))
                .lineAggregator(item -> item) // Writes the original string line
                .build();
    }

    // Custom Skip Listener to write skipped CSV lines to the error file
    @Bean
    public CsvSkipListener csvSkipListener(FlatFileItemWriter<String> csvErrorWriter) {
        return new CsvSkipListener(csvErrorWriter);
    }

    // --- XML Processing Step Components ---

    @Bean
    @StepScope
    public StaxEventItemReader<Transaction> xmlTransactionReader(@Value("#{jobParameters['xmlInputFile']}") String xmlInputFile) {
        Jaxb2Marshaller marshaller = new Jaxb2Marshaller();
        marshaller.setClassesToBeBound(Transaction.class);

        return new StaxEventItemReaderBuilder<Transaction>()
                .name("xmlTransactionReader")
                .resource(new FileSystemResource(xmlInputFile))
                .addFragmentRootElements("transaction") // Name of the root element for each item
                .unmarshaller(marshaller)
                .build();
    }

    @Bean
    @StepScope
    public JdbcBatchItemWriter<Transaction> xmlTransactionDbWriter() {
        return new JdbcBatchItemWriterBuilder<Transaction>()
                .itemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>())
                .sql("INSERT INTO transaction_data (transaction_id, amount, currency) VALUES (:id, :amount, :currency)") // Adjust
                .dataSource(dataSource)
                .build();
    }

    // --- DB to File (Post-XML) Step Components ---
    @Bean
    @StepScope
    public JdbcCursorItemReader<TransactionData> transactionDbReader() {
        // Reads data that was just written by xmlTransactionDbWriter
        return new JdbcCursorItemReaderBuilder<TransactionData>()
                .name("transactionDbReader")
                .dataSource(dataSource)
                .sql("SELECT transaction_id, amount, currency FROM transaction_data") // Adjust
                .rowMapper(new BeanPropertyRowMapper<>(TransactionData.class))
                .build();
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<TransactionData> transactionDataFileWriter(@Value("#{jobParameters['outputFileFromDb']}") String outputFileFromDb) {
        return new FlatFileItemWriterBuilder<TransactionData>()
                .name("transactionDataFileWriter")
                .resource(new FileSystemResource(outputFileFromDb))
                .delimited()
                .names("transactionId", "amount", "currency") // Field names for header and extraction
                .headerCallback(writer -> writer.write("TRANSACTION_ID,AMOUNT,CURRENCY")) // Optional header
                .build();
    }


    // --- Define Steps ---

    @Bean
    public Step processCsvStep(FlatFileItemReader<String> csvFileReader,
                               ItemProcessor<String, ValidCsvData> csvLineProcessor,
                               JdbcBatchItemWriter<ValidCsvData> csvDbWriter,
                               CsvSkipListener csvSkipListener) {
        FixedBackOffPolicy backOffPolicy = new FixedBackOffPolicy();
        backOffPolicy.setBackOffPeriod(1000L); // 1 second

        return stepBuilderFactory.get("processCsvStep")
                .<String, ValidCsvData>chunk(100) // Process 100 lines at a time
                .reader(csvFileReader)
                .processor(csvLineProcessor)
                .writer(csvDbWriter)
                .faultTolerant()
                .retryLimit(3) // Retry 3 times for retryable exceptions
                .retry(DeadlockLoserDataAccessException.class) // Example: retry on deadlock
                .retry(Exception.class) // Be careful, this retries on ANY exception in read/process/write
                .backOffPolicy(backOffPolicy) // Wait before retrying
                .skipLimit(Integer.MAX_VALUE) // Skip as many invalid lines as needed
                .skip(InvalidCsvLineException.class) // Skip items that cause this exception
                .listener(csvSkipListener) // Write skipped items to error file
                .build();
    }

    @Bean
    public Step processXmlStep(StaxEventItemReader<Transaction> xmlTransactionReader,
                               JdbcBatchItemWriter<Transaction> xmlTransactionDbWriter) {
        return stepBuilderFactory.get("processXmlStep")
                .<Transaction, Transaction>chunk(50) // Process 50 transactions at a time
                .reader(xmlTransactionReader)
                .writer(xmlTransactionDbWriter)
                // Add fault tolerance (retry/skip) if needed for XML processing too
                .build();
    }

    @Bean
    public Step dbToOutputFileStep(JdbcCursorItemReader<TransactionData> transactionDbReader,
                                   FlatFileItemWriter<TransactionData> transactionDataFileWriter) {
        return stepBuilderFactory.get("dbToOutputFileStep")
                .<TransactionData, TransactionData>chunk(200) // Read 200 records from DB at a time
                .reader(transactionDbReader)
                .writer(transactionDataFileWriter)
                .build();
    }

    // --- Define Job ---

    @Bean
    public Job fileProcessingJob(Step processCsvStep, Step processXmlStep, Step dbToOutputFileStep) {
        return jobBuilderFactory.get("fileProcessingJob")
                .incrementer(new RunIdIncrementer()) // Ensures JobParameters are unique for each run (good for reruns)
                .start(processCsvStep)
                .next(processXmlStep)
                .next(dbToOutputFileStep)
                .build();
    }
}

// Custom exception for CSV validation
class InvalidCsvLineException extends RuntimeException {
    public InvalidCsvLineException(String message) {
        super(message);
    }
}

// Custom Skip Listener for CSV
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class CsvSkipListener implements SkipListener<String, ValidCsvData> {
    private static final Logger log = LoggerFactory.getLogger(CsvSkipListener.class);
    private final FlatFileItemWriter<String> errorWriter;

    public CsvSkipListener(FlatFileItemWriter<String> errorWriter) {
        this.errorWriter = errorWriter;
    }

    @Override
    public void onSkipInRead(Throwable t) {
        log.warn("Skipped in read due to: {}", t.getMessage());
    }

    @Override
    public void onSkipInWrite(ValidCsvData item, Throwable t) {
        // This won't be called if the skip is due to InvalidCsvLineException in processor
        // because the item type changes.
        log.warn("Skipped in write for item {} due to: {}", item, t.getMessage());
    }

    @Override
    public void onSkipInProcess(String item, Throwable t) {
        log.warn("Skipping line during processing: '{}' due to: {}", item, t.getMessage());
        try {
            // Ensure error writer is open if it's step-scoped and might not be.
            // For simplicity, assuming it's available. If it's @StepScope, it needs to be opened.
            // One way is to pass the error file path and create a new writer or open/close it here.
            // Or, ensure the errorWriter bean is not step-scoped if used this way or managed carefully.
            // For now, let's assume it's correctly managed.
            errorWriter.write(Collections.singletonList(item)); // item here is the original String line
        } catch (Exception e) {
            log.error("Failed to write skipped item to error file: {}", item, e);
        }
    }
}
```

---

**3. Launching the Job:**

**a. From a REST Controller:**

```java
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;

@RestController
public class JobLaunchController {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    @Qualifier("fileProcessingJob") // Qualify if you have multiple jobs
    private Job job;

    @PostMapping("/launch-job")
    public String launchJob(@RequestParam("csvFile") String csvFilePath,
                            @RequestParam("xmlFile") String xmlFilePath,
                            @RequestParam("csvErrorFile") String csvErrorFilePath,
                            @RequestParam("outputFileFromDb") String outputFileFromDbPath) {
        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("csvInputFile", csvFilePath)
                    .addString("xmlInputFile", xmlFilePath)
                    .addString("csvErrorFile", csvErrorFilePath)
                    .addString("outputFileFromDb", outputFileFromDbPath)
                    .addDate("runDate", new Date()) // To ensure uniqueness for re-runs with same file paths
                    .toJobParameters();

            jobLauncher.run(job, jobParameters);
            return "Batch job: " + job.getName() + " has been invoked with parameters.";
        } catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException | JobParametersInvalidException e) {
            return "Job execution failed: " + e.getMessage();
        }
    }
}
```

**b. From a Scheduled Method:**

```java
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;

@Component
@EnableScheduling
public class ScheduledJobRunner {

    @Autowired
    private JobLauncher jobLauncher;

    @Autowired
    @Qualifier("fileProcessingJob")
    private Job job;

    // Example: Run every day at 2 AM
    // @Scheduled(cron = "0 0 2 * * ?")
    // For testing, run every 2 minutes
    @Scheduled(fixedRate = 120000)
    public void runJob() {
        // Define your parameters. These might come from config, database, or be fixed for the schedule.
        String timestamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        String csvFilePath = "/path/to/your/input/data.csv"; // Make these configurable
        String xmlFilePath = "/path/to/your/input/transactions.xml";
        String csvErrorFilePath = "/path/to/your/output/errors-" + timestamp + ".csv";
        String outputFileFromDbPath = "/path/to/your/output/db_export-" + timestamp + ".txt";


        try {
            JobParameters jobParameters = new JobParametersBuilder()
                    .addString("csvInputFile", csvFilePath)
                    .addString("xmlInputFile", xmlFilePath)
                    .addString("csvErrorFile", csvErrorFilePath)
                    .addString("outputFileFromDb", outputFileFromDbPath)
                    // The RunIdIncrementer in the Job definition handles uniqueness for scheduled runs if parameters are the same.
                    // If you want explicit uniqueness per scheduled run, add a date/timestamp.
                    .addDate("scheduledRunTime", new Date())
                    .toJobParameters();

            System.out.println("Scheduler: Launching job with parameters: " + jobParameters);
            jobLauncher.run(job, jobParameters);
            System.out.println("Scheduler: Job finished.");

        } catch (Exception e) {
            System.err.println("Scheduler: Job execution failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
```
**To make scheduled jobs truly re-runnable with identical *input file paths* but treating them as new instances, the `RunIdIncrementer` in the job definition is usually sufficient. If you also want output files to be unique per run, add a timestamp or unique ID to their names via `JobParameters` as shown.**

---

**4. Database Schema:**

You'll need to ensure the tables `VALID_CSV_DATA` and `TRANSACTION_DATA` exist in your database, along with the Spring Batch metadata tables (which `spring.batch.initialize-schema=always` can create for H2/embedded DBs).

For `VALID_CSV_DATA`:
```sql
CREATE TABLE valid_csv_data (
    db_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    identifier VARCHAR(255),
    data_payload VARCHAR(1000)
    -- Add other columns as needed
);
```

For `TRANSACTION_DATA`:
```sql
CREATE TABLE transaction_data (
    transaction_id VARCHAR(255) PRIMARY KEY,
    amount DOUBLE,
    currency VARCHAR(10)
    -- Add other columns as needed
);
```

And Spring Batch will create its own tables like `BATCH_JOB_INSTANCE`, `BATCH_JOB_EXECUTION`, etc.

---

**Key Considerations & Explanations:**

*   **`@StepScope`:** Crucial for `ItemReader`s and `ItemWriter`s that need access to `jobParameters` (like file paths). The bean is created fresh for each step execution.
*   **Parameter Uniqueness:** For a job instance (a specific run of a job) to be considered "new" by Spring Batch, its `JobParameters` must be different from previous completed instances of the same job.
    *   The `RunIdIncrementer()` helps by adding a `run.id` parameter that increments automatically.
    *   For REST calls, adding a timestamp or a unique request ID to `JobParameters` (`.addDate("runDate", new Date())` or `.addString("uniqueRunKey", UUID.randomUUID().toString())`) is good practice.
*   **Retry Logic (`.faultTolerant().retry().retryLimit().backOffPolicy()`):**
    *   Applied to the CSV processing step.
    *   `retry(Exception.class)` is broad; narrow it down to specific, transient exceptions like `DataAccessException`, `OptimisticLockingFailureException`, `DeadlockLoserDataAccessException`, or custom network/IO exceptions if possible.
    *   `backOffPolicy` makes it wait before retrying.
*   **Skip Logic (`.skip().skipLimit().listener()`):**
    *   When `csvLineProcessor` throws `InvalidCsvLineException`, the item (the raw line) is skipped.
    *   `skipLimit(Integer.MAX_VALUE)` means it will skip any number of such errors.
    *   The `CsvSkipListener` catches these skipped items (specifically in the `onSkipInProcess` method for processor skips) and writes the original problematic line to the `csvErrorWriter`.
    *   **Important for `CsvSkipListener` and `csvErrorWriter`:**
        *   The `csvErrorWriter` is also `@StepScope`.
        *   If `CsvSkipListener` needs to write, ensure `csvErrorWriter` is properly initialized/opened. One robust way for listeners is to inject the `Resource` (file path) via `@Value("#{jobParameters['csvErrorFile']}")` into the listener itself and manage a simple writer there, or ensure the `@StepScope` writer bean is correctly handled (Spring Batch usually manages this if the listener is part of the step's listener chain).
        *   The provided `CsvSkipListener` uses the `FlatFileItemWriter` bean. This writer needs to be "open" when `write` is called. Spring Batch opens/closes writers for the main chunk processing. If the listener tries to write when the main writer is closed or not yet open for the current chunk, it might fail. A safer approach for listeners might be to open/append/close the error file directly within the listener method or use a non-step-scoped writer bean configured for append. However, for many common use cases, injecting the writer bean works if the listener is correctly registered with the step.
*   **Error File Path:** The `csvErrorFile` and `outputFileFromDb` paths are passed as `JobParameters` so each job run can have its unique error/output files.
*   **Idempotency:** If a job fails mid-way and is restarted, Spring Batch (with proper configuration) can often pick up where it left off (e.g., re-process the current chunk). Ensure your database writes are idempotent or handled correctly on restart.
*   **Testing:** Test with small sample files first. Mock your database interactions if needed for unit testing components.

This comprehensive guide should give you a very solid foundation for building your Spring Boot Batch application. Remember to adapt table names, SQL queries, and POJO fields to your exact specifications.

# JPA example

```
N3q8ryccAAQ8aUgLvgUAAAAAAAAjAAAAAAAAAGYUjKPgGXMEm10AOBhImc0XdSQII3EZ9Ttbib78X8gN3DV
UbmtNN7okF2CfoNp6hVXk0kXqv3cgelgOQdTUMurIrS7tEIkU4tzWyCUDderx3+6V3Hfl/X9k8BHBGbYuZ7
NTf+DvqLeJrkex/T5CF0FREI6cslDWLP/vgwDgw9wt7uKIPBRdnXHhhLlfRJPXaQXlhYgbuz0MONQaHjbxp
vAudy5cJ3D+P8FAq2yfvVFjtp07o+HoqiGbx/rZY6yJHi3mY80zxyW25CfpGgZpW0jRe41cghSAZuG8vlhD
7Zos2i4MSuE51765G6jvu7rZhju+1Ci+Huh8D5LlkjJB7kGoJ4VnHfsDamY9RNWeBEtquLHGtJn+w2GRADy
+rEz0i5P1HhiW3hRGizkL6wyOCpssldF91dzTXzhNXcLJQvYMgCYyWlt2bjBIxLiySHjoE1JrAQSKOCtIW0
0wXjnL+7S2KzHShNNyXWBEgPdrCJOtUKmfD4tAjOasLhhvJqKbN9V55jVWjK5kDUnJOSdOabf4nXftFpVgK
1TZhgvPjvqKthMcqY/IaNILU/AHCJ5bibHuwNPbvNsQWRk0bFURqQUUKZck/i+OHnQQCed68K5Cfy5aXXMy
8Usy3zLNiItx96RXwrh5tJ6RdNij8BIvidDqDNlURco2HPuxokAy9z7bIYsXffquNZ2AwgVBvCQPOFimAO8
JKesBc/56+GtULs89gzJi8DI1ZixSvFqH5lvODf1momTTKYLcn8PNbnMrw6ai9XE1Ey4jW4GbC17o5cS+Rb
8XxocOgiGbySol8t+yy+c5tMzgdpVBchitpLcSDqkOYuosmbPLbr/uQ2TEtgXxrhWKMhKvNssPqn+3Y0imm
lEuMn7gsIhftYBITpucznaC4ycAGYtuVt+iDnvFf8PuVnCpPBFheWQiWiuI9Z6nQBnC6GVOvNa/NjI1Dwzw
qGJciamqJcKwoBv3yaCNejxyc5d7XwWDQNU2zRft+GrDimt2tXNZTxKdWdh1zbOIddL0lzA6M+cjM+fOtmE
sAXKZXlZUS/zSY6x2ONhE2hljPiOJjypd9VEfnCJbBeYe6c9HsLpC08mEnmUDrTDcOdQNfRJ5PCjsXrXwtk
IyEV6mvRXejsVpH/Weoqa05ezSdQ7SNpvbxW+LA+ZFV4pe/3tLdVNgG5t4GUoSDER9nErwJ7EHzlBPwd9BD
t2KlQyUNb7aF3+WFAfcA31Zo1yWeJ56XhtS1E1KqktBHemkqUg1QqNdhdsv4GOcxCyO4uX+D1sPfNRfwMPv
N9LVefKNh9PbLFhnsAgTTEqQssiL2piEbbMzNLr/RdbLg1bQSSg1Wq1HEAV95nwuO85KN2933YjJwtiYy0r
VKzseBUY31kHGBMRz4rfE4+LUHqQKv/Z1L76kxpyaC2J7dk0z6X4VbkcX7lGRtclU2U/2mz4N9ofWtIjjXI
WGt7m/SCF6eeM4TA5h5COENKwqumJa7+KpugffdK8+oC3D5G7WId8IGv+vfY3Bw8wuEwLnkc9N8o7+5cAie
ETPA2fmZdCyK7uC8Uzq11wP0Iy/wSe/5BMwTTnl8EnfjyDDAAAAgTMHrg/VXG/91yTT/rNwFrFVLnCzVOQG
6dOI9EZ24ugXBhD1L+lRj5SJ+A8JgApPKzGgn8+FFanKPzsCLEFY8YBZRKVOQHVqrFWG4BWAybxUNeHGG6T
O6jqgvlZKOTreV2qzr60SeN08HgJWDZaaT0bDSV4f7gM8upPLuWZouN/veVsSkQSMI9HbYjDhDL1N71oHbr
dBpIAUkUXDhsNnozK/T0tXJPez+7GMuz7fwqa50qZImQ0ynLwkv8q5/ZRC5TVhqNN6cBZtH/bkog5jukGWV
MoEUTSoBbpaE8SYt8OrvX5S6O10D3nB3BwSmtOaiHzEZSZDwD1hOxsgxLhjU1LOY3LO6fbS3mAaB43lLJaq
l6BWWU4AAAAXBoSjAQmBGwAHCwEAASMDAQEFXQAQAAAMgxYKAXoB+pYAAA==
```
