# tainted-spring-notification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Kafka에서 `graph.updated`·`comment.created` 이벤트를 소비해 Redis 기반 중복제거(dedup)와 함께 알림을 생성하고, 내부 조회 API로 블랙박스 검증이 가능한 `notification-service`(Java 17 · Gradle · Spring Boot 3.3 · Redis)를 RestAssured/Testcontainers로 구현한다.

**Architecture:** Kafka consumer 전용 서비스. 아웃바운드 REST 호출 없음. 알림 대상은 이벤트 페이로드(`graph.updated.userId`, `comment.created.postAuthorUserId`)에서 직접 추출. 알림은 Redis(논리 DB 3)에 사용자별 리스트(`notif:<userId>`)로 JSON 직렬화 저장. 중복제거는 SET(`notif:dedup`)에 `dedupKey`(= `type:eventId`) 존재 여부로 판단. 시간/ID는 주입 가능한 `Clock`/`IdGenerator` 빈으로 결정론 확보. 추적/관측 라이브러리는 넣지 않음(actuator health만 유지).

**Tech Stack:** Java 17, Gradle 8.10, Spring Boot 3.3.5, Spring Kafka (consumer only), Spring Data Redis, springdoc-openapi 2.6, Redis 7, Testcontainers (Redis + Kafka), RestAssured.

---

## 사전 조건

- `tainted-spring-platform` 계획이 완료되어 있어야 통합 실행이 쉽다(단위/통합 테스트는 Testcontainers로 자체 인프라를 띄우므로 platform 없이도 테스트는 통과한다).
- **레포 위치**: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-notification/` (모든 경로는 이 루트 기준).
- **포트**: 8087. **패키지 루트**: `com.tainted.notification`. **Redis 논리 DB**: 3.
- **데이터스토어**: Redis 7(논리 DB 3). Kafka는 소비 전용(프로듀서 없음).
- **로컬 사전 설치**: Docker 데몬(Task 7 통합 테스트의 Testcontainers, Task 8 이미지 빌드, Task 9 compose에 필요), JDK 17, Gradle 8.10(`gradle wrapper` 로 감싸므로 로컬 Gradle 설치 없어도 가능), Git.

## File Structure

```
build.gradle
settings.gradle
gradlew / gradlew.bat / gradle/wrapper/gradle-wrapper.properties
Dockerfile
src/main/resources/application.yml
src/main/resources/application-docker.yml
src/main/java/com/tainted/notification/
  NotificationApplication.java
  config/DeterminismConfig.java        # Clock 빈
  config/OpenApiConfig.java
  config/RedisConfig.java              # ObjectMapper / RedisTemplate 설정
  id/IdGenerator.java                  # 인터페이스
  id/UuidIdGenerator.java              # 기본 구현
  domain/Notification.java             # 알림 레코드(record)
  event/GraphUpdatedEvent.java         # Kafka 수신 POJO
  event/CommentCreatedEvent.java       # Kafka 수신 POJO
  kafka/NotificationKafkaConsumer.java # @KafkaListener (두 토픽)
  service/NotificationService.java     # 비즈니스 로직 + Redis 연산
  web/InternalNotificationController.java  # GET /internal/notifications/{userId}
  error/GlobalExceptionHandler.java    # RFC 7807
src/test/java/com/tainted/notification/
  NotificationIntegrationTest.java     # Testcontainers (Redis + Kafka) + RestAssured
```

---

## Task 1: 레포 스캐폴드 + 빌드 가능한 빈 앱

**Files:**
- Create: `build.gradle`, `settings.gradle`, `src/main/java/com/tainted/notification/NotificationApplication.java`, `src/main/resources/application.yml`, `.gitignore`

- [ ] **Step 1: 디렉토리 생성 + git init**

Run:
```bash
mkdir -p /Users/changjoonbaek/workspace_claude-code/tainted-spring-notification
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-notification
git init
mkdir -p src/main/java/com/tainted/notification src/main/resources src/test/java/com/tainted/notification
```

- [ ] **Step 2: `.gitignore`**

`.gitignore`:
```gitignore
build/
.gradle/
*.log
.DS_Store
.idea/
```

- [ ] **Step 3: `settings.gradle`**

`settings.gradle`:
```groovy
rootProject.name = 'notification-service'
```

- [ ] **Step 4: `build.gradle`**

`build.gradle`:
```groovy
plugins {
    id 'java'
    id 'org.springframework.boot' version '3.3.5'
    id 'io.spring.dependency-management' version '1.1.6'
}

group = 'com.tainted'
version = '0.1.0'

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(17)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation 'org.springframework.boot:spring-boot-starter-web'
    implementation 'org.springframework.boot:spring-boot-starter-data-redis'
    implementation 'org.springframework.boot:spring-boot-starter-validation'
    implementation 'org.springframework.boot:spring-boot-starter-actuator'
    implementation 'org.springdoc:springdoc-openapi-starter-webmvc-ui:2.6.0'
    implementation 'org.springframework.kafka:spring-kafka'

    testImplementation 'org.springframework.boot:spring-boot-starter-test'
    testImplementation 'org.springframework.kafka:spring-kafka-test'
    testImplementation 'org.testcontainers:junit-jupiter:1.20.3'
    testImplementation 'org.testcontainers:kafka:1.20.3'
    testImplementation 'org.testcontainers:testcontainers:1.20.3'
    testImplementation 'io.rest-assured:rest-assured'
}

tasks.named('test') {
    useJUnitPlatform()
}
```

- [ ] **Step 5: Gradle Wrapper 생성**

Run:
```bash
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-notification
gradle wrapper --gradle-version 8.10
```
Expected: `gradlew`, `gradlew.bat`, `gradle/wrapper/gradle-wrapper.properties` 생성.

- [ ] **Step 6: 메인 클래스 + 최소 설정**

`src/main/java/com/tainted/notification/NotificationApplication.java`:
```java
package com.tainted.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class NotificationApplication {
    public static void main(String[] args) {
        SpringApplication.run(NotificationApplication.class, args);
    }
}
```

`src/main/resources/application.yml`:
```yaml
server:
  port: 8087
spring:
  application:
    name: notification-service
  data:
    redis:
      host: localhost
      port: 6379
      database: 3
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: notification-service
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
  mvc:
    problemdetails:
      enabled: true
management:
  endpoints:
    web:
      exposure:
        include: health
```

- [ ] **Step 7: 컴파일 확인**

Run: `./gradlew -q compileJava`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 8: 커밋**

```bash
git add -A
git commit -m "chore: scaffold notification-service (buildable empty app)"
```

---

## Task 2: 결정론 빈 (Clock, IdGenerator)

**Files:**
- Create: `id/IdGenerator.java`, `id/UuidIdGenerator.java`, `config/DeterminismConfig.java`

- [ ] **Step 1: `IdGenerator` 인터페이스**

`src/main/java/com/tainted/notification/id/IdGenerator.java`:
```java
package com.tainted.notification.id;

/** 식별자 생성 추상화. 테스트에서 결정론적 구현으로 교체 가능. */
public interface IdGenerator {
    String newId();
}
```

- [ ] **Step 2: 기본 UUID 구현**

`src/main/java/com/tainted/notification/id/UuidIdGenerator.java`:
```java
package com.tainted.notification.id;

import org.springframework.stereotype.Component;
import java.util.UUID;

@Component
public class UuidIdGenerator implements IdGenerator {
    @Override
    public String newId() {
        return UUID.randomUUID().toString();
    }
}
```

- [ ] **Step 3: Clock 빈**

`src/main/java/com/tainted/notification/config/DeterminismConfig.java`:
```java
package com.tainted.notification.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.time.Clock;

@Configuration
public class DeterminismConfig {
    @Bean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
```

- [ ] **Step 4: 컴파일 + 커밋**

Run: `./gradlew -q compileJava`
Expected: BUILD SUCCESSFUL.
```bash
git add -A
git commit -m "feat: add injectable Clock and IdGenerator beans"
```

---

## Task 3: 도메인 모델 + 이벤트 POJO

**Files:**
- Create: `domain/Notification.java`, `event/GraphUpdatedEvent.java`, `event/CommentCreatedEvent.java`

- [ ] **Step 1: `Notification` 레코드**

`src/main/java/com/tainted/notification/domain/Notification.java`:
```java
package com.tainted.notification.domain;

import java.time.Instant;

/**
 * 알림 도메인 객체. Redis 리스트에 JSON으로 직렬화·저장된다.
 *
 * @param id        알림 고유 식별자(UuidIdGenerator 생성)
 * @param userId    알림 수신 대상 사용자 id
 * @param type      알림 종류: "GRAPH_UPDATED" | "NEW_COMMENT"
 * @param payload   원본 이벤트 페이로드(JSON 문자열)
 * @param dedupKey  중복제거 키: type + ":" + eventId
 * @param createdAt 알림 생성 시각(UTC)
 */
public record Notification(
        String id,
        String userId,
        String type,
        String payload,
        String dedupKey,
        Instant createdAt
) {}
```

- [ ] **Step 2: `GraphUpdatedEvent` POJO**

`src/main/java/com/tainted/notification/event/GraphUpdatedEvent.java`:
```java
package com.tainted.notification.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * graph.updated 이벤트 페이로드.
 * 발행자(mindgraph-service)의 필드 정의를 이 서비스가 독립적으로 복제 보유.
 * 모르는 필드는 무시(FAIL_ON_UNKNOWN_PROPERTIES=false).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record GraphUpdatedEvent(
        String eventId,
        String userId,
        String diaryId,
        Integer nodeCount,
        String occurredAt
) {}
```

- [ ] **Step 3: `CommentCreatedEvent` POJO**

`src/main/java/com/tainted/notification/event/CommentCreatedEvent.java`:
```java
package com.tainted.notification.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * comment.created 이벤트 페이로드.
 * 발행자(community-service)의 필드 정의를 이 서비스가 독립적으로 복제 보유.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CommentCreatedEvent(
        String eventId,
        String postId,
        String commentId,
        String postAuthorUserId,
        String role,
        String occurredAt
) {}
```

- [ ] **Step 4: 컴파일 + 커밋**

Run: `./gradlew -q compileJava`
Expected: BUILD SUCCESSFUL.
```bash
git add -A
git commit -m "feat: Notification domain record and Kafka event POJOs"
```

---

## Task 4: Redis 설정 + NotificationService

**Files:**
- Create: `config/RedisConfig.java`, `service/NotificationService.java`

- [ ] **Step 1: `RedisConfig` — ObjectMapper 공유 + `StringRedisTemplate` 재사용**

`src/main/java/com/tainted/notification/config/RedisConfig.java`:
```java
package com.tainted.notification.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Redis는 Spring Boot 자동구성의 StringRedisTemplate을 그대로 쓴다.
 * ObjectMapper는 Notification JSON 직렬화에 공유하기 위해 별도 빈으로 등록한다.
 */
@Configuration
public class RedisConfig {

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }
}
```

- [ ] **Step 2: `NotificationService` 구현**

`src/main/java/com/tainted/notification/service/NotificationService.java`:
```java
package com.tainted.notification.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tainted.notification.domain.Notification;
import com.tainted.notification.id.IdGenerator;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 알림 저장 + 중복제거 + 조회.
 *
 * Redis 키 규약:
 *   notif:<userId>   — 사용자별 알림 JSON 목록(Redis List, LPUSH → LRANGE 최신 순)
 *   notif:dedup      — 처리된 dedupKey SET (중복제거용)
 */
@Service
public class NotificationService {

    static final String LIST_PREFIX = "notif:";
    static final String DEDUP_KEY   = "notif:dedup";

    private final StringRedisTemplate redis;
    private final ObjectMapper objectMapper;
    private final IdGenerator idGenerator;
    private final Clock clock;

    public NotificationService(StringRedisTemplate redis,
                               ObjectMapper objectMapper,
                               IdGenerator idGenerator,
                               Clock clock) {
        this.redis = redis;
        this.objectMapper = objectMapper;
        this.idGenerator = idGenerator;
        this.clock = clock;
    }

    /**
     * 알림을 저장한다. dedupKey 가 이미 존재하면 저장하지 않는다(멱등).
     *
     * @param userId   알림 수신 대상
     * @param type     알림 종류 ("GRAPH_UPDATED" | "NEW_COMMENT")
     * @param payload  원본 이벤트를 직렬화한 JSON 문자열
     * @param eventId  이벤트 고유 식별자(dedup 키 구성에 사용)
     */
    public void save(String userId, String type, String payload, String eventId) {
        String dedupKey = type + ":" + eventId;

        // SET에 dedupKey 추가 시도 — 이미 존재하면 false 반환 → 중복 스킵
        Boolean added = redis.opsForSet().add(DEDUP_KEY, dedupKey);
        if (added == null || added == 0) {
            return; // 중복 이벤트
        }

        Notification notification = new Notification(
                idGenerator.newId(),
                userId,
                type,
                payload,
                dedupKey,
                Instant.now(clock)
        );

        try {
            String json = objectMapper.writeValueAsString(notification);
            // LPUSH: 최신 알림이 리스트 앞에 오도록
            redis.opsForList().leftPush(LIST_PREFIX + userId, json);
        } catch (JsonProcessingException e) {
            // JSON 직렬화 실패 시 dedup SET에서 키 제거(재시도 허용)
            redis.opsForSet().remove(DEDUP_KEY, dedupKey);
            throw new RuntimeException("Notification serialization failed", e);
        }
    }

    /**
     * 사용자의 알림 목록을 최신 순으로 반환한다.
     *
     * @param userId 조회 대상 사용자 id
     * @return 알림 리스트(비어 있을 수 있음)
     */
    public List<Notification> findByUserId(String userId) {
        List<String> jsonList = redis.opsForList().range(LIST_PREFIX + userId, 0, -1);
        if (jsonList == null || jsonList.isEmpty()) {
            return Collections.emptyList();
        }
        List<Notification> result = new ArrayList<>(jsonList.size());
        for (String json : jsonList) {
            try {
                result.add(objectMapper.readValue(json, Notification.class));
            } catch (JsonProcessingException e) {
                // 역직렬화 실패한 항목은 로그만 남기고 건너뜀(서비스 내성 유지)
                // Spring Boot 기본 로깅 사용 — 추가 라이브러리 없음
                System.err.println("[NotificationService] skip malformed json: " + e.getMessage());
            }
        }
        return result;
    }
}
```

- [ ] **Step 3: 컴파일 + 커밋**

Run: `./gradlew -q compileJava`
Expected: BUILD SUCCESSFUL.
```bash
git add -A
git commit -m "feat: RedisConfig and NotificationService (save + dedup + query)"
```

---

## Task 5: Kafka Consumer + 내부 조회 API + RFC 7807 + OpenAPI

**Files:**
- Create: `kafka/NotificationKafkaConsumer.java`, `web/InternalNotificationController.java`, `error/GlobalExceptionHandler.java`, `config/OpenApiConfig.java`

- [ ] **Step 1: `NotificationKafkaConsumer`**

`src/main/java/com/tainted/notification/kafka/NotificationKafkaConsumer.java`:
```java
package com.tainted.notification.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tainted.notification.event.CommentCreatedEvent;
import com.tainted.notification.event.GraphUpdatedEvent;
import com.tainted.notification.service.NotificationService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * graph.updated 와 comment.created 토픽을 소비한다.
 * 아웃바운드 REST 호출 없음 — 수신 페이로드에서 대상 userId 를 직접 추출.
 */
@Component
public class NotificationKafkaConsumer {

    private final NotificationService notificationService;
    private final ObjectMapper objectMapper;

    public NotificationKafkaConsumer(NotificationService notificationService,
                                     ObjectMapper objectMapper) {
        this.notificationService = notificationService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "graph.updated", groupId = "${spring.kafka.consumer.group-id}")
    public void onGraphUpdated(String message) {
        try {
            GraphUpdatedEvent event = objectMapper.readValue(message, GraphUpdatedEvent.class);
            if (event.eventId() == null || event.userId() == null) {
                return; // 필수 필드 결측 — 무시
            }
            notificationService.save(event.userId(), "GRAPH_UPDATED", message, event.eventId());
        } catch (Exception e) {
            // 파싱 실패 시 소비 위치는 전진(재처리 루프 방지). 기본 로깅.
            System.err.println("[NotificationKafkaConsumer] graph.updated parse error: " + e.getMessage());
        }
    }

    @KafkaListener(topics = "comment.created", groupId = "${spring.kafka.consumer.group-id}")
    public void onCommentCreated(String message) {
        try {
            CommentCreatedEvent event = objectMapper.readValue(message, CommentCreatedEvent.class);
            if (event.eventId() == null || event.postAuthorUserId() == null) {
                return; // 필수 필드 결측 — 무시
            }
            notificationService.save(event.postAuthorUserId(), "NEW_COMMENT", message, event.eventId());
        } catch (Exception e) {
            System.err.println("[NotificationKafkaConsumer] comment.created parse error: " + e.getMessage());
        }
    }
}
```

- [ ] **Step 2: `InternalNotificationController`**

`src/main/java/com/tainted/notification/web/InternalNotificationController.java`:
```java
package com.tainted.notification.web;

import com.tainted.notification.domain.Notification;
import com.tainted.notification.service.NotificationService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 사용자별 알림 조회 — 블랙박스 테스트 및 내부 서비스 조회 전용.
 * 최신 알림이 리스트 앞에 온다(LPUSH 순서).
 */
@RestController
@RequestMapping("/internal/notifications")
public class InternalNotificationController {

    private final NotificationService notificationService;

    public InternalNotificationController(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @GetMapping("/{userId}")
    public List<Notification> getNotifications(@PathVariable String userId) {
        return notificationService.findByUserId(userId);
    }
}
```

- [ ] **Step 3: `GlobalExceptionHandler` (RFC 7807)**

`src/main/java/com/tainted/notification/error/GlobalExceptionHandler.java`:
```java
package com.tainted.notification.error;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidation(MethodArgumentNotValidException ex) {
        ProblemDetail pd = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        pd.setTitle("Validation failed");
        pd.setDetail(ex.getBindingResult().getAllErrors().isEmpty()
                ? "invalid request"
                : ex.getBindingResult().getAllErrors().get(0).getDefaultMessage());
        return pd;
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleGeneral(Exception ex) {
        ProblemDetail pd = ProblemDetail.forStatus(HttpStatus.INTERNAL_SERVER_ERROR);
        pd.setTitle("Internal server error");
        pd.setDetail(ex.getMessage());
        return pd;
    }
}
```

- [ ] **Step 4: `OpenApiConfig`**

`src/main/java/com/tainted/notification/config/OpenApiConfig.java`:
```java
package com.tainted.notification.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {
    @Bean
    public OpenAPI notificationOpenApi() {
        return new OpenAPI().info(new Info()
                .title("notification-service API")
                .version("0.1.0")
                .description("Kafka 이벤트 소비 기반 알림 생성 및 조회 (consumer-only, no outbound REST)"));
    }
}
```

- [ ] **Step 5: 컴파일 + 커밋**

Run: `./gradlew -q compileJava`
Expected: BUILD SUCCESSFUL.
```bash
git add -A
git commit -m "feat: Kafka consumer, internal query API, RFC7807, OpenAPI"
```

---

## Task 6: 통합 테스트 (Testcontainers + RestAssured)

**Files:**
- Test: `src/test/java/com/tainted/notification/NotificationIntegrationTest.java`

- [ ] **Step 1: 통합 테스트 작성**

`src/test/java/com/tainted/notification/NotificationIntegrationTest.java`:
```java
package com.tainted.notification;

import io.restassured.RestAssured;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static org.hamcrest.Matchers.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class NotificationIntegrationTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Container
    static GenericContainer<?> redis = new GenericContainer<>(
            DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
        r.add("spring.data.redis.host", redis::getHost);
        r.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @LocalServerPort
    int port;

    KafkaTemplate<String, String> producer;

    @BeforeEach
    void setUp() {
        RestAssured.port = port;
        producer = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(Map.of(
                BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer",
                VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer"
        )));
    }

    // ─── 보조 메서드 ────────────────────────────────────────────────────────────

    /**
     * 비동기 Kafka 소비를 기다리는 폴링 헬퍼.
     * GET /{path} 를 최대 {@code maxAttempts}회 호출하여 응답 배열 크기가 expectedSize 이상이 될 때까지 대기.
     */
    private void awaitNotificationCount(String userId, int expectedSize, int maxAttempts) throws InterruptedException {
        for (int i = 0; i < maxAttempts; i++) {
            int count = given()
                    .when().get("/internal/notifications/" + userId)
                    .then().statusCode(200)
                    .extract().jsonPath().getList("$").size();
            if (count >= expectedSize) {
                return;
            }
            Thread.sleep(300);
        }
        throw new AssertionError("알림이 " + maxAttempts * 300 + "ms 내에 도달하지 않았습니다. userId=" + userId);
    }

    // ─── 테스트 케이스 ──────────────────────────────────────────────────────────

    @Test
    void commentCreated_producesNewCommentNotification() throws Exception {
        String eventId = "evt-comment-001";
        String json = "{\"eventId\":\"" + eventId + "\",\"postId\":\"p1\",\"commentId\":\"c1\"," +
                "\"postAuthorUserId\":\"u1\",\"role\":\"empathic\",\"occurredAt\":\"2026-06-13T10:00:00Z\"}";

        producer.send("comment.created", json).get();

        awaitNotificationCount("u1", 1, 20);

        given().when().get("/internal/notifications/u1")
                .then().statusCode(200)
                .body("[0].userId", equalTo("u1"))
                .body("[0].type", equalTo("NEW_COMMENT"))
                .body("[0].dedupKey", equalTo("NEW_COMMENT:" + eventId));
    }

    @Test
    void commentCreated_dedup_sameEventIdSkipped() throws Exception {
        String eventId = "evt-comment-dedup-001";
        String json = "{\"eventId\":\"" + eventId + "\",\"postId\":\"p2\",\"commentId\":\"c2\"," +
                "\"postAuthorUserId\":\"u2\",\"role\":\"analytical\",\"occurredAt\":\"2026-06-13T10:01:00Z\"}";

        // 동일 이벤트를 두 번 발행
        producer.send("comment.created", json).get();
        producer.send("comment.created", json).get();

        awaitNotificationCount("u2", 1, 20);
        Thread.sleep(600); // 두 번째 메시지가 소비될 충분한 시간

        // 중복 스킵 — 여전히 1개
        given().when().get("/internal/notifications/u2")
                .then().statusCode(200)
                .body("$.size()", equalTo(1));
    }

    @Test
    void graphUpdated_producesGraphUpdatedNotification() throws Exception {
        String eventId = "evt-graph-001";
        String json = "{\"eventId\":\"" + eventId + "\",\"userId\":\"u3\",\"diaryId\":\"d1\"," +
                "\"nodeCount\":5,\"occurredAt\":\"2026-06-13T10:02:00Z\"}";

        producer.send("graph.updated", json).get();

        awaitNotificationCount("u3", 1, 20);

        given().when().get("/internal/notifications/u3")
                .then().statusCode(200)
                .body("[0].userId", equalTo("u3"))
                .body("[0].type", equalTo("GRAPH_UPDATED"))
                .body("[0].dedupKey", equalTo("GRAPH_UPDATED:" + eventId));
    }

    @Test
    void getNotifications_unknownUser_returnsEmptyList() {
        given().when().get("/internal/notifications/no-such-user")
                .then().statusCode(200)
                .body("$.size()", equalTo(0));
    }
}
```

- [ ] **Step 2: 전체 테스트 실행 (Docker 필요)**

Run: `./gradlew -q test`
Expected: PASS — `NotificationIntegrationTest`(4) 모두 통과. (Docker 데몬이 떠 있어야 Testcontainers 동작.)

- [ ] **Step 3: 커밋**

```bash
git add -A
git commit -m "test: Testcontainers (Redis + Kafka) + RestAssured integration tests"
```

---

## Task 7: Dockerfile + docker 프로파일

**Files:**
- Create: `Dockerfile`, `src/main/resources/application-docker.yml`

- [ ] **Step 1: docker 프로파일 설정 (compose 네트워크 호스트명)**

`src/main/resources/application-docker.yml`:
```yaml
spring:
  data:
    redis:
      host: redis
      port: 6379
      database: 3
  kafka:
    bootstrap-servers: kafka:29092
```

- [ ] **Step 2: 멀티스테이지 Dockerfile (non-root)**

`Dockerfile`:
```dockerfile
# build
FROM gradle:8.10-jdk17 AS build
WORKDIR /app
COPY build.gradle settings.gradle ./
COPY gradle ./gradle
RUN gradle -q dependencies --no-daemon || true
COPY src ./src
RUN gradle -q -x test bootJar --no-daemon

# run
FROM eclipse-temurin:17-jre
WORKDIR /app
# curl: compose/k8s 헬스체크가 /actuator/health 를 호출하는 데 사용.
# temurin JRE 기본 이미지에는 curl/wget 이 없으므로 명시적으로 설치한다.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd -r -u 1001 appuser
COPY --from=build /app/build/libs/notification-service-0.1.0.jar app.jar
USER appuser
EXPOSE 8087
HEALTHCHECK --interval=15s --timeout=5s --start-period=40s --retries=20 \
  CMD curl -fsS http://localhost:8087/actuator/health || exit 1
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 3: 이미지 빌드 확인**

Run: `docker build -t tainted-spring/notification:0.1.0 .`
Expected: 빌드 성공, 최종 이미지 생성.

- [ ] **Step 4: 커밋**

```bash
git add -A
git commit -m "build: Dockerfile and docker profile"
```

---

## Task 8: platform 레포에 notification 배선 (compose + k8s 스켈레톤)

> ⚠️ **작업 레포 전환**: 이 Task의 모든 파일 수정·커밋은 `tainted-spring-notification` 이 아니라 **`tainted-spring-platform` 레포**에서 한다. 각 Step의 경로를 확인할 것.

**Files (다른 레포 `tainted-spring-platform`):**
- Modify: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-platform/docker-compose.yml`
- Create: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-platform/k8s/notification/deployment.yaml`

- [ ] **Step 1: compose에 notification 서비스 추가**

`tainted-spring-platform/docker-compose.yml` 의 `services:` 아래에 추가 (앵커 `*java-service` 재사용):
```yaml
  notification:
    <<: *java-service
    build:
      context: ../tainted-spring-notification
    depends_on:
      redis:
        condition: service_healthy
      kafka:
        condition: service_healthy
    environment:
      JAVA_TOOL_OPTIONS: "${JAVA_TOOL_OPTIONS:-}"
      SPRING_PROFILES_ACTIVE: "docker"
    ports:
      - "8087:8087"
    healthcheck:
      # 런타임 이미지에 설치한 curl 사용. -f: 비2xx 응답이면 실패.
      test: ["CMD", "curl", "-fsS", "http://localhost:8087/actuator/health"]
      interval: 15s
      timeout: 5s
      retries: 20
      start_period: 40s
```

> 참고: `<<: *java-service` 가 `environment` 를 병합하지 않고 덮어쓰므로, 위에서 `JAVA_TOOL_OPTIONS`/`SPRING_PROFILES_ACTIVE` 를 명시적으로 다시 적었다. notification 서비스는 MySQL에 의존하지 않으므로 `depends_on` 에 mysql 이 없다.

- [ ] **Step 2: compose로 통합 기동 검증**

Run (platform 레포에서):
```bash
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-platform
docker compose up -d --build --wait notification
curl -s http://localhost:8087/actuator/health
```
Expected: `--wait` 가 notification 헬스체크 통과까지 블록. health `{"status":"UP"}`.

- [ ] **Step 3: k8s Deployment/Service 스켈레톤**

`tainted-spring-platform/k8s/notification/deployment.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tainted-spring-notification
  namespace: tainted-spring
spec:
  replicas: 1
  selector:
    matchLabels: { app: notification }
  template:
    metadata:
      labels: { app: notification }
    spec:
      containers:
        - name: notification
          image: tainted-spring/notification:0.1.0
          ports: [{ containerPort: 8087 }]
          env:
            - { name: SPRING_PROFILES_ACTIVE, value: "eks" }
            - { name: JAVA_TOOL_OPTIONS, value: "" }
          readinessProbe:
            httpGet: { path: /actuator/health, port: 8087 }
            initialDelaySeconds: 20
            periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: tainted-spring-notification
  namespace: tainted-spring
spec:
  selector: { app: notification }
  ports:
    - { port: 80, targetPort: 8087 }
```

- [ ] **Step 4: platform 레포 커밋**

```bash
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-platform
git add -A
git commit -m "feat: wire notification service into compose and k8s skeleton"
```

---

## Self-Review

**Spec coverage (설계문서 §3.6 대비):**
- `graph.updated` 소비 → `GRAPH_UPDATED` 알림 생성 → `NotificationKafkaConsumer.onGraphUpdated` + Task 6 테스트 ✔
- `comment.created` 소비 → `NEW_COMMENT` 알림 생성 → `NotificationKafkaConsumer.onCommentCreated` + Task 6 테스트 ✔
- Redis 기반 중복제거(`notif:dedup` SET) → `NotificationService.save` + dedup 테스트 ✔
- **아웃바운드 REST 없음**: 빌드 의존성에 `RestTemplate`/`WebClient` 없음, Kafka consumer 전용 ✔
- 알림 대상 = 이벤트 페이로드에서 직접 추출(`graph.updated.userId`, `comment.created.postAuthorUserId`) ✔
- `Notification{id, userId, type, payload, dedupKey, createdAt}` 엔티티 구조 ✔
- `GET /internal/notifications/{userId}` 조회 API(블랙박스 테스트 가능) ✔
- Redis 논리 DB 3 → `application.yml` + `application-docker.yml` ✔
- OpenAPI 노출 → `OpenApiConfig` + springdoc ✔
- RFC 7807 problem+json → `GlobalExceptionHandler` + `spring.mvc.problemdetails.enabled: true` ✔
- `/actuator/health` 유지, 추적 라이브러리 없음 → build.gradle에 actuator만, OTel/Micrometer Tracing 미포함 ✔
- 결정론(Clock/IdGenerator) → Task 2 ✔
- Kafka JSON 역직렬화, 추적 헤더 없음, 이벤트 POJO 자체 복제 보유 → Task 3 + `@JsonIgnoreProperties(ignoreUnknown=true)` ✔
- Docker(멀티스테이지/non-root) + curl 설치 + HEALTHCHECK + docker 프로파일 DNS → Task 7 ✔
- compose 배선(빈 JAVA_TOOL_OPTIONS 슬롯, depends_on redis+kafka service_healthy, start_period:40s, retries:20) + k8s 스켈레톤 → Task 8 ✔
- `docker compose up -d --wait` → Task 8 Step 2 ✔

**Placeholder scan:** 모든 코드 step에 완전한 소스, 모든 명령에 기대출력 명시. 플레이스홀더 없음. ✔

**Type/이름 일관성:**
- `IdGenerator.newId()` — Task 2 정의, Task 4 `NotificationService` 사용 일치 ✔
- `Notification` record 필드(`id`, `userId`, `type`, `payload`, `dedupKey`, `createdAt`) — Task 3 정의, Task 4 저장·Task 6 RestAssured 검증 키 일치 ✔
- Redis 키 `notif:<userId>` / `notif:dedup` — `NotificationService` 상수(`LIST_PREFIX`, `DEDUP_KEY`) 정의, 조회 로직 일치 ✔
- Kafka consumer groupId `${spring.kafka.consumer.group-id}` — `application.yml` 에 `notification-service` 로 정의 ✔
- 이벤트 POJO 필드(`GraphUpdatedEvent.userId`, `CommentCreatedEvent.postAuthorUserId`) — Task 3 정의, Task 5 consumer 추출 로직 일치 ✔
- jar 파일명 `notification-service-0.1.0.jar` — `settings.gradle` `rootProject.name='notification-service'` + `version='0.1.0'` 와 Dockerfile COPY 일치 ✔
- 포트 8087 — `application.yml`, Dockerfile `EXPOSE`, HEALTHCHECK, compose ports, k8s containerPort/readinessProbe 전부 일치 ✔
- Testcontainers Kafka 이미지 `confluentinc/cp-kafka:7.6.1` — `org.testcontainers:kafka:1.20.3` 의존성 사용 ✔
- `awaitNotificationCount` 헬퍼 — 비동기 소비 대기 로직(bounded retry polling)을 테스트 내부에 명시적 구현 ✔
