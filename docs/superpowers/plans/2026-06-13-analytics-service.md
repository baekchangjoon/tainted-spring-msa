# tainted-spring-analytics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `diary.created`, `post.created`, `mood.logged` 세 Kafka 토픽을 소비하여 사용자별 감정/활력 시계열을 Postgres에 집계하고, 무드 포인트 조회 API를 제공하는 `analytics-service`(Java 23 · Maven · Spring Boot 3.4.x · Postgres)를 Testcontainers/RestAssured로 검증 가능하게 구현한다.

**Architecture:** Spring MVC REST 서비스(Kafka consumer + 조회 API 전용). 세 Kafka 토픽을 `@KafkaListener`로 소비해 `MoodPoint` 엔티티로 변환·저장한다. 이벤트 `eventId`를 MoodPoint의 PK로 사용해 중복 소비를 방지(idempotency). 외부 서비스 호출 없음. 시간/ID는 주입 가능한 `Clock`/`IdGenerator` 빈으로 결정론 확보. 추적/관측 라이브러리는 넣지 않음(actuator health만 유지).

**Tech Stack:** Java 23, Maven, Spring Boot 3.4.1, Spring Data JPA, spring-kafka, springdoc-openapi 2.6, Postgres, Testcontainers(PostgreSQL + Kafka), RestAssured.

---

## 사전 조건

- `tainted-spring-platform` 계획이 완료되어 있어야 통합 실행이 쉽다(단위/통합 테스트는 Testcontainers로 자체 인프라를 띄우므로 platform 없이도 테스트는 통과한다).
- **레포 위치**: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-analytics/` (모든 경로는 이 루트 기준).
- **포트**: 8086. **패키지 루트**: `com.tainted.analytics`. **데이터스토어**: Postgres DB `analytics`.
- **로컬 사전 설치**: Docker 데몬, JDK 23, Maven 3.9+, Git. Docker는 Task 7 통합 테스트의 Testcontainers, Task 8 이미지 빌드, Task 9 compose에 필요. 단위 테스트(Task 3 `EventMappingTest`)는 Docker 없이도 동작한다.

## File Structure

```
pom.xml
Dockerfile
src/main/resources/application.yml
src/main/resources/application-docker.yml
src/main/java/com/tainted/analytics/
  AnalyticsApplication.java
  config/DeterminismConfig.java        # Clock 빈
  config/OpenApiConfig.java
  config/KafkaConfig.java              # ConsumerFactory, ConcurrentKafkaListenerContainerFactory
  id/IdGenerator.java                  # 인터페이스
  id/UuidIdGenerator.java              # 기본 구현
  domain/MoodPoint.java                # JPA 엔티티
  domain/MoodPointRepository.java
  event/DiaryCreatedEvent.java         # Kafka 이벤트 POJO (record)
  event/PostCreatedEvent.java
  event/MoodLoggedEvent.java
  listener/AnalyticsKafkaListener.java # @KafkaListener 3개 토픽
  service/AnalyticsService.java        # 저장 + 조회 로직
  web/AnalyticsController.java         # /internal/analytics/*
  web/dto/MoodPointResponse.java
  web/dto/UserMoodResponse.java        # MoodPoint 목록 + averageScore + count
  web/dto/GlobalAggregateResponse.java # totalPoints + countBySource
  error/GlobalExceptionHandler.java    # RFC 7807
src/test/java/com/tainted/analytics/
  event/EventMappingTest.java          # 단위: 이벤트 → MoodPoint 매핑
  AnalyticsIntegrationTest.java        # Testcontainers + RestAssured + Awaitility-style retry
```

---

## Task 1: 레포 스캐폴드 + 빌드 가능한 빈 앱

**Files:**
- Create: `pom.xml`, `src/main/java/com/tainted/analytics/AnalyticsApplication.java`, `src/main/resources/application.yml`, `.gitignore`

- [ ] **Step 1: 디렉토리 생성 + git init**

Run:
```bash
mkdir -p /Users/changjoonbaek/workspace_claude-code/tainted-spring-analytics
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-analytics
git init
mkdir -p src/main/java/com/tainted/analytics src/main/resources src/test/java/com/tainted/analytics
```

- [ ] **Step 2: `.gitignore`**

`.gitignore`:
```gitignore
target/
*.log
.DS_Store
.idea/
```

- [ ] **Step 3: `pom.xml`**

`pom.xml`:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>
  <parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.4.1</version>
    <relativePath/>
  </parent>
  <groupId>com.tainted</groupId>
  <artifactId>analytics-service</artifactId>
  <version>0.1.0</version>
  <properties>
    <java.version>23</java.version>
  </properties>
  <dependencies>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-web</artifactId></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-data-jpa</artifactId></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-validation</artifactId></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-actuator</artifactId></dependency>
    <dependency><groupId>org.postgresql</groupId><artifactId>postgresql</artifactId><scope>runtime</scope></dependency>
    <dependency><groupId>org.springdoc</groupId><artifactId>springdoc-openapi-starter-webmvc-ui</artifactId><version>2.6.0</version></dependency>
    <dependency><groupId>org.springframework.kafka</groupId><artifactId>spring-kafka</artifactId></dependency>

    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-test</artifactId><scope>test</scope></dependency>
    <dependency><groupId>org.springframework.kafka</groupId><artifactId>spring-kafka-test</artifactId><scope>test</scope></dependency>
    <dependency><groupId>org.testcontainers</groupId><artifactId>junit-jupiter</artifactId><version>1.21.4</version><scope>test</scope></dependency>
    <dependency><groupId>org.testcontainers</groupId><artifactId>postgresql</artifactId><version>1.21.4</version><scope>test</scope></dependency>
    <dependency><groupId>org.testcontainers</groupId><artifactId>kafka</artifactId><version>1.21.4</version><scope>test</scope></dependency>
    <dependency><groupId>io.rest-assured</groupId><artifactId>rest-assured</artifactId><scope>test</scope></dependency>
  </dependencies>
  <build>
    <plugins>
      <plugin><groupId>org.springframework.boot</groupId><artifactId>spring-boot-maven-plugin</artifactId></plugin>
    </plugins>
  </build>
</project>
```

- [ ] **Step 4: 메인 클래스 + 최소 설정**

`src/main/java/com/tainted/analytics/AnalyticsApplication.java`:
```java
package com.tainted.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AnalyticsApplication {
    public static void main(String[] args) {
        SpringApplication.run(AnalyticsApplication.class, args);
    }
}
```

`src/main/resources/application.yml`:
```yaml
server:
  port: 8086
spring:
  application:
    name: analytics-service
  mvc:
    problemdetails:
      enabled: true
  datasource:
    url: jdbc:postgresql://localhost:5432/analytics
    username: postgres
    password: postgrespw
  jpa:
    hibernate:
      ddl-auto: update
    open-in-view: false
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: analytics-service
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      properties:
        spring.json.trusted.packages: "com.tainted.analytics.event"
management:
  endpoints:
    web:
      exposure:
        include: health
```

- [ ] **Step 5: 컴파일 확인**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.

- [ ] **Step 6: 커밋**

```bash
git add -A
git commit -m "chore: scaffold analytics-service (buildable empty app)"
```

---

## Task 2: 결정론 빈 (Clock, IdGenerator)

**Files:**
- Create: `id/IdGenerator.java`, `id/UuidIdGenerator.java`, `config/DeterminismConfig.java`

- [ ] **Step 1: `IdGenerator` 인터페이스**

`src/main/java/com/tainted/analytics/id/IdGenerator.java`:
```java
package com.tainted.analytics.id;

/** 식별자 생성 추상화. 테스트에서 결정론적 구현으로 교체 가능. */
public interface IdGenerator {
    String newId();
}
```

- [ ] **Step 2: 기본 UUID 구현**

`src/main/java/com/tainted/analytics/id/UuidIdGenerator.java`:
```java
package com.tainted.analytics.id;

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

`src/main/java/com/tainted/analytics/config/DeterminismConfig.java`:
```java
package com.tainted.analytics.config;

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

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.
```bash
git add -A
git commit -m "feat: add injectable Clock and IdGenerator beans"
```

---

## Task 3: 이벤트 POJO + 도메인 엔티티 (TDD — 매핑 단위 테스트)

**Files:**
- Create: `event/DiaryCreatedEvent.java`, `event/PostCreatedEvent.java`, `event/MoodLoggedEvent.java`
- Create: `domain/MoodPoint.java`, `domain/MoodPointRepository.java`
- Test: `src/test/java/com/tainted/analytics/event/EventMappingTest.java`

- [ ] **Step 1: 실패하는 단위 테스트 작성**

`src/test/java/com/tainted/analytics/event/EventMappingTest.java`:
```java
package com.tainted.analytics.event;

import com.tainted.analytics.domain.MoodPoint;
import org.junit.jupiter.api.Test;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

class EventMappingTest {

    @Test
    void diaryCreatedMapsToMoodPoint() {
        DiaryCreatedEvent event = new DiaryCreatedEvent(
                "evt-1", "user-1", "diary-1", "불안", 3, Instant.parse("2026-01-01T00:00:00Z"));
        MoodPoint point = MoodPoint.fromDiaryCreated(event);
        assertEquals("evt-1", point.getId());
        assertEquals("user-1", point.getUserId());
        assertEquals("diary", point.getSource());
        assertEquals("불안", point.getLabel());
        assertEquals(3, point.getScore());
        assertEquals(Instant.parse("2026-01-01T00:00:00Z"), point.getOccurredAt());
    }

    @Test
    void moodLoggedMapsToMoodPoint() {
        MoodLoggedEvent event = new MoodLoggedEvent(
                "evt-2", "user-1", "community", "기쁨", 8, Instant.parse("2026-01-02T00:00:00Z"));
        MoodPoint point = MoodPoint.fromMoodLogged(event);
        assertEquals("evt-2", point.getId());
        assertEquals("user-1", point.getUserId());
        assertEquals("community", point.getSource());
        assertEquals("기쁨", point.getLabel());
        assertEquals(8, point.getScore());
    }

    @Test
    void postCreatedMapsToMoodPointWithScoreZero() {
        PostCreatedEvent event = new PostCreatedEvent(
                "evt-3", "post-1", "user-1", "daily", "😊", Instant.parse("2026-01-03T00:00:00Z"));
        MoodPoint point = MoodPoint.fromPostCreated(event);
        assertEquals("evt-3", point.getId());
        assertEquals("user-1", point.getUserId());
        assertEquals("community", point.getSource());
        assertEquals("😊", point.getLabel());
        assertEquals(0, point.getScore());
    }

    @Test
    void sameDiaryEventIdProducesSamePointId() {
        DiaryCreatedEvent e1 = new DiaryCreatedEvent(
                "evt-dup", "user-1", "diary-1", "슬픔", 2, Instant.parse("2026-01-01T00:00:00Z"));
        DiaryCreatedEvent e2 = new DiaryCreatedEvent(
                "evt-dup", "user-1", "diary-1", "슬픔", 2, Instant.parse("2026-01-01T00:00:00Z"));
        assertEquals(MoodPoint.fromDiaryCreated(e1).getId(),
                     MoodPoint.fromDiaryCreated(e2).getId(),
                     "같은 eventId는 같은 MoodPoint id — idempotency 기반");
    }
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `mvn -q -Dtest=EventMappingTest test`
Expected: FAIL — `DiaryCreatedEvent`, `PostCreatedEvent`, `MoodLoggedEvent`, `MoodPoint` 미존재로 컴파일 에러.

- [ ] **Step 3: 이벤트 POJO 3종 (record)**

`src/main/java/com/tainted/analytics/event/DiaryCreatedEvent.java`:
```java
package com.tainted.analytics.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;

/** diary-service 가 diary.created 토픽에 발행하는 이벤트. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record DiaryCreatedEvent(
        String eventId,
        String userId,
        String diaryId,
        String primaryEmotion,
        int energyScore,
        Instant occurredAt) {}
```

`src/main/java/com/tainted/analytics/event/PostCreatedEvent.java`:
```java
package com.tainted.analytics.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;

/** community-service 가 post.created 토픽에 발행하는 이벤트. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record PostCreatedEvent(
        String eventId,
        String postId,
        String userId,
        String category,
        String moodEmoji,
        Instant occurredAt) {}
```

`src/main/java/com/tainted/analytics/event/MoodLoggedEvent.java`:
```java
package com.tainted.analytics.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;

/** community-service 가 mood.logged 토픽에 발행하는 이벤트. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record MoodLoggedEvent(
        String eventId,
        String userId,
        String source,
        String mood,
        int score,
        Instant occurredAt) {}
```

- [ ] **Step 4: `MoodPoint` 엔티티 (팩터리 메서드 포함)**

`src/main/java/com/tainted/analytics/domain/MoodPoint.java`:
```java
package com.tainted.analytics.domain;

import com.tainted.analytics.event.DiaryCreatedEvent;
import com.tainted.analytics.event.MoodLoggedEvent;
import com.tainted.analytics.event.PostCreatedEvent;
import jakarta.persistence.*;
import java.time.Instant;

/**
 * Kafka 이벤트 하나를 집계한 무드 데이터 포인트.
 * id == eventId 이므로 중복 소비 시 INSERT OR IGNORE 로 idempotency 보장.
 */
@Entity
@Table(name = "mood_point")
public class MoodPoint {

    @Id
    @Column(length = 64)
    private String id;

    @Column(name = "user_id", nullable = false, length = 64)
    private String userId;

    /** 이벤트 출처 (diary / community). */
    @Column(nullable = false, length = 32)
    private String source;

    /** 감정 레이블 (primaryEmotion, mood, moodEmoji). */
    @Column(nullable = false, length = 64)
    private String label;

    /** 0~10 정수. post.created 는 0 고정. */
    @Column(nullable = false)
    private int score;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    protected MoodPoint() {}

    public MoodPoint(String id, String userId, String source, String label, int score, Instant occurredAt) {
        this.id = id;
        this.userId = userId;
        this.source = source;
        this.label = label;
        this.score = score;
        this.occurredAt = occurredAt;
    }

    // ── 팩터리 메서드 ──────────────────────────────────────────────

    public static MoodPoint fromDiaryCreated(DiaryCreatedEvent e) {
        return new MoodPoint(e.eventId(), e.userId(), "diary",
                e.primaryEmotion(), e.energyScore(), e.occurredAt());
    }

    public static MoodPoint fromMoodLogged(MoodLoggedEvent e) {
        return new MoodPoint(e.eventId(), e.userId(), e.source(),
                e.mood(), e.score(), e.occurredAt());
    }

    public static MoodPoint fromPostCreated(PostCreatedEvent e) {
        return new MoodPoint(e.eventId(), e.userId(), "community",
                e.moodEmoji(), 0, e.occurredAt());
    }

    // ── 접근자 ──────────────────────────────────────────────────────

    public String getId() { return id; }
    public String getUserId() { return userId; }
    public String getSource() { return source; }
    public String getLabel() { return label; }
    public int getScore() { return score; }
    public Instant getOccurredAt() { return occurredAt; }
}
```

- [ ] **Step 5: `MoodPointRepository`**

`src/main/java/com/tainted/analytics/domain/MoodPointRepository.java`:
```java
package com.tainted.analytics.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import java.util.List;

public interface MoodPointRepository extends JpaRepository<MoodPoint, String> {

    List<MoodPoint> findByUserIdOrderByOccurredAtAsc(String userId);

    @Query("SELECT COUNT(m) FROM MoodPoint m WHERE m.userId = :userId")
    long countByUserId(@Param("userId") String userId);

    @Query("SELECT COALESCE(AVG(m.score), 0) FROM MoodPoint m WHERE m.userId = :userId")
    double avgScoreByUserId(@Param("userId") String userId);

    @Query("SELECT COUNT(m) FROM MoodPoint m")
    long countAll();

    @Query("SELECT m.source, COUNT(m) FROM MoodPoint m GROUP BY m.source")
    List<Object[]> countGroupedBySource();
}
```

- [ ] **Step 6: 테스트 통과 확인**

Run: `mvn -q -Dtest=EventMappingTest test`
Expected: PASS (4 tests).

- [ ] **Step 7: 커밋**

```bash
git add -A
git commit -m "feat: Kafka event POJOs, MoodPoint entity, repository, and mapping unit tests"
```

---

## Task 4: Kafka 설정 + 리스너

**Files:**
- Create: `config/KafkaConfig.java`, `listener/AnalyticsKafkaListener.java`

- [ ] **Step 1: `KafkaConfig` — ObjectMapper 기반 역직렬화**

`src/main/java/com/tainted/analytics/config/KafkaConfig.java`:
```java
package com.tainted.analytics.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Bean
    public ObjectMapper kafkaObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper;
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }
}
```

- [ ] **Step 2: `AnalyticsKafkaListener`**

`src/main/java/com/tainted/analytics/listener/AnalyticsKafkaListener.java`:
```java
package com.tainted.analytics.listener;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tainted.analytics.event.DiaryCreatedEvent;
import com.tainted.analytics.event.MoodLoggedEvent;
import com.tainted.analytics.event.PostCreatedEvent;
import com.tainted.analytics.service.AnalyticsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AnalyticsKafkaListener {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsKafkaListener.class);

    private final AnalyticsService analyticsService;
    private final ObjectMapper objectMapper;

    public AnalyticsKafkaListener(AnalyticsService analyticsService, ObjectMapper kafkaObjectMapper) {
        this.analyticsService = analyticsService;
        this.objectMapper = kafkaObjectMapper;
    }

    @KafkaListener(topics = "diary.created", groupId = "${spring.kafka.consumer.group-id}")
    public void onDiaryCreated(String payload) {
        try {
            DiaryCreatedEvent event = objectMapper.readValue(payload, DiaryCreatedEvent.class);
            analyticsService.processDiaryCreated(event);
        } catch (Exception e) {
            log.error("Failed to process diary.created payload: {}", payload, e);
        }
    }

    @KafkaListener(topics = "mood.logged", groupId = "${spring.kafka.consumer.group-id}")
    public void onMoodLogged(String payload) {
        try {
            MoodLoggedEvent event = objectMapper.readValue(payload, MoodLoggedEvent.class);
            analyticsService.processMoodLogged(event);
        } catch (Exception e) {
            log.error("Failed to process mood.logged payload: {}", payload, e);
        }
    }

    @KafkaListener(topics = "post.created", groupId = "${spring.kafka.consumer.group-id}")
    public void onPostCreated(String payload) {
        try {
            PostCreatedEvent event = objectMapper.readValue(payload, PostCreatedEvent.class);
            analyticsService.processPostCreated(event);
        } catch (Exception e) {
            log.error("Failed to process post.created payload: {}", payload, e);
        }
    }
}
```

- [ ] **Step 3: 컴파일 + 커밋**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.
```bash
git add -A
git commit -m "feat: Kafka listener for diary.created, mood.logged, post.created"
```

---

## Task 5: AnalyticsService + DTO

**Files:**
- Create: `web/dto/MoodPointResponse.java`, `web/dto/UserMoodResponse.java`, `web/dto/GlobalAggregateResponse.java`
- Create: `service/AnalyticsService.java`

- [ ] **Step 1: DTO 3종**

`src/main/java/com/tainted/analytics/web/dto/MoodPointResponse.java`:
```java
package com.tainted.analytics.web.dto;

import java.time.Instant;

public record MoodPointResponse(
        String id,
        String source,
        String label,
        int score,
        Instant occurredAt) {}
```

`src/main/java/com/tainted/analytics/web/dto/UserMoodResponse.java`:
```java
package com.tainted.analytics.web.dto;

import java.util.List;

public record UserMoodResponse(
        String userId,
        List<MoodPointResponse> points,
        double averageScore,
        long count) {}
```

`src/main/java/com/tainted/analytics/web/dto/GlobalAggregateResponse.java`:
```java
package com.tainted.analytics.web.dto;

import java.util.Map;

public record GlobalAggregateResponse(
        long totalPoints,
        Map<String, Long> countBySource) {}
```

- [ ] **Step 2: `AnalyticsService`**

`src/main/java/com/tainted/analytics/service/AnalyticsService.java`:
```java
package com.tainted.analytics.service;

import com.tainted.analytics.domain.MoodPoint;
import com.tainted.analytics.domain.MoodPointRepository;
import com.tainted.analytics.event.DiaryCreatedEvent;
import com.tainted.analytics.event.MoodLoggedEvent;
import com.tainted.analytics.event.PostCreatedEvent;
import com.tainted.analytics.web.dto.GlobalAggregateResponse;
import com.tainted.analytics.web.dto.MoodPointResponse;
import com.tainted.analytics.web.dto.UserMoodResponse;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class AnalyticsService {

    private final MoodPointRepository repository;

    public AnalyticsService(MoodPointRepository repository) {
        this.repository = repository;
    }

    // ── Kafka 이벤트 처리 (idempotent: 같은 eventId 는 PK 충돌로 무시) ──

    @Transactional
    public void processDiaryCreated(DiaryCreatedEvent event) {
        saveIgnoreDuplicate(MoodPoint.fromDiaryCreated(event));
    }

    @Transactional
    public void processMoodLogged(MoodLoggedEvent event) {
        saveIgnoreDuplicate(MoodPoint.fromMoodLogged(event));
    }

    @Transactional
    public void processPostCreated(PostCreatedEvent event) {
        saveIgnoreDuplicate(MoodPoint.fromPostCreated(event));
    }

    /**
     * eventId == PK 이므로 동일 이벤트 재전달 시 PK 충돌이 발생한다.
     * DataIntegrityViolationException 을 정상 처리(중복 무시)하여 idempotency 보장.
     */
    private void saveIgnoreDuplicate(MoodPoint point) {
        if (!repository.existsById(point.getId())) {
            try {
                repository.save(point);
            } catch (DataIntegrityViolationException ignored) {
                // 동시 소비 또는 재전달로 인한 PK 중복 — 무시
            }
        }
    }

    // ── 조회 ────────────────────────────────────────────────────────

    @Transactional(readOnly = true)
    public UserMoodResponse getUserMood(String userId) {
        List<MoodPoint> points = repository.findByUserIdOrderByOccurredAtAsc(userId);
        List<MoodPointResponse> dtos = points.stream()
                .map(p -> new MoodPointResponse(p.getId(), p.getSource(),
                        p.getLabel(), p.getScore(), p.getOccurredAt()))
                .toList();
        double avg = points.stream().mapToInt(MoodPoint::getScore).average().orElse(0.0);
        return new UserMoodResponse(userId, dtos, avg, points.size());
    }

    @Transactional(readOnly = true)
    public GlobalAggregateResponse getGlobal() {
        long total = repository.countAll();
        List<Object[]> rows = repository.countGroupedBySource();
        Map<String, Long> countBySource = new LinkedHashMap<>();
        for (Object[] row : rows) {
            countBySource.put((String) row[0], (Long) row[1]);
        }
        return new GlobalAggregateResponse(total, countBySource);
    }
}
```

- [ ] **Step 3: 컴파일 + 커밋**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.
```bash
git add -A
git commit -m "feat: analytics service and response DTOs"
```

---

## Task 6: 컨트롤러 + RFC 7807 에러 + OpenAPI

**Files:**
- Create: `web/AnalyticsController.java`, `error/GlobalExceptionHandler.java`, `config/OpenApiConfig.java`

- [ ] **Step 1: `AnalyticsController`**

`src/main/java/com/tainted/analytics/web/AnalyticsController.java`:
```java
package com.tainted.analytics.web;

import com.tainted.analytics.service.AnalyticsService;
import com.tainted.analytics.web.dto.GlobalAggregateResponse;
import com.tainted.analytics.web.dto.UserMoodResponse;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/internal/analytics")
public class AnalyticsController {

    private final AnalyticsService analyticsService;

    public AnalyticsController(AnalyticsService analyticsService) {
        this.analyticsService = analyticsService;
    }

    /**
     * 특정 사용자의 MoodPoint 목록 + 단순 집계(averageScore, count)를 반환.
     * occurredAt 오름차순 정렬.
     */
    @GetMapping("/mood/{userId}")
    public UserMoodResponse getUserMood(@PathVariable String userId) {
        return analyticsService.getUserMood(userId);
    }

    /**
     * 전체 집계: totalPoints + source 별 count.
     */
    @GetMapping("/global")
    public GlobalAggregateResponse getGlobal() {
        return analyticsService.getGlobal();
    }
}
```

- [ ] **Step 2: RFC 7807 예외 핸들러**

`src/main/java/com/tainted/analytics/error/GlobalExceptionHandler.java`:
```java
package com.tainted.analytics.error;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentTypeMismatchException.class)
    public ProblemDetail handleTypeMismatch(MethodArgumentTypeMismatchException ex) {
        ProblemDetail pd = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        pd.setTitle("Invalid argument");
        pd.setDetail(ex.getMessage());
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

- [ ] **Step 3: OpenAPI 설정**

`src/main/java/com/tainted/analytics/config/OpenApiConfig.java`:
```java
package com.tainted.analytics.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {
    @Bean
    public OpenAPI analyticsOpenApi() {
        return new OpenAPI().info(new Info()
                .title("analytics-service API")
                .version("0.1.0")
                .description("Kafka 기반 감정 시계열 집계 및 무드 조회 API"));
    }
}
```

- [ ] **Step 4: 컴파일 + 커밋**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.
```bash
git add -A
git commit -m "feat: analytics controller, RFC7807 error handling, OpenAPI"
```

---

## Task 7: 통합 테스트 (Testcontainers Postgres + Kafka + RestAssured)

**Files:**
- Test: `src/test/java/com/tainted/analytics/AnalyticsIntegrationTest.java`

- [ ] **Step 1: 통합 테스트 작성**

`src/test/java/com/tainted/analytics/AnalyticsIntegrationTest.java`:
```java
package com.tainted.analytics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.restassured.RestAssured;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class AnalyticsIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("analytics")
            .withUsername("postgres")
            .withPassword("postgrespw");

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", postgres::getJdbcUrl);
        r.add("spring.datasource.username", postgres::getUsername);
        r.add("spring.datasource.password", postgres::getPassword);
        r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @LocalServerPort
    int port;

    private KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @BeforeEach
    void setUp() {
        RestAssured.port = port;
        Map<String, Object> producerProps = Map.of(
                "bootstrap.servers", kafka.getBootstrapServers(),
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer"
        );
        kafkaTemplate = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(producerProps));
    }

    /**
     * diary.created + mood.logged 각 1건 발행 → u1의 MoodPoint 2건 확인.
     * 비동기 소비이므로 최대 10초간 폴링.
     */
    @Test
    void publishDiaryAndMoodEvents_thenQueryReturns2Points() throws Exception {
        String diaryPayload = objectMapper.writeValueAsString(Map.of(
                "eventId", "evt-diary-u1",
                "userId", "u1",
                "diaryId", "d1",
                "primaryEmotion", "불안",
                "energyScore", 3,
                "occurredAt", "2026-01-01T00:00:00Z"
        ));
        String moodPayload = objectMapper.writeValueAsString(Map.of(
                "eventId", "evt-mood-u1",
                "userId", "u1",
                "source", "diary",
                "mood", "긴장",
                "score", 5,
                "occurredAt", "2026-01-01T01:00:00Z"
        ));

        kafkaTemplate.send("diary.created", diaryPayload).get();
        kafkaTemplate.send("mood.logged", moodPayload).get();

        // 비동기 소비 대기: 최대 10초간 2건이 될 때까지 폴링
        awaitMoodCount("u1", 2);

        given().when().get("/internal/analytics/mood/u1")
                .then().statusCode(200)
                .body("userId", equalTo("u1"))
                .body("count", equalTo(2))
                .body("points", hasSize(2))
                .body("points[0].label", equalTo("불안"))
                .body("points[0].score", equalTo(3))
                .body("points[1].label", equalTo("긴장"))
                .body("points[1].score", equalTo(5));
    }

    /**
     * 동일한 eventId 로 diary.created 를 두 번 발행해도 MoodPoint 1건만 저장됨(idempotency).
     */
    @Test
    void duplicateEventId_doesNotDoubleCount() throws Exception {
        String payload = objectMapper.writeValueAsString(Map.of(
                "eventId", "evt-dedup-u2",
                "userId", "u2",
                "diaryId", "d2",
                "primaryEmotion", "슬픔",
                "energyScore", 2,
                "occurredAt", "2026-02-01T00:00:00Z"
        ));

        // 동일 페이로드 2회 발행
        kafkaTemplate.send("diary.created", payload).get();
        kafkaTemplate.send("diary.created", payload).get();

        // 1건만 저장될 때까지 대기
        awaitMoodCount("u2", 1);

        // 추가 지연 후에도 여전히 1건
        Thread.sleep(2000);
        given().when().get("/internal/analytics/mood/u2")
                .then().statusCode(200)
                .body("count", equalTo(1));
    }

    /**
     * /internal/analytics/global 은 totalPoints 와 countBySource 를 포함한다.
     */
    @Test
    void globalAggregateContainsTotalPoints() throws Exception {
        String payload = objectMapper.writeValueAsString(Map.of(
                "eventId", "evt-global-u3",
                "userId", "u3",
                "diaryId", "d3",
                "primaryEmotion", "기쁨",
                "energyScore", 9,
                "occurredAt", "2026-03-01T00:00:00Z"
        ));
        kafkaTemplate.send("diary.created", payload).get();

        awaitTotalPointsAtLeast(1);

        given().when().get("/internal/analytics/global")
                .then().statusCode(200)
                .body("totalPoints", greaterThanOrEqualTo(1))
                .body("countBySource", notNullValue());
    }

    // ── 헬퍼: 비동기 소비 대기 ─────────────────────────────────────

    /** userId의 MoodPoint 수가 expected 이상이 될 때까지 최대 10초 폴링. */
    private void awaitMoodCount(String userId, int expected) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            int count = given().when().get("/internal/analytics/mood/" + userId)
                    .then().statusCode(200)
                    .extract().path("count");
            if (count >= expected) return;
            Thread.sleep(500);
        }
        throw new AssertionError("userId=" + userId + " did not reach " + expected + " MoodPoints within 10s");
    }

    /** 전체 totalPoints 가 atLeast 이상이 될 때까지 최대 10초 폴링. */
    private void awaitTotalPointsAtLeast(int atLeast) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            int total = given().when().get("/internal/analytics/global")
                    .then().statusCode(200)
                    .extract().path("totalPoints");
            if (total >= atLeast) return;
            Thread.sleep(500);
        }
        throw new AssertionError("totalPoints did not reach " + atLeast + " within 10s");
    }
}
```

- [ ] **Step 2: 전체 테스트 실행 (Docker 필요)**

Run: `mvn -q test`
Expected: PASS — `EventMappingTest`(4) + `AnalyticsIntegrationTest`(3) 모두 통과. (Docker 데몬이 떠 있어야 Testcontainers 동작.)

- [ ] **Step 3: 커밋**

```bash
git add -A
git commit -m "test: Testcontainers + RestAssured + idempotency integration tests"
```

---

## Task 8: Dockerfile + docker 프로파일

**Files:**
- Create: `Dockerfile`, `src/main/resources/application-docker.yml`

- [ ] **Step 1: docker 프로파일 설정 (compose 네트워크 호스트명)**

> `${POSTGRES_PASSWORD:postgrespw}` 값은 Task 9에서 compose `analytics` 서비스의 `environment:`로 주입된다(미주입 시 기본값 `postgrespw`).

`src/main/resources/application-docker.yml`:
```yaml
spring:
  datasource:
    url: jdbc:postgresql://postgres:5432/analytics
    username: postgres
    password: ${POSTGRES_PASSWORD:postgrespw}
  kafka:
    bootstrap-servers: kafka:29092
```

- [ ] **Step 2: 멀티스테이지 Dockerfile (non-root)**

`Dockerfile`:
```dockerfile
# build
FROM maven:3.9-eclipse-temurin-23 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B -DskipTests package

# run
FROM eclipse-temurin:23-jre
WORKDIR /app
# curl: compose/k8s 헬스체크가 /actuator/health 를 호출하는 데 사용.
# temurin JRE 기본 이미지에는 curl/wget 이 없으므로 명시적으로 설치한다.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd -r -u 1001 appuser
COPY --from=build /app/target/analytics-service-0.1.0.jar app.jar
USER appuser
EXPOSE 8086
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 3: 이미지 빌드 확인**

Run: `docker build -t tainted-spring/analytics:0.1.0 .`
Expected: 빌드 성공, 최종 이미지 생성.

- [ ] **Step 4: 커밋**

```bash
git add -A
git commit -m "build: Dockerfile and docker profile"
```

---

## Task 9: platform 레포에 analytics 배선 (compose + k8s 스켈레톤)

> ⚠️ **작업 레포 전환**: 이 Task의 모든 파일 수정·커밋은 `tainted-spring-analytics` 가 아니라 **`tainted-spring-platform` 레포**에서 한다. 각 Step의 경로를 확인할 것.

**Files (다른 레포 `tainted-spring-platform`):**
- Modify: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-platform/docker-compose.yml`
- Create: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-platform/k8s/analytics/deployment.yaml`

- [ ] **Step 1: compose에 analytics 서비스 추가**

`tainted-spring-platform/docker-compose.yml` 의 `services:` 아래에 추가 (앵커 `*java-service` 재사용):
```yaml
  analytics:
    <<: *java-service
    build:
      context: ../tainted-spring-analytics
    depends_on:
      postgres:
        condition: service_healthy
      kafka:
        condition: service_healthy
    environment:
      JAVA_TOOL_OPTIONS: "${JAVA_TOOL_OPTIONS:-}"
      SPRING_PROFILES_ACTIVE: "docker"
      POSTGRES_PASSWORD: "${POSTGRES_PASSWORD:-postgrespw}"
    ports:
      - "8086:8086"
    healthcheck:
      # 런타임 이미지에 설치한 curl 사용. -f: 비2xx 응답이면 실패 → actuator DOWN 시 unhealthy.
      test: ["CMD", "curl", "-fsS", "http://localhost:8086/actuator/health"]
      interval: 15s
      timeout: 5s
      retries: 20
      start_period: 40s
```

> 참고: `<<: *java-service` 가 `environment` 를 병합하지 않고 덮어쓰므로, 위에서 `JAVA_TOOL_OPTIONS`/`SPRING_PROFILES_ACTIVE` 를 명시적으로 다시 적었다.

- [ ] **Step 2: compose로 통합 기동 검증**

Run (platform 레포에서):
```bash
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-platform
docker compose up -d --build --wait analytics
curl -s http://localhost:8086/actuator/health
curl -s http://localhost:8086/internal/analytics/global
```
Expected: `--wait` 가 analytics 헬스체크 통과까지 블록. health `{"status":"UP"}`, global 응답에 `totalPoints`·`countBySource` 포함.

- [ ] **Step 3: k8s Deployment/Service 스켈레톤**

`tainted-spring-platform/k8s/analytics/deployment.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tainted-spring-analytics
  namespace: tainted-spring
spec:
  replicas: 1
  selector:
    matchLabels: { app: analytics }
  template:
    metadata:
      labels: { app: analytics }
    spec:
      containers:
        - name: analytics
          image: tainted-spring/analytics:0.1.0
          ports: [{ containerPort: 8086 }]
          env:
            - { name: SPRING_PROFILES_ACTIVE, value: "eks" }
            - { name: JAVA_TOOL_OPTIONS, value: "" }
          readinessProbe:
            httpGet: { path: /actuator/health, port: 8086 }
            initialDelaySeconds: 20
            periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: tainted-spring-analytics
  namespace: tainted-spring
spec:
  selector: { app: analytics }
  ports:
    - { port: 80, targetPort: 8086 }
```

- [ ] **Step 4: platform 레포 커밋**

```bash
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-platform
git add -A
git commit -m "feat: wire analytics service into compose and k8s skeleton"
```

---

## Self-Review

**Spec coverage (설계문서 §3.7 대비):**
- `diary.created` 소비 → `MoodPoint(source='diary', label=primaryEmotion, score=energyScore)` → Task 3 팩터리 + Task 4 리스너 ✔
- `mood.logged` 소비 → `MoodPoint(source=event.source, label=mood, score=score)` → Task 3 팩터리 + Task 4 리스너 ✔
- `post.created` 소비 → `MoodPoint(source='community', label=moodEmoji, score=0)` → Task 3 팩터리 + Task 4 리스너 ✔
- 이벤트 발행 없음(consumer 전용) → `pom.xml` 에 KafkaProducer 설정 없음, `KafkaTemplate` 빈 미등록 ✔
- `GET /internal/analytics/mood/{userId}` → points 목록(occurredAt 오름차순) + averageScore + count → Task 5·6 ✔
- `GET /internal/analytics/global` → totalPoints + countBySource → Task 5·6 ✔
- Idempotency: `eventId == MoodPoint.id(PK)` → 중복 소비 시 `existsById` 체크 + PK 충돌 무시 → Task 5, 통합 테스트 검증 ✔
- Postgres DB `analytics`, user `postgres`/`postgrespw` → application.yml + Task 7 Testcontainers ✔
- Kafka `bootstrap-servers` 환경별 분리(로컬 9092 / docker kafka:29092) → application.yml + application-docker.yml ✔
- OpenAPI 노출 → Task 6 `OpenApiConfig` + springdoc ✔
- RFC 7807 problem+json → `spring.mvc.problemdetails.enabled: true` + Task 6 `GlobalExceptionHandler` ✔
- `/actuator/health` 유지, 추적 라이브러리 없음 → `pom.xml` actuator만, OTel/Sleuth 미포함 ✔
- 결정론(Clock/IdGenerator) → Task 2 ✔
- Docker(멀티스테이지/non-root/curl 설치/포트 8086) + docker 프로파일 DNS → Task 8 ✔
- compose 배선(빈 `JAVA_TOOL_OPTIONS` 슬롯 + `depends_on` postgres+kafka `service_healthy`) + k8s 스켈레톤 → Task 9 ✔
- Java 23 record 사용(이벤트 POJO 3종, DTO 3종) → Tasks 3·5 ✔

**Placeholder scan:** 모든 코드 step에 완전한 소스, 모든 명령에 기대 출력 명시. 플레이스홀더 없음. ✔

**Type/이름 일관성:**
- `MoodPoint.fromDiaryCreated/fromMoodLogged/fromPostCreated` — Task 3 정의, Task 4·5 사용 일치 ✔
- `MoodPointRepository` 조회 메서드 — Task 3 정의, Task 5 사용 일치 ✔
- DTO 필드명(`userId`, `points`, `averageScore`, `count`, `totalPoints`, `countBySource`) — Task 5 정의, Task 7 RestAssured 검증 키와 일치 ✔
- jar 파일명 `analytics-service-0.1.0.jar` — `pom.xml` `artifactId`+`version` 과 Dockerfile COPY 일치 ✔
- Kafka topic 명칭(`diary.created`, `post.created`, `mood.logged`) — 카탈로그 §4 그대로 ✔
- `@KafkaListener groupId` — `${spring.kafka.consumer.group-id}` 프로퍼티 참조, application.yml `analytics-service` 와 일치 ✔
