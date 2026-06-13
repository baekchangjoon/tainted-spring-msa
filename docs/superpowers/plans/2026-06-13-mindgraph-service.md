# tainted-spring-mindgraph Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `diary.created` 이벤트를 소비해 diary-service에서 복호화된 일기 본문을 OpenFeign으로 가져오고, **규칙기반 결정론적** 마음 그래프로 변환·저장한 뒤 `graph.updated`를 발행하는 `mindgraph-service`(Java 11 · Maven · Spring Boot 2.7 · Postgres + Redis)를 RestAssured/Testcontainers로 검증 가능하게 구현한다.

**Architecture:** Spring MVC REST 서비스 + Kafka consumer/producer. diary 본문 조회는 OpenFeign `DiaryContentClient`(`@FeignClient(name="diary", url="${services.diary.url}")`)로 동기 호출. 그래프 추출은 LLM이 아닌 **고정 키워드 사전 기반 결정론 규칙**(같은 본문 → 항상 같은 노드/링크). 그래프는 Postgres(`mindgraph` DB)에 `GraphRecord` 엔티티로, nodes/links는 `ObjectMapper`로 직렬화한 JSON 텍스트 컬럼에 저장. 사용자별 최신 그래프는 Redis(논리 DB 1)에 캐시. 시간/ID는 주입 가능한 `Clock`/`IdGenerator` 빈으로 결정론 확보. 추적/관측 라이브러리는 넣지 않음(actuator health만 유지, correlation/trace id·MDC 주입 없음).

**Tech Stack:** Java 11, Maven, Spring Boot 2.7.18, Spring Cloud OpenFeign(2021.0.8 BOM), Spring Data JPA, Spring Data Redis, spring-kafka(Boot 관리), springdoc-openapi-ui 1.7.0, PostgreSQL, Redis 7, Testcontainers 1.21.4, RestAssured.

---

## 사전 조건

- `tainted-spring-platform` 계획이 완료되어 있어야 통합 실행이 쉽다(단위/통합 테스트는 Testcontainers로 자체 인프라를 띄우므로 platform 없이도 테스트는 통과한다). diary-service는 통합 테스트에서 `@TestConfiguration` 스텁 Feign 빈으로 대체하므로 실제로 떠 있을 필요가 없다.
- **레포 위치**: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-mindgraph/` (모든 경로는 이 루트 기준).
- **포트**: 8083. **패키지 루트**: `com.tainted.mindgraph`.
- **데이터스토어**: Postgres DB `mindgraph` (호스트 `postgres:5432`, user `postgres` / pw `postgrespw`), Redis 논리 DB **1**(캐시). 테스트에서는 Testcontainers PostgreSQLContainer + GenericContainer(redis) + Kafka 컨테이너로 대체.
- **로컬 사전 설치**: **Docker 데몬**(Task 8 통합 테스트의 Testcontainers, Task 9 이미지 빌드, Task 10 compose에 필요) + **JDK 11** + **Maven 3.9+** + **Git**. 단, 단위 테스트 `RuleBasedGraphExtractorTest`(Task 5)는 Docker 없이도 동작한다.

## File Structure

```
pom.xml
Dockerfile
src/main/resources/application.yml
src/main/resources/application-docker.yml
src/main/java/com/tainted/mindgraph/
  MindgraphApplication.java            # @EnableFeignClients
  config/DeterminismConfig.java        # Clock 빈
  config/OpenApiConfig.java
  id/IdGenerator.java                  # 인터페이스
  id/UuidIdGenerator.java              # 기본 구현
  client/DiaryContentClient.java       # @FeignClient(name="diary")
  client/DiaryContent.java             # {title, content} 평범한 클래스
  graph/MindNode.java                  # 평범한 클래스
  graph/MindLink.java                  # 평범한 클래스
  graph/MindGraph.java                 # nodes + links 컨테이너
  graph/RuleBasedGraphExtractor.java   # 결정론 규칙기반 추출기
  domain/GraphRecord.java              # JPA 엔티티 (diaryId PK)
  domain/GraphRecordRepository.java
  cache/GraphCache.java                # Redis index 1 캐시
  event/DiaryCreatedEvent.java         # 소비 POJO
  event/GraphUpdatedEvent.java         # 발행 POJO
  event/DiaryCreatedConsumer.java      # @KafkaListener
  event/GraphEventPublisher.java       # KafkaTemplate 발행
  service/GraphService.java            # 추출→저장→캐시→발행
  web/GraphController.java             # /internal/graphs/*
  web/dto/GraphResponse.java
  error/GraphNotFoundException.java
  error/GlobalExceptionHandler.java    # RFC 7807 (Map 기반)
src/test/java/com/tainted/mindgraph/
  graph/RuleBasedGraphExtractorTest.java   # 단위 (결정론)
  MindgraphIntegrationTest.java            # Testcontainers + Kafka + RestAssured
```

---

## Task 1: 레포 스캐폴드 + 빌드 가능한 빈 앱

**Files:**
- Create: `pom.xml`, `src/main/java/com/tainted/mindgraph/MindgraphApplication.java`, `src/main/resources/application.yml`, `.gitignore`

- [ ] **Step 1: 디렉토리 생성 + git init**

Run:
```bash
mkdir -p /Users/changjoonbaek/workspace_claude-code/tainted-spring-mindgraph
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-mindgraph
git init
mkdir -p src/main/java/com/tainted/mindgraph src/main/resources src/test/java/com/tainted/mindgraph
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

> Boot 2.7.18 + Java 11. OpenFeign은 Spring Cloud `2021.0.8` BOM(`dependencyManagement`)로 버전을 관리하므로 starter에는 명시 버전을 적지 않는다. springdoc는 1.x(`springdoc-openapi-ui:1.7.0`)이다.

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
    <version>2.7.18</version>
    <relativePath/>
  </parent>
  <groupId>com.tainted</groupId>
  <artifactId>mindgraph-service</artifactId>
  <version>0.1.0</version>
  <properties>
    <java.version>11</java.version>
    <spring-cloud.version>2021.0.8</spring-cloud.version>
  </properties>
  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>org.springframework.cloud</groupId>
        <artifactId>spring-cloud-dependencies</artifactId>
        <version>${spring-cloud.version}</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>
  <dependencies>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-web</artifactId></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-data-jpa</artifactId></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-data-redis</artifactId></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-validation</artifactId></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-actuator</artifactId></dependency>
    <dependency><groupId>org.springframework.cloud</groupId><artifactId>spring-cloud-starter-openfeign</artifactId></dependency>
    <dependency><groupId>org.springframework.kafka</groupId><artifactId>spring-kafka</artifactId></dependency>
    <dependency><groupId>org.postgresql</groupId><artifactId>postgresql</artifactId><scope>runtime</scope></dependency>
    <dependency><groupId>org.springdoc</groupId><artifactId>springdoc-openapi-ui</artifactId><version>1.7.0</version></dependency>

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

- [ ] **Step 4: 메인 클래스 (`@EnableFeignClients`) + 최소 설정**

`src/main/java/com/tainted/mindgraph/MindgraphApplication.java`:
```java
package com.tainted.mindgraph;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableFeignClients
public class MindgraphApplication {
    public static void main(String[] args) {
        SpringApplication.run(MindgraphApplication.class, args);
    }
}
```

`src/main/resources/application.yml`:
```yaml
server:
  port: 8083
spring:
  application:
    name: mindgraph-service
  datasource:
    url: jdbc:postgresql://localhost:5432/mindgraph
    username: postgres
    password: postgrespw
  jpa:
    hibernate:
      ddl-auto: update
    open-in-view: false
    properties:
      hibernate:
        dialect: org.hibernate.dialect.PostgreSQLDialect
  redis:
    host: localhost
    port: 6379
    database: 1
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: mindgraph-service
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
services:
  diary:
    url: http://localhost:8082
mindgraph:
  topics:
    diary-created: diary.created
    graph-updated: graph.updated
management:
  endpoints:
    web:
      exposure:
        include: health
```

> 참고: Boot 2.7에서 Redis 프로퍼티 경로는 `spring.redis.*`(Boot 3의 `spring.data.redis.*` 아님)이다.

- [ ] **Step 5: 컴파일 확인**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.

- [ ] **Step 6: 커밋**

```bash
git add -A
git commit -m "chore: scaffold mindgraph-service (buildable empty app)"
```

---

## Task 2: 결정론 빈 (Clock, IdGenerator)

**Files:**
- Create: `id/IdGenerator.java`, `id/UuidIdGenerator.java`, `config/DeterminismConfig.java`

- [ ] **Step 1: `IdGenerator` 인터페이스**

`src/main/java/com/tainted/mindgraph/id/IdGenerator.java`:
```java
package com.tainted.mindgraph.id;

/** 식별자 생성 추상화. 테스트에서 결정론적 구현으로 교체 가능. */
public interface IdGenerator {
    String newId();
}
```

- [ ] **Step 2: 기본 UUID 구현**

`src/main/java/com/tainted/mindgraph/id/UuidIdGenerator.java`:
```java
package com.tainted.mindgraph.id;

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

`src/main/java/com/tainted/mindgraph/config/DeterminismConfig.java`:
```java
package com.tainted.mindgraph.config;

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

## Task 3: OpenFeign diary 본문 클라이언트

**Files:**
- Create: `client/DiaryContent.java`, `client/DiaryContentClient.java`

> diary-service의 복호화 엔드포인트 `GET /internal/diaries/{id}/content`는 `{title, content}` JSON을 반환한다. `DiaryContent`는 Java 11이라 record를 쓸 수 없으므로 기본 생성자 + setter/getter를 갖춘 평범한 클래스로 둔다(Jackson 역직렬화용).

- [ ] **Step 1: `DiaryContent` 평범한 클래스**

`src/main/java/com/tainted/mindgraph/client/DiaryContent.java`:
```java
package com.tainted.mindgraph.client;

/** diary-service 복호화 응답 본문. Java 11 — record 사용 금지, 평범한 클래스. */
public class DiaryContent {

    private String title;
    private String content;

    public DiaryContent() {
    }

    public DiaryContent(String title, String content) {
        this.title = title;
        this.content = content;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
```

- [ ] **Step 2: `DiaryContentClient` Feign 인터페이스**

`src/main/java/com/tainted/mindgraph/client/DiaryContentClient.java`:
```java
package com.tainted.mindgraph.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@FeignClient(name = "diary", url = "${services.diary.url}")
public interface DiaryContentClient {

    @GetMapping("/internal/diaries/{id}/content")
    DiaryContent getContent(@PathVariable("id") String id);
}
```

- [ ] **Step 3: 컴파일 + 커밋**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.
```bash
git add -A
git commit -m "feat: OpenFeign DiaryContentClient for decrypted diary body"
```

---

## Task 4: 그래프 모델 (평범한 클래스)

**Files:**
- Create: `graph/MindNode.java`, `graph/MindLink.java`, `graph/MindGraph.java`

> 모두 Java 11 평범한 클래스(record 금지). Jackson 직렬화/역직렬화를 위해 기본 생성자 + getter/setter를 둔다.

- [ ] **Step 1: `MindNode`**

`src/main/java/com/tainted/mindgraph/graph/MindNode.java`:
```java
package com.tainted.mindgraph.graph;

/**
 * 마음 그래프 노드.
 * group ∈ {STRESSOR, SUPPORTER, COPING_MECHANISM, EMOTION}
 * sentiment ∈ {POSITIVE, NEGATIVE, NEUTRAL}
 */
public class MindNode {

    private String id;        // 라벨 (예: "직장 상사")
    private String group;
    private double intensity;
    private String sentiment;

    public MindNode() {
    }

    public MindNode(String id, String group, double intensity, String sentiment) {
        this.id = id;
        this.group = group;
        this.intensity = intensity;
        this.sentiment = sentiment;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getGroup() { return group; }
    public void setGroup(String group) { this.group = group; }

    public double getIntensity() { return intensity; }
    public void setIntensity(double intensity) { this.intensity = intensity; }

    public String getSentiment() { return sentiment; }
    public void setSentiment(String sentiment) { this.sentiment = sentiment; }
}
```

- [ ] **Step 2: `MindLink`**

`src/main/java/com/tainted/mindgraph/graph/MindLink.java`:
```java
package com.tainted.mindgraph.graph;

/**
 * 마음 그래프 간선.
 * relationType ∈ {TRIGGERS, MITIGATES, ASSOCIATED_WITH}
 */
public class MindLink {

    private String source;
    private String target;
    private String relationType;

    public MindLink() {
    }

    public MindLink(String source, String target, String relationType) {
        this.source = source;
        this.target = target;
        this.relationType = relationType;
    }

    public String getSource() { return source; }
    public void setSource(String source) { this.source = source; }

    public String getTarget() { return target; }
    public void setTarget(String target) { this.target = target; }

    public String getRelationType() { return relationType; }
    public void setRelationType(String relationType) { this.relationType = relationType; }
}
```

- [ ] **Step 3: `MindGraph` 컨테이너**

`src/main/java/com/tainted/mindgraph/graph/MindGraph.java`:
```java
package com.tainted.mindgraph.graph;

import java.util.ArrayList;
import java.util.List;

/** 노드/링크 묶음. 직렬화 시 nodes/links 두 배열로 저장된다. */
public class MindGraph {

    private List<MindNode> nodes = new ArrayList<>();
    private List<MindLink> links = new ArrayList<>();

    public MindGraph() {
    }

    public MindGraph(List<MindNode> nodes, List<MindLink> links) {
        this.nodes = nodes;
        this.links = links;
    }

    public List<MindNode> getNodes() { return nodes; }
    public void setNodes(List<MindNode> nodes) { this.nodes = nodes; }

    public List<MindLink> getLinks() { return links; }
    public void setLinks(List<MindLink> links) { this.links = links; }
}
```

- [ ] **Step 4: 컴파일 + 커밋**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.
```bash
git add -A
git commit -m "feat: MindNode/MindLink/MindGraph plain model classes"
```

---

## Task 5: 규칙기반 결정론 그래프 추출기 (TDD)

**Files:**
- Create: `graph/RuleBasedGraphExtractor.java`
- Test: `src/test/java/com/tainted/mindgraph/graph/RuleBasedGraphExtractorTest.java`

> 핵심 결정론 컴포넌트. 고정 키워드 사전을 `contains`로 스캔해 매칭된 키워드마다 노드를 만든다. `intensity`는 본문 내 등장 횟수(상한 적용). 링크 규칙: 모든 STRESSOR → TRIGGERS → 모든 EMOTION, 모든 COPING_MECHANISM → MITIGATES → 모든 EMOTION, 모든 SUPPORTER → ASSOCIATED_WITH → 모든 EMOTION. 키워드 사전은 **삽입 순서가 보장되는 `LinkedHashMap`**으로 두어 노드/링크 산출 순서까지 결정론적이게 한다.

- [ ] **Step 1: 실패하는 테스트 작성**

`src/test/java/com/tainted/mindgraph/graph/RuleBasedGraphExtractorTest.java`:
```java
package com.tainted.mindgraph.graph;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class RuleBasedGraphExtractorTest {

    private final RuleBasedGraphExtractor extractor = new RuleBasedGraphExtractor();

    @Test
    void extractsDeterministicNodesAndLinksForFixedKoreanText() {
        // 상사(STRESSOR/NEGATIVE), 운동(COPING_MECHANISM/POSITIVE),
        // 불안(EMOTION/NEGATIVE), 평온(EMOTION/POSITIVE) 키워드 포함.
        String content = "오늘 직장 상사 때문에 불안했지만 저녁에 운동을 하고 나니 평온해졌다. 상사 생각은 여전했다.";

        MindGraph graph = extractor.extract(content);

        // 노드: 상사, 운동, 불안, 평온 → 4개
        List<String> nodeIds = graph.getNodes().stream()
                .map(MindNode::getId).collect(Collectors.toList());
        assertEquals(List.of("상사", "운동", "불안", "평온"), nodeIds,
                "키워드 사전 삽입 순서대로 결정론적으로 노드가 생성된다");

        MindNode boss = graph.getNodes().get(0);
        assertEquals("STRESSOR", boss.getGroup());
        assertEquals("NEGATIVE", boss.getSentiment());
        assertEquals(2.0, boss.getIntensity(), 0.0001, "'상사'가 본문에 2번 등장");

        // 링크: STRESSOR(상사) → TRIGGERS → {불안, 평온} = 2
        //       COPING(운동) → MITIGATES → {불안, 평온} = 2
        // 총 4개
        assertEquals(4, graph.getLinks().size());

        List<String> linkSig = graph.getLinks().stream()
                .map(l -> l.getSource() + "-" + l.getRelationType() + "->" + l.getTarget())
                .collect(Collectors.toList());
        assertEquals(List.of(
                "상사-TRIGGERS->불안",
                "상사-TRIGGERS->평온",
                "운동-MITIGATES->불안",
                "운동-MITIGATES->평온"
        ), linkSig);
    }

    @Test
    void sameInputProducesSameOutput() {
        String content = "엄마와 통화하니 행복했다.";
        MindGraph a = extractor.extract(content);
        MindGraph b = extractor.extract(content);
        assertEquals(a.getNodes().size(), b.getNodes().size());
        assertEquals(a.getNodes().get(0).getId(), b.getNodes().get(0).getId());
    }

    @Test
    void noKeywordsYieldsEmptyGraph() {
        MindGraph graph = extractor.extract("특별한 일 없는 하루였다.");
        assertTrue(graph.getNodes().isEmpty());
        assertTrue(graph.getLinks().isEmpty());
    }
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `mvn -q -Dtest=RuleBasedGraphExtractorTest test`
Expected: FAIL — `RuleBasedGraphExtractor` 미존재로 컴파일 에러.

- [ ] **Step 3: 최소 구현**

`src/main/java/com/tainted/mindgraph/graph/RuleBasedGraphExtractor.java`:
```java
package com.tainted.mindgraph.graph;

import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 결정론적 규칙기반 마음 그래프 추출기. LLM 없음.
 *
 * 고정 키워드 사전(삽입 순서 보장 LinkedHashMap)을 본문에서 contains 로 스캔하여
 * 매칭된 키워드마다 노드를 1개 만든다. intensity = 본문 내 등장 횟수(INTENSITY_CAP 상한).
 *
 * 링크 규칙(모두 결정론):
 *   STRESSOR          → TRIGGERS        → 모든 EMOTION
 *   COPING_MECHANISM  → MITIGATES       → 모든 EMOTION
 *   SUPPORTER         → ASSOCIATED_WITH → 모든 EMOTION
 *
 * 같은 본문 → 항상 같은 노드/링크(순서 포함).
 */
@Component
public class RuleBasedGraphExtractor {

    private static final double INTENSITY_CAP = 5.0;

    /** 키워드 → 분류 정의. 삽입 순서가 곧 노드 산출 순서. */
    private static final Map<String, Rule> DICTIONARY = buildDictionary();

    private static Map<String, Rule> buildDictionary() {
        Map<String, Rule> m = new LinkedHashMap<>();
        // STRESSOR (부정)
        m.put("상사", new Rule("STRESSOR", "NEGATIVE"));
        m.put("직장", new Rule("STRESSOR", "NEGATIVE"));
        m.put("회사", new Rule("STRESSOR", "NEGATIVE"));
        // SUPPORTER (긍정)
        m.put("가족", new Rule("SUPPORTER", "POSITIVE"));
        m.put("엄마", new Rule("SUPPORTER", "POSITIVE"));
        m.put("아빠", new Rule("SUPPORTER", "POSITIVE"));
        m.put("친구", new Rule("SUPPORTER", "POSITIVE"));
        // COPING_MECHANISM (긍정)
        m.put("운동", new Rule("COPING_MECHANISM", "POSITIVE"));
        m.put("산책", new Rule("COPING_MECHANISM", "POSITIVE"));
        m.put("노래방", new Rule("COPING_MECHANISM", "POSITIVE"));
        // EMOTION (단어별 감정)
        m.put("우울", new Rule("EMOTION", "NEGATIVE"));
        m.put("불안", new Rule("EMOTION", "NEGATIVE"));
        m.put("분노", new Rule("EMOTION", "NEGATIVE"));
        m.put("평온", new Rule("EMOTION", "POSITIVE"));
        m.put("행복", new Rule("EMOTION", "POSITIVE"));
        return m;
    }

    public MindGraph extract(String content) {
        List<MindNode> nodes = new ArrayList<>();
        if (content == null || content.isEmpty()) {
            return new MindGraph(nodes, new ArrayList<>());
        }

        for (Map.Entry<String, Rule> entry : DICTIONARY.entrySet()) {
            String keyword = entry.getKey();
            int count = countOccurrences(content, keyword);
            if (count > 0) {
                double intensity = Math.min(count, INTENSITY_CAP);
                Rule rule = entry.getValue();
                nodes.add(new MindNode(keyword, rule.group, intensity, rule.sentiment));
            }
        }

        List<MindLink> links = buildLinks(nodes);
        return new MindGraph(nodes, links);
    }

    private List<MindLink> buildLinks(List<MindNode> nodes) {
        List<MindLink> links = new ArrayList<>();
        List<MindNode> emotions = new ArrayList<>();
        for (MindNode n : nodes) {
            if ("EMOTION".equals(n.getGroup())) {
                emotions.add(n);
            }
        }
        for (MindNode n : nodes) {
            String relation = relationFor(n.getGroup());
            if (relation == null) {
                continue;
            }
            for (MindNode emotion : emotions) {
                links.add(new MindLink(n.getId(), emotion.getId(), relation));
            }
        }
        return links;
    }

    private String relationFor(String group) {
        if ("STRESSOR".equals(group)) {
            return "TRIGGERS";
        }
        if ("COPING_MECHANISM".equals(group)) {
            return "MITIGATES";
        }
        if ("SUPPORTER".equals(group)) {
            return "ASSOCIATED_WITH";
        }
        return null;
    }

    private int countOccurrences(String content, String keyword) {
        int count = 0;
        int from = 0;
        while (true) {
            int idx = content.indexOf(keyword, from);
            if (idx < 0) {
                break;
            }
            count++;
            from = idx + keyword.length();
        }
        return count;
    }

    private static final class Rule {
        final String group;
        final String sentiment;

        Rule(String group, String sentiment) {
            this.group = group;
            this.sentiment = sentiment;
        }
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `mvn -q -Dtest=RuleBasedGraphExtractorTest test`
Expected: PASS (3 tests).

- [ ] **Step 5: 커밋**

```bash
git add -A
git commit -m "feat: deterministic rule-based mind graph extractor"
```

---

## Task 6: 그래프 영속화 도메인 (JPA) + Redis 캐시

**Files:**
- Create: `domain/GraphRecord.java`, `domain/GraphRecordRepository.java`, `cache/GraphCache.java`

> `GraphRecord`는 `diaryId`를 PK로 갖고 nodes/links를 JSON 텍스트로 저장한다(가장 단순한 결정론적 저장). JPA 어노테이션은 Boot 2.7이므로 **`javax.persistence.*`**(jakarta 아님)를 import 한다.

- [ ] **Step 1: `GraphRecord` 엔티티**

`src/main/java/com/tainted/mindgraph/domain/GraphRecord.java`:
```java
package com.tainted.mindgraph.domain;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.time.Instant;

@Entity
@Table(name = "graph_record")
public class GraphRecord {

    @Id
    @Column(name = "diary_id", length = 64)
    private String diaryId;

    @Column(name = "user_id", nullable = false, length = 64)
    private String userId;

    @Column(name = "nodes_json", nullable = false, columnDefinition = "text")
    private String nodesJson;

    @Column(name = "links_json", nullable = false, columnDefinition = "text")
    private String linksJson;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected GraphRecord() {
    }

    public GraphRecord(String diaryId, String userId, String nodesJson, String linksJson, Instant updatedAt) {
        this.diaryId = diaryId;
        this.userId = userId;
        this.nodesJson = nodesJson;
        this.linksJson = linksJson;
        this.updatedAt = updatedAt;
    }

    public String getDiaryId() { return diaryId; }
    public String getUserId() { return userId; }
    public String getNodesJson() { return nodesJson; }
    public String getLinksJson() { return linksJson; }
    public Instant getUpdatedAt() { return updatedAt; }
}
```

- [ ] **Step 2: 리포지토리**

`src/main/java/com/tainted/mindgraph/domain/GraphRecordRepository.java`:
```java
package com.tainted.mindgraph.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface GraphRecordRepository extends JpaRepository<GraphRecord, String> {
}
```

- [ ] **Step 3: Redis 캐시 (사용자별 최신 그래프, 논리 DB 1)**

`src/main/java/com/tainted/mindgraph/cache/GraphCache.java`:
```java
package com.tainted.mindgraph.cache;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.Optional;

/** 사용자별 최신 그래프 JSON 캐시. Redis 논리 DB 1(application.yml의 spring.redis.database). */
@Component
public class GraphCache {

    private static final String PREFIX = "mindgraph:latest:";

    private final StringRedisTemplate redis;

    public GraphCache(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /** userId 의 최신 그래프 JSON 을 저장(덮어쓰기). */
    public void putLatest(String userId, String graphJson) {
        redis.opsForValue().set(PREFIX + userId, graphJson);
    }

    /** userId 의 최신 그래프 JSON 조회. 없으면 empty. */
    public Optional<String> getLatest(String userId) {
        return Optional.ofNullable(redis.opsForValue().get(PREFIX + userId));
    }
}
```

- [ ] **Step 4: 컴파일 + 커밋**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.
```bash
git add -A
git commit -m "feat: GraphRecord entity, repository, and Redis graph cache"
```

---

## Task 7: 이벤트 POJO + Kafka consumer/producer + 서비스 + 컨트롤러

**Files:**
- Create: `event/DiaryCreatedEvent.java`, `event/GraphUpdatedEvent.java`, `event/GraphEventPublisher.java`, `event/DiaryCreatedConsumer.java`
- Create: `service/GraphService.java`
- Create: `web/dto/GraphResponse.java`, `web/GraphController.java`
- Create: `error/GraphNotFoundException.java`, `error/GlobalExceptionHandler.java`, `config/OpenApiConfig.java`

> 이벤트 POJO는 자기완결적이며 추적 헤더가 없다. `diary.created` 소비, `graph.updated` 발행. 모든 이벤트는 JSON 문자열로 송수신하고 `ObjectMapper`로 변환한다(스키마 레지스트리 없음, 자기 코드의 POJO로 독립 반영). 소비 POJO는 모르는 필드를 무시한다(`FAIL_ON_UNKNOWN_PROPERTIES=false` — 스키마 드리프트 허용).

- [ ] **Step 1: 이벤트 POJO 2종 (평범한 클래스)**

`src/main/java/com/tainted/mindgraph/event/DiaryCreatedEvent.java`:
```java
package com.tainted.mindgraph.event;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.time.Instant;

/** diary.created 소비 페이로드. 스키마 드리프트 허용 — 모르는 필드는 무시. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DiaryCreatedEvent {

    private String eventId;
    private String userId;
    private String diaryId;
    private String primaryEmotion;
    private Integer energyScore;
    private Instant occurredAt;

    public DiaryCreatedEvent() {
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getDiaryId() { return diaryId; }
    public void setDiaryId(String diaryId) { this.diaryId = diaryId; }

    public String getPrimaryEmotion() { return primaryEmotion; }
    public void setPrimaryEmotion(String primaryEmotion) { this.primaryEmotion = primaryEmotion; }

    public Integer getEnergyScore() { return energyScore; }
    public void setEnergyScore(Integer energyScore) { this.energyScore = energyScore; }

    public Instant getOccurredAt() { return occurredAt; }
    public void setOccurredAt(Instant occurredAt) { this.occurredAt = occurredAt; }
}
```

`src/main/java/com/tainted/mindgraph/event/GraphUpdatedEvent.java`:
```java
package com.tainted.mindgraph.event;

import java.time.Instant;

/** graph.updated 발행 페이로드. 자기완결적, 추적 헤더 없음. */
public class GraphUpdatedEvent {

    private String eventId;
    private String userId;
    private String diaryId;
    private int nodeCount;
    private Instant occurredAt;

    public GraphUpdatedEvent() {
    }

    public GraphUpdatedEvent(String eventId, String userId, String diaryId, int nodeCount, Instant occurredAt) {
        this.eventId = eventId;
        this.userId = userId;
        this.diaryId = diaryId;
        this.nodeCount = nodeCount;
        this.occurredAt = occurredAt;
    }

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getDiaryId() { return diaryId; }
    public void setDiaryId(String diaryId) { this.diaryId = diaryId; }

    public int getNodeCount() { return nodeCount; }
    public void setNodeCount(int nodeCount) { this.nodeCount = nodeCount; }

    public Instant getOccurredAt() { return occurredAt; }
    public void setOccurredAt(Instant occurredAt) { this.occurredAt = occurredAt; }
}
```

- [ ] **Step 2: `GraphEventPublisher` (KafkaTemplate)**

`src/main/java/com/tainted/mindgraph/event/GraphEventPublisher.java`:
```java
package com.tainted.mindgraph.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class GraphEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String graphUpdatedTopic;

    public GraphEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                               ObjectMapper objectMapper,
                               @Value("${mindgraph.topics.graph-updated}") String graphUpdatedTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.graphUpdatedTopic = graphUpdatedTopic;
    }

    public void publishGraphUpdated(GraphUpdatedEvent event) {
        try {
            String json = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(graphUpdatedTopic, event.getUserId(), json);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize graph.updated", e);
        }
    }
}
```

- [ ] **Step 3: `GraphService` (추출 → 저장 → 캐시 → 발행)**

`src/main/java/com/tainted/mindgraph/service/GraphService.java`:
```java
package com.tainted.mindgraph.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tainted.mindgraph.cache.GraphCache;
import com.tainted.mindgraph.client.DiaryContent;
import com.tainted.mindgraph.client.DiaryContentClient;
import com.tainted.mindgraph.domain.GraphRecord;
import com.tainted.mindgraph.domain.GraphRecordRepository;
import com.tainted.mindgraph.error.GraphNotFoundException;
import com.tainted.mindgraph.event.DiaryCreatedEvent;
import com.tainted.mindgraph.event.GraphEventPublisher;
import com.tainted.mindgraph.event.GraphUpdatedEvent;
import com.tainted.mindgraph.graph.MindGraph;
import com.tainted.mindgraph.graph.RuleBasedGraphExtractor;
import com.tainted.mindgraph.id.IdGenerator;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;

@Service
public class GraphService {

    private final DiaryContentClient diaryContentClient;
    private final RuleBasedGraphExtractor extractor;
    private final GraphRecordRepository repository;
    private final GraphCache cache;
    private final GraphEventPublisher publisher;
    private final ObjectMapper objectMapper;
    private final IdGenerator idGenerator;
    private final Clock clock;

    public GraphService(DiaryContentClient diaryContentClient,
                        RuleBasedGraphExtractor extractor,
                        GraphRecordRepository repository,
                        GraphCache cache,
                        GraphEventPublisher publisher,
                        ObjectMapper objectMapper,
                        IdGenerator idGenerator,
                        Clock clock) {
        this.diaryContentClient = diaryContentClient;
        this.extractor = extractor;
        this.repository = repository;
        this.cache = cache;
        this.publisher = publisher;
        this.objectMapper = objectMapper;
        this.idGenerator = idGenerator;
        this.clock = clock;
    }

    /** diary.created 처리: 본문 조회 → 추출 → 저장 → 캐시 → graph.updated 발행. */
    @Transactional
    public void handleDiaryCreated(DiaryCreatedEvent event) {
        DiaryContent content = diaryContentClient.getContent(event.getDiaryId());
        String text = content.getContent() == null ? "" : content.getContent();

        MindGraph graph = extractor.extract(text);
        Instant now = Instant.now(clock);

        String nodesJson = writeJson(graph.getNodes());
        String linksJson = writeJson(graph.getLinks());

        repository.save(new GraphRecord(
                event.getDiaryId(), event.getUserId(), nodesJson, linksJson, now));

        cache.putLatest(event.getUserId(), writeJson(graph));

        publisher.publishGraphUpdated(new GraphUpdatedEvent(
                idGenerator.newId(),
                event.getUserId(),
                event.getDiaryId(),
                graph.getNodes().size(),
                now));
    }

    /** diaryId 의 저장된 그래프를 MindGraph 로 복원. */
    @Transactional(readOnly = true)
    public MindGraph getGraphByDiaryId(String diaryId) {
        GraphRecord record = repository.findById(diaryId)
                .orElseThrow(() -> new GraphNotFoundException("graph not found for diary: " + diaryId));
        return toMindGraph(record);
    }

    /** userId 의 최신 캐시 그래프를 MindGraph 로 복원. */
    @Transactional(readOnly = true)
    public MindGraph getLatestGraphByUserId(String userId) {
        String json = cache.getLatest(userId)
                .orElseThrow(() -> new GraphNotFoundException("no cached graph for user: " + userId));
        try {
            return objectMapper.readValue(json, MindGraph.class);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("corrupt cached graph for user: " + userId, e);
        }
    }

    private MindGraph toMindGraph(GraphRecord record) {
        try {
            MindGraph graph = new MindGraph();
            graph.setNodes(objectMapper.readValue(record.getNodesJson(),
                    objectMapper.getTypeFactory().constructCollectionType(
                            java.util.List.class, com.tainted.mindgraph.graph.MindNode.class)));
            graph.setLinks(objectMapper.readValue(record.getLinksJson(),
                    objectMapper.getTypeFactory().constructCollectionType(
                            java.util.List.class, com.tainted.mindgraph.graph.MindLink.class)));
            return graph;
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("corrupt stored graph for diary: " + record.getDiaryId(), e);
        }
    }

    private String writeJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize graph", e);
        }
    }
}
```

- [ ] **Step 4: `DiaryCreatedConsumer` (`@KafkaListener`)**

`src/main/java/com/tainted/mindgraph/event/DiaryCreatedConsumer.java`:
```java
package com.tainted.mindgraph.event;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tainted.mindgraph.service.GraphService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class DiaryCreatedConsumer {

    private final GraphService graphService;
    private final ObjectMapper objectMapper;

    public DiaryCreatedConsumer(GraphService graphService, ObjectMapper objectMapper) {
        this.graphService = graphService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${mindgraph.topics.diary-created}", groupId = "${spring.kafka.consumer.group-id}")
    public void onMessage(String message) throws Exception {
        DiaryCreatedEvent event = objectMapper.readValue(message, DiaryCreatedEvent.class);
        graphService.handleDiaryCreated(event);
    }
}
```

- [ ] **Step 5: `GraphResponse` DTO + `GraphController`**

`src/main/java/com/tainted/mindgraph/web/dto/GraphResponse.java`:
```java
package com.tainted.mindgraph.web.dto;

import com.tainted.mindgraph.graph.MindLink;
import com.tainted.mindgraph.graph.MindNode;

import java.util.List;

/** 내부 그래프 조회 응답. Java 11 — record 금지, 평범한 클래스. */
public class GraphResponse {

    private List<MindNode> nodes;
    private List<MindLink> links;

    public GraphResponse() {
    }

    public GraphResponse(List<MindNode> nodes, List<MindLink> links) {
        this.nodes = nodes;
        this.links = links;
    }

    public List<MindNode> getNodes() { return nodes; }
    public void setNodes(List<MindNode> nodes) { this.nodes = nodes; }

    public List<MindLink> getLinks() { return links; }
    public void setLinks(List<MindLink> links) { this.links = links; }
}
```

`src/main/java/com/tainted/mindgraph/web/GraphController.java`:
```java
package com.tainted.mindgraph.web;

import com.tainted.mindgraph.graph.MindGraph;
import com.tainted.mindgraph.service.GraphService;
import com.tainted.mindgraph.web.dto.GraphResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/internal/graphs")
public class GraphController {

    private final GraphService graphService;

    public GraphController(GraphService graphService) {
        this.graphService = graphService;
    }

    @GetMapping("/user/{userId}")
    public GraphResponse byUser(@PathVariable("userId") String userId) {
        MindGraph graph = graphService.getLatestGraphByUserId(userId);
        return new GraphResponse(graph.getNodes(), graph.getLinks());
    }

    @GetMapping("/diary/{diaryId}")
    public GraphResponse byDiary(@PathVariable("diaryId") String diaryId) {
        MindGraph graph = graphService.getGraphByDiaryId(diaryId);
        return new GraphResponse(graph.getNodes(), graph.getLinks());
    }
}
```

- [ ] **Step 6: 예외 + RFC 7807 핸들러 (Boot 2.7 — `ProblemDetail` 없음, `Map` 반환)**

`src/main/java/com/tainted/mindgraph/error/GraphNotFoundException.java`:
```java
package com.tainted.mindgraph.error;

public class GraphNotFoundException extends RuntimeException {
    public GraphNotFoundException(String message) {
        super(message);
    }
}
```

> Boot 2.7에는 `ProblemDetail` 타입이 없으므로 `@RestControllerAdvice`에서 `ResponseEntity<Map<String,Object>>`를 직접 만들어 `Content-Type: application/problem+json`과 RFC 7807 필드(type/title/status/detail)를 채운다.

`src/main/java/com/tainted/mindgraph/error/GlobalExceptionHandler.java`:
```java
package com.tainted.mindgraph.error;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final MediaType PROBLEM_JSON = MediaType.valueOf("application/problem+json");

    @ExceptionHandler(GraphNotFoundException.class)
    public ResponseEntity<Map<String, Object>> handleNotFound(GraphNotFoundException ex) {
        return problem(HttpStatus.NOT_FOUND, "Graph not found", ex.getMessage());
    }

    private ResponseEntity<Map<String, Object>> problem(HttpStatus status, String title, String detail) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("type", "about:blank");
        body.put("title", title);
        body.put("status", status.value());
        body.put("detail", detail);
        return ResponseEntity.status(status).contentType(PROBLEM_JSON).body(body);
    }
}
```

- [ ] **Step 7: OpenAPI 설정**

`src/main/java/com/tainted/mindgraph/config/OpenApiConfig.java`:
```java
package com.tainted.mindgraph.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {
    @Bean
    public OpenAPI mindgraphOpenApi() {
        return new OpenAPI().info(new Info()
                .title("mindgraph-service API")
                .version("0.1.0")
                .description("일기 → 규칙기반 마음 그래프 변환, 그래프 조회"));
    }
}
```

> springdoc 1.x의 Swagger UI는 `/swagger-ui.html`에서 제공된다.

- [ ] **Step 8: 컴파일 + 커밋**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.
```bash
git add -A
git commit -m "feat: kafka consume/produce, graph service, controller, RFC7807, OpenAPI"
```

---

## Task 8: 통합 테스트 (Testcontainers Postgres + Kafka + Redis, RestAssured)

**Files:**
- Test: `src/test/java/com/tainted/mindgraph/MindgraphIntegrationTest.java`

> 핵심 검증: `diary.created` JSON 을 토픽에 발행 → 소비 대기(`GET /internal/graphs/diary/{diaryId}` 폴링) → 그래프 영속화 확인 → `graph.updated` 발행 확인(원시 KafkaConsumer). diary-service는 `@TestConfiguration`의 스텁 `DiaryContentClient` 빈으로 대체(고정 한국어 본문 반환)하므로 실제로 띄울 필요 없다. Redis는 `GenericContainer(redis:7-alpine)`. `GenericContainer`는 `junit-jupiter`가 코어를 전이 의존으로 끌어오므로 별도 의존 추가 불필요.

- [ ] **Step 1: 통합 테스트 작성**

`src/test/java/com/tainted/mindgraph/MindgraphIntegrationTest.java`:
```java
package com.tainted.mindgraph;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.tainted.mindgraph.client.DiaryContent;
import com.tainted.mindgraph.client.DiaryContentClient;
import io.restassured.RestAssured;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class MindgraphIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16-alpine").withDatabaseName("mindgraph");

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Container
    static KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", postgres::getJdbcUrl);
        r.add("spring.datasource.username", postgres::getUsername);
        r.add("spring.datasource.password", postgres::getPassword);
        r.add("spring.redis.host", redis::getHost);
        r.add("spring.redis.port", () -> redis.getMappedPort(6379));
        r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    /** diary-service 를 띄우지 않도록 Feign 클라이언트를 고정 본문 스텁으로 대체. */
    @TestConfiguration
    static class StubDiaryClientConfig {
        @Bean
        @Primary
        DiaryContentClient stubDiaryContentClient() {
            return id -> new DiaryContent("제목",
                    "오늘 직장 상사 때문에 불안했지만 저녁에 운동을 하고 나니 평온해졌다.");
        }
    }

    @LocalServerPort
    int port;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        RestAssured.port = port;
    }

    @Test
    void diaryCreatedProducesPersistedGraphAndGraphUpdatedEvent() throws Exception {
        String userId = "user-1";
        String diaryId = "diary-" + UUID.randomUUID();

        // graph.updated 를 미리 구독.
        KafkaConsumer<String, String> consumer = newConsumer();
        consumer.subscribe(Collections.singletonList("graph.updated"));

        // diary.created 발행.
        String event = "{"
                + "\"eventId\":\"" + UUID.randomUUID() + "\","
                + "\"userId\":\"" + userId + "\","
                + "\"diaryId\":\"" + diaryId + "\","
                + "\"primaryEmotion\":\"anxiety\","
                + "\"energyScore\":3,"
                + "\"occurredAt\":\"2026-06-13T00:00:00Z\"}";
        try (KafkaProducer<String, String> producer = newProducer()) {
            producer.send(new ProducerRecord<>("diary.created", userId, event)).get();
        }

        // 소비/저장 완료까지 폴링(최대 ~30초).
        awaitGraphPersisted(diaryId);

        // 그래프 검증: 상사(STRESSOR), 운동(COPING), 불안(EMOTION), 평온(EMOTION) → 4노드.
        given().when().get("/internal/graphs/diary/" + diaryId)
                .then().statusCode(200)
                .body("nodes.size()", org.hamcrest.Matchers.equalTo(4))
                .body("nodes[0].id", org.hamcrest.Matchers.equalTo("상사"))
                .body("nodes[0].group", org.hamcrest.Matchers.equalTo("STRESSOR"))
                .body("links.size()", org.hamcrest.Matchers.equalTo(4));

        // 사용자 최신 캐시도 동일 노드 수.
        given().when().get("/internal/graphs/user/" + userId)
                .then().statusCode(200)
                .body("nodes.size()", org.hamcrest.Matchers.equalTo(4));

        // graph.updated 메시지 수신 검증.
        JsonNode produced = pollFor(consumer, diaryId);
        assertNotNull(produced, "graph.updated 메시지를 수신해야 한다");
        assertEquals(userId, produced.get("userId").asText());
        assertEquals(4, produced.get("nodeCount").asInt());
        consumer.close();
    }

    @Test
    void unknownDiaryGraphReturnsProblemJson() {
        given().when().get("/internal/graphs/diary/does-not-exist")
                .then().statusCode(404)
                .contentType("application/problem+json")
                .body("title", org.hamcrest.Matchers.equalTo("Graph not found"));
    }

    private void awaitGraphPersisted(String diaryId) throws InterruptedException {
        for (int i = 0; i < 60; i++) {
            int status = given().when().get("/internal/graphs/diary/" + diaryId)
                    .then().extract().statusCode();
            if (status == 200) {
                return;
            }
            Thread.sleep(500);
        }
        fail("graph was not persisted within timeout for diary: " + diaryId);
    }

    private JsonNode pollFor(KafkaConsumer<String, String> consumer, String diaryId) throws Exception {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> rec : records) {
                JsonNode node = objectMapper.readTree(rec.value());
                if (node.has("diaryId") && diaryId.equals(node.get("diaryId").asText())) {
                    return node;
                }
            }
        }
        return null;
    }

    private KafkaConsumer<String, String> newConsumer() {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "it-verifier-" + UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(p);
    }

    private KafkaProducer<String, String> newProducer() {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new KafkaProducer<>(p);
    }
}
```

- [ ] **Step 2: 전체 테스트 실행 (Docker 필요)**

Run: `mvn -q test`
Expected: PASS — `RuleBasedGraphExtractorTest`(3) + `MindgraphIntegrationTest`(2) 모두 통과. (Docker 데몬이 떠 있어야 Testcontainers Postgres/Kafka/Redis 동작.)

- [ ] **Step 3: 커밋**

```bash
git add -A
git commit -m "test: Testcontainers (postgres+kafka+redis) + RestAssured integration tests"
```

---

## Task 9: Dockerfile + docker 프로파일

**Files:**
- Create: `Dockerfile`, `src/main/resources/application-docker.yml`

- [ ] **Step 1: docker 프로파일 설정 (compose 네트워크 호스트명)**

> `${POSTGRES_PASSWORD:postgrespw}`·`${SERVICES_DIARY_URL:http://diary:8082}` 등의 값은 Task 10에서 compose `mindgraph` 서비스의 `environment:`로 주입된다(미주입 시 기본값 사용).

`src/main/resources/application-docker.yml`:
```yaml
spring:
  datasource:
    url: jdbc:postgresql://postgres:5432/mindgraph
    username: ${POSTGRES_USER:postgres}
    password: ${POSTGRES_PASSWORD:postgrespw}
  redis:
    host: redis
    port: 6379
    database: 1
  kafka:
    bootstrap-servers: kafka:29092
services:
  diary:
    url: ${SERVICES_DIARY_URL:http://diary:8082}
```

- [ ] **Step 2: 멀티스테이지 Dockerfile (non-root)**

`Dockerfile`:
```dockerfile
# build
FROM maven:3.9-eclipse-temurin-11 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B -DskipTests package

# run
FROM eclipse-temurin:11-jre
WORKDIR /app
# curl: compose/k8s 헬스체크가 /actuator/health 를 호출하는 데 사용.
# temurin JRE 기본 이미지에는 curl/wget 이 없으므로 명시적으로 설치한다.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd -r -u 1001 appuser
COPY --from=build /app/target/mindgraph-service-0.1.0.jar app.jar
USER appuser
EXPOSE 8083
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 3: 이미지 빌드 확인**

Run: `docker build -t tainted-spring/mindgraph:0.1.0 .`
Expected: 빌드 성공, 최종 이미지 생성.

- [ ] **Step 4: 커밋**

```bash
git add -A
git commit -m "build: Dockerfile and docker profile"
```

---

## Task 10: platform 레포에 mindgraph 배선 (compose + k8s 스켈레톤)

> ⚠️ **작업 레포 전환**: 이 Task의 모든 파일 수정·커밋은 `tainted-spring-mindgraph` 가 아니라 **`tainted-spring-platform` 레포**에서 한다. 각 Step의 `cd` 경로를 확인할 것.

**Files (다른 레포 `tainted-spring-platform`):**
- Modify: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-platform/docker-compose.yml`
- Create: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-platform/k8s/mindgraph/deployment.yaml`

- [ ] **Step 1: compose에 mindgraph 서비스 추가**

`tainted-spring-platform/docker-compose.yml` 의 `services:` 아래에 추가 (앵커 `*java-service` 재사용):
```yaml
  mindgraph:
    <<: *java-service
    build:
      context: ../tainted-spring-mindgraph
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
      kafka:
        condition: service_healthy
    environment:
      JAVA_TOOL_OPTIONS: "${JAVA_TOOL_OPTIONS:-}"
      SPRING_PROFILES_ACTIVE: "docker"
      POSTGRES_USER: "${POSTGRES_USER:-postgres}"
      POSTGRES_PASSWORD: "${POSTGRES_PASSWORD:-postgrespw}"
      SERVICES_DIARY_URL: "http://diary:8082"
    ports:
      - "8083:8083"
    healthcheck:
      # 런타임 이미지에 설치한 curl 사용. -f: 비2xx 응답이면 실패 → actuator DOWN 시 unhealthy.
      test: ["CMD", "curl", "-fsS", "http://localhost:8083/actuator/health"]
      interval: 15s
      timeout: 5s
      retries: 20
      start_period: 40s
```

> 참고: `<<: *java-service` 가 `environment` 를 병합하지 않고 덮어쓰므로, 위에서 `JAVA_TOOL_OPTIONS`/`SPRING_PROFILES_ACTIVE` 를 명시적으로 다시 적었다. `SERVICES_DIARY_URL` 은 diary-service의 compose DNS(`http://diary:8082`)를 가리킨다.

- [ ] **Step 2: compose로 통합 기동 검증**

Run (platform 레포에서):
```bash
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-platform
docker compose up -d --build --wait mindgraph
curl -s http://localhost:8083/actuator/health
```
Expected: `--wait` 가 mindgraph 헬스체크 통과까지 블록. health `{"status":"UP"}`. (그래프 생성은 diary-service가 `diary.created` 를 발행해야 일어나므로 단독 기동에서는 health만 확인한다.)

- [ ] **Step 3: k8s Deployment/Service 스켈레톤**

`tainted-spring-platform/k8s/mindgraph/deployment.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tainted-spring-mindgraph
  namespace: tainted-spring
spec:
  replicas: 1
  selector:
    matchLabels: { app: mindgraph }
  template:
    metadata:
      labels: { app: mindgraph }
    spec:
      containers:
        - name: mindgraph
          image: tainted-spring/mindgraph:0.1.0
          ports: [{ containerPort: 8083 }]
          env:
            - { name: SPRING_PROFILES_ACTIVE, value: "eks" }
            - { name: JAVA_TOOL_OPTIONS, value: "" }
            - { name: SERVICES_DIARY_URL, value: "http://tainted-spring-diary" }
          readinessProbe:
            httpGet: { path: /actuator/health, port: 8083 }
            initialDelaySeconds: 20
            periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: tainted-spring-mindgraph
  namespace: tainted-spring
spec:
  selector: { app: mindgraph }
  ports:
    - { port: 80, targetPort: 8083 }
```

- [ ] **Step 4: platform 레포 커밋**

```bash
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-platform
git add -A
git commit -m "feat: wire mindgraph service into compose and k8s skeleton"
```

---

## Self-Review

**Spec coverage (설계문서 §3.3 / §4 대비):**
- `diary.created` 소비 → Task 7 `DiaryCreatedConsumer` ✔
- diary 본문 복호화 조회(OpenFeign `GET /internal/diaries/{id}/content`) → Task 3 `DiaryContentClient` ✔
- 규칙기반 결정론 그래프 추출 → Task 5 `RuleBasedGraphExtractor`(고정 LinkedHashMap 사전, contains 스캔, 결정론 링크 규칙) ✔
- 그래프 저장(Postgres `GraphRecord`, nodes/links JSON) → Task 6 ✔
- `graph.updated` 발행(`{eventId,userId,diaryId,nodeCount,occurredAt}`) → Task 7 `GraphEventPublisher` ✔
- 사용자별 최신 그래프 Redis(index 1) 캐시 → Task 6 `GraphCache` ✔
- 내부 API `GET /internal/graphs/user/{userId}` / `GET /internal/graphs/diary/{diaryId}` → Task 7 `GraphController` ✔
- OpenAPI 노출(springdoc 1.x) → Task 7 `OpenApiConfig`, `/swagger-ui.html` ✔
- RFC 7807 problem+json(Boot 2.7 — `ProblemDetail` 미사용, `Map` 반환) → Task 7 `GlobalExceptionHandler` + Task 8 검증 ✔
- `/actuator/health` 유지, 추적 라이브러리 없음 → pom.xml에 actuator만, OTel/Sleuth/correlation id 미포함 ✔
- 결정론(Clock/IdGenerator) → Task 2 ✔
- Docker(멀티스테이지/non-root/curl 설치) + docker 프로파일 DNS → Task 9 ✔
- compose 배선(빈 JAVA_TOOL_OPTIONS 슬롯, SERVICES_DIARY_URL) + k8s 스켈레톤 → Task 10 ✔

**Java 11 / Boot 2.7 제약 점검:**
- record 미사용 — `DiaryContent`/`MindNode`/`MindLink`/`MindGraph`/`GraphResponse`/이벤트 POJO 모두 평범한 클래스(생성자 + getter/setter) ✔
- JPA import `javax.persistence.*`(jakarta 아님) → Task 6 `GraphRecord` ✔
- `ProblemDetail` 미사용, `ResponseEntity<Map<String,Object>>` + `application/problem+json` → Task 7 ✔
- springdoc 1.x(`springdoc-openapi-ui:1.7.0`, `/swagger-ui.html`) — 2.x 아님 ✔
- Redis 프로퍼티 `spring.redis.*`(Boot 2.7 경로, `spring.data.redis.*` 아님) ✔
- OpenFeign: `spring-cloud-starter-openfeign` + `spring-cloud-dependencies:2021.0.8` BOM + `@EnableFeignClients` ✔
- text block / switch expression / pattern matching 미사용 ✔

**Placeholder scan:** 모든 코드 step에 완전한 소스, 모든 명령에 기대출력 명시. 플레이스홀더 없음. ✔

**Type/이름 일관성:**
- `IdGenerator.newId()` — Task 2 정의, Task 7 `GraphService` 사용 일치 ✔
- `RuleBasedGraphExtractor.extract(String): MindGraph` — Task 5 정의, Task 7 사용 일치 ✔
- `DiaryContentClient.getContent(String): DiaryContent` — Task 3 정의, Task 7·8 사용 일치 ✔
- `GraphCache.putLatest/getLatest` — Task 6 정의, Task 7 사용 일치 ✔
- `GraphRecord(diaryId,userId,nodesJson,linksJson,updatedAt)` — Task 6 정의, Task 7 save 인자 일치 ✔
- 이벤트 필드(`eventId,userId,diaryId,nodeCount,occurredAt` / `eventId,userId,diaryId,primaryEmotion,energyScore,occurredAt`) — §4 카탈로그와 일치, Task 8 검증 키와 일치 ✔
- jar 파일명 `mindgraph-service-0.1.0.jar` — pom `artifactId`+`version` 과 Dockerfile COPY 일치 ✔
- 포트 8083 — application.yml / Dockerfile EXPOSE / compose / k8s 전부 일치 ✔
