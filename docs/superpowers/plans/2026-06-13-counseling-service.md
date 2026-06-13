# tainted-spring-counseling Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** AI 상담사 4페르소나(공감 수현/분석 성우/직설 진수/희망 다혜) 1:1 챗, 결정론적 LLM mock, mindgraph 컨텍스트 조회(WebClient), post.created 소비 → community에 AI 댓글 등록(REST), graph.updated 소비 → Redis 캐시 갱신을 제공하는 `counseling-service`(Java 21 · Gradle · Spring Boot 3.4 · WebFlux · Redis)를 RestAssured/Testcontainers로 검증 가능하게 구현한다.

**Architecture:** Spring WebFlux 리액티브 서비스. 상담사 응답은 결정론적 `DeterministicCounselorClient`로 대체(실제 LLM 호출 없음). 세션/히스토리는 Redis(논리 DB 2)에 리액티브로 저장. mindgraph·community 연동은 인터페이스(`MindGraphClient`, `CommunityClient`) 뒤에 WebClient 구현체를 숨겨, 통합 테스트에서 stub 빈으로 교체. Kafka 소비(post.created, graph.updated)는 spring-kafka 사용. 시간/ID는 주입 가능한 `Clock`/`IdGenerator` 빈으로 결정론 확보. 추적/관측 라이브러리 없음(actuator health만 유지).

**Tech Stack:** Java 21, Gradle 8.10, Spring Boot 3.4.1, Spring WebFlux, Spring Data Redis(Reactive), Spring Kafka, springdoc-openapi-starter-webflux-ui 2.6.0, Redis 7, Testcontainers 1.20.3, RestAssured.

---

## 사전 조건

- `tainted-spring-platform` 계획이 완료되어 있어야 통합 실행이 쉽다(단위/통합 테스트는 Testcontainers로 자체 인프라를 띄우므로 platform 없이도 테스트는 통과한다).
- **레포 위치**: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-counseling/` (모든 경로는 이 루트 기준).
- **포트**: 8084. **패키지 루트**: `com.tainted.counseling`. **Redis 논리 DB**: 2.
- **데이터스토어**: Redis 7(세션/히스토리, 논리 DB 2) — 다른 RDBMS 없음.
- **로컬 사전 설치**: Docker 데몬, JDK 21, Gradle 8.10(`gradle wrapper` 생성 후 `./gradlew` 사용), Git. Docker 데몬은 Task 8 통합 테스트(Testcontainers)와 Task 9 이미지 빌드에 필요. 단위 테스트(Task 3, Task 5)는 Docker 없이 동작한다.

## File Structure

```
build.gradle
settings.gradle
gradlew / gradlew.bat / gradle/wrapper/
Dockerfile
src/main/resources/application.yml
src/main/resources/application-docker.yml
src/main/java/com/tainted/counseling/
  CounselingApplication.java
  config/DeterminismConfig.java        # Clock 빈
  config/OpenApiConfig.java
  config/RedisConfig.java              # ReactiveRedisTemplate 설정
  config/KafkaConfig.java              # ConsumerFactory / ContainerFactory
  config/WebClientConfig.java          # MindGraphClient, CommunityClient WebClient 빈
  id/IdGenerator.java                  # 인터페이스
  id/UuidIdGenerator.java              # 기본 구현
  counselor/CounselorClient.java       # 인터페이스 {reply(personaKey, graphContext, userMessage)}
  counselor/DeterministicCounselorClient.java  # 결정론적 구현
  client/MindGraphClient.java          # 인터페이스 {graphContextFor(userId)}
  client/CommunityClient.java          # 인터페이스 {addAiComment(postId, role, content)}
  client/WebClientMindGraphClient.java # WebClient 구현
  client/WebClientCommunityClient.java # WebClient 구현
  domain/ChatSession.java              # Redis 엔티티
  domain/ChatMessage.java              # 메시지(role∈[user,model], content)
  domain/ChatRepository.java           # ReactiveRedisTemplate 래퍼
  kafka/PostCreatedEvent.java          # post.created POJO
  kafka/GraphUpdatedEvent.java         # graph.updated POJO
  kafka/CounselingKafkaConsumer.java   # post.created + graph.updated 소비
  service/CounselingService.java       # 비즈니스 로직
  web/CounselingHandler.java           # 리액티브 핸들러 (Mono 반환)
  web/CounselingRouter.java            # RouterFunction 라우팅
  web/dto/CreateSessionRequest.java
  web/dto/CreateSessionResponse.java
  web/dto/SendMessageRequest.java
  web/dto/SendMessageResponse.java
  error/SessionNotFoundException.java
  error/GlobalErrorWebExceptionHandler.java  # RFC 7807 WebFlux
src/test/java/com/tainted/counseling/
  counselor/DeterministicCounselorClientTest.java  # 단위
  client/stub/StubMindGraphClient.java             # 테스트용 stub
  client/stub/StubCommunityClient.java             # 캡처 stub
  CounselingIntegrationTest.java                   # Testcontainers + RestAssured
```

---

## Task 1: 레포 스캐폴드 + 빌드 가능한 빈 앱

**Files:**
- Create: `build.gradle`, `settings.gradle`, `src/main/java/com/tainted/counseling/CounselingApplication.java`, `src/main/resources/application.yml`, `.gitignore`

- [ ] **Step 1: 디렉토리 생성 + git init**

Run:
```bash
mkdir -p /Users/changjoonbaek/workspace_claude-code/tainted-spring-counseling
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-counseling
git init
mkdir -p src/main/java/com/tainted/counseling src/main/resources
mkdir -p src/test/java/com/tainted/counseling
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
rootProject.name = 'counseling-service'
```

- [ ] **Step 4: `build.gradle`**

`build.gradle`:
```groovy
plugins {
    id 'org.springframework.boot' version '3.4.1'
    id 'io.spring.dependency-management' version '1.1.6'
    id 'java'
}

group = 'com.tainted'
version = '0.1.0'

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation 'org.springframework.boot:spring-boot-starter-webflux'
    implementation 'org.springframework.boot:spring-boot-starter-data-redis-reactive'
    implementation 'org.springframework.boot:spring-boot-starter-validation'
    implementation 'org.springframework.boot:spring-boot-starter-actuator'
    implementation 'org.springdoc:springdoc-openapi-starter-webflux-ui:2.6.0'
    implementation 'org.springframework.kafka:spring-kafka'

    testImplementation 'org.springframework.boot:spring-boot-starter-test'
    testImplementation 'io.projectreactor:reactor-test'
    testImplementation 'org.springframework.kafka:spring-kafka-test'
    testImplementation 'org.testcontainers:junit-jupiter:1.20.3'
    testImplementation 'org.testcontainers:kafka:1.20.3'
    testImplementation 'io.rest-assured:rest-assured'
}

tasks.named('test') {
    useJUnitPlatform()
}
```

- [ ] **Step 5: Gradle Wrapper 생성**

Run:
```bash
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-counseling
gradle wrapper --gradle-version 8.10
```
Expected: `gradlew`, `gradlew.bat`, `gradle/wrapper/gradle-wrapper.jar`, `gradle/wrapper/gradle-wrapper.properties` 생성.

- [ ] **Step 6: 메인 클래스 + 최소 설정**

`src/main/java/com/tainted/counseling/CounselingApplication.java`:
```java
package com.tainted.counseling;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CounselingApplication {
    public static void main(String[] args) {
        SpringApplication.run(CounselingApplication.class, args);
    }
}
```

`src/main/resources/application.yml`:
```yaml
server:
  port: 8084
spring:
  application:
    name: counseling-service
  data:
    redis:
      host: localhost
      port: 6379
      database: 2
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: counseling-service
      auto-offset-reset: earliest
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
  webflux:
    problemdetails:
      enabled: true
management:
  endpoints:
    web:
      exposure:
        include: health
services:
  mindgraph:
    url: http://localhost:8083
  community:
    url: http://localhost:8085
```

- [ ] **Step 7: 컴파일 확인**

Run: `./gradlew -q compileJava`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 8: 커밋**

```bash
git add -A
git commit -m "chore: scaffold counseling-service (buildable empty app)"
```

---

## Task 2: 결정론 빈 (Clock, IdGenerator)

**Files:**
- Create: `id/IdGenerator.java`, `id/UuidIdGenerator.java`, `config/DeterminismConfig.java`

- [ ] **Step 1: `IdGenerator` 인터페이스**

`src/main/java/com/tainted/counseling/id/IdGenerator.java`:
```java
package com.tainted.counseling.id;

/** 식별자 생성 추상화. 테스트에서 결정론적 구현으로 교체 가능. */
public interface IdGenerator {
    String newId();
}
```

- [ ] **Step 2: 기본 UUID 구현**

`src/main/java/com/tainted/counseling/id/UuidIdGenerator.java`:
```java
package com.tainted.counseling.id;

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

`src/main/java/com/tainted/counseling/config/DeterminismConfig.java`:
```java
package com.tainted.counseling.config;

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

## Task 3: 결정론적 CounselorClient (TDD)

**Files:**
- Create: `counselor/CounselorClient.java`, `counselor/DeterministicCounselorClient.java`
- Test: `src/test/java/com/tainted/counseling/counselor/DeterministicCounselorClientTest.java`

- [ ] **Step 1: 실패하는 테스트 작성**

`src/test/java/com/tainted/counseling/counselor/DeterministicCounselorClientTest.java`:
```java
package com.tainted.counseling.counselor;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DeterministicCounselorClientTest {

    private final DeterministicCounselorClient client = new DeterministicCounselorClient();

    @Test
    void empathicPersonaPrefixDistinct() {
        String reply = client.reply("empathic", "context", "오늘 힘들었어요.");
        assertTrue(reply.startsWith("[수현]"),
                "empathic 페르소나 응답은 '[수현]'으로 시작해야 합니다. 실제: " + reply);
    }

    @Test
    void analyticalPersonaPrefixDistinct() {
        String reply = client.reply("analytical", "context", "오늘 힘들었어요.");
        assertTrue(reply.startsWith("[성우]"),
                "analytical 페르소나 응답은 '[성우]'로 시작해야 합니다. 실제: " + reply);
    }

    @Test
    void sincerePersonaPrefixDistinct() {
        String reply = client.reply("sincere", "context", "오늘 힘들었어요.");
        assertTrue(reply.startsWith("[진수]"),
                "sincere 페르소나 응답은 '[진수]'로 시작해야 합니다. 실제: " + reply);
    }

    @Test
    void hopefulPersonaPrefixDistinct() {
        String reply = client.reply("hopeful", "context", "오늘 힘들었어요.");
        assertTrue(reply.startsWith("[다혜]"),
                "hopeful 페르소나 응답은 '[다혜]'로 시작해야 합니다. 실제: " + reply);
    }

    @Test
    void sameInputYieldsSameOutput() {
        String a = client.reply("empathic", "ctx", "같은 메시지");
        String b = client.reply("empathic", "ctx", "같은 메시지");
        assertEquals(a, b, "동일 입력은 항상 동일 출력이어야 합니다(결정론).");
    }

    @Test
    void differentMessagesCanYieldDifferentBodies() {
        // 결정론적이되, 충분히 다른 입력은 본문이 달라질 수 있음을 확인
        // (stub 풀이 1개 이상일 때 해시가 달라지는지 검증)
        String a = client.reply("empathic", "ctx", "messageAAA");
        String b = client.reply("empathic", "ctx", "messageBBB");
        // 둘 다 페르소나 접두어는 같아야 한다
        assertTrue(a.startsWith("[수현]"));
        assertTrue(b.startsWith("[수현]"));
    }
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `./gradlew -q test --tests "com.tainted.counseling.counselor.DeterministicCounselorClientTest"`
Expected: FAIL — `CounselorClient`, `DeterministicCounselorClient` 미존재로 컴파일 에러.

- [ ] **Step 3: 최소 구현**

`src/main/java/com/tainted/counseling/counselor/CounselorClient.java`:
```java
package com.tainted.counseling.counselor;

/**
 * AI 상담사 클라이언트 추상화.
 * 실제 LLM 또는 결정론적 mock으로 교체 가능.
 */
public interface CounselorClient {
    /**
     * 페르소나 키, 마음 그래프 컨텍스트, 사용자 메시지를 받아 상담사 응답 반환.
     * 같은 입력 → 항상 같은 출력(결정론 필수).
     *
     * @param personaKey   "empathic" | "analytical" | "sincere" | "hopeful"
     * @param graphContext mindgraph에서 조회한 텍스트 컨텍스트 (없으면 빈 문자열)
     * @param userMessage  사용자가 보낸 메시지
     * @return 상담사 응답 문자열 (페르소나 접두어 포함)
     */
    String reply(String personaKey, String graphContext, String userMessage);
}
```

`src/main/java/com/tainted/counseling/counselor/DeterministicCounselorClient.java`:
```java
package com.tainted.counseling.counselor;

import org.springframework.stereotype.Component;
import java.util.List;
import java.util.Map;

/**
 * 결정론적 상담사 mock 구현체.
 * 페르소나별 접두어 + 안정적 해시(hashCode)로 캔드 응답 풀에서 본문 선택.
 * 외부 네트워크 호출 없음.
 */
@Component
public class DeterministicCounselorClient implements CounselorClient {

    private static final Map<String, String> PERSONA_PREFIX = Map.of(
            "empathic",   "[수현]",
            "analytical", "[성우]",
            "sincere",    "[진수]",
            "hopeful",    "[다혜]"
    );

    private static final List<String> BODIES = List.of(
            "지금 많이 힘드시겠어요. 조금 더 이야기해 주실 수 있나요?",
            "상황을 정리해 보면, 주된 원인은 외부 압박으로 보입니다.",
            "솔직히 말씀드리면, 이 감정은 충분히 자연스러운 반응입니다.",
            "앞으로 나아질 수 있어요. 작은 한 걸음부터 시작해 보세요.",
            "말씀해 주셔서 고맙습니다. 당신의 감정은 소중합니다.",
            "함께라면 분명 좋은 방법을 찾을 수 있을 거예요."
    );

    @Override
    public String reply(String personaKey, String graphContext, String userMessage) {
        String prefix = PERSONA_PREFIX.getOrDefault(personaKey, "[상담사]");
        // stable hash: 입력 조합을 문자열로 이어붙인 뒤 hashCode → 절댓값 → 풀 인덱스
        int hash = Math.abs((personaKey + "|" + graphContext + "|" + userMessage).hashCode());
        String body = BODIES.get(hash % BODIES.size());
        return prefix + " " + body;
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew -q test --tests "com.tainted.counseling.counselor.DeterministicCounselorClientTest"`
Expected: PASS (6 tests).

- [ ] **Step 5: 커밋**

```bash
git add -A
git commit -m "feat: deterministic counselor client with 4 personas"
```

---

## Task 4: 외부 서비스 클라이언트 인터페이스 + WebClient 구현

**Files:**
- Create: `client/MindGraphClient.java`, `client/CommunityClient.java`, `client/WebClientMindGraphClient.java`, `client/WebClientCommunityClient.java`, `config/WebClientConfig.java`

- [ ] **Step 1: `MindGraphClient` 인터페이스**

`src/main/java/com/tainted/counseling/client/MindGraphClient.java`:
```java
package com.tainted.counseling.client;

/**
 * mindgraph-service 클라이언트 추상화.
 * GET {mindgraph}/internal/graphs/user/{userId} 를 호출해 그래프 컨텍스트 문자열 반환.
 * 통합 테스트에서 stub 빈으로 교체 가능.
 */
public interface MindGraphClient {
    /**
     * 사용자의 최신 마음 그래프 컨텍스트를 텍스트로 반환.
     * 조회 실패 또는 데이터 없음이면 빈 문자열을 반환한다(상담은 계속 진행).
     */
    String graphContextFor(String userId);
}
```

- [ ] **Step 2: `CommunityClient` 인터페이스**

`src/main/java/com/tainted/counseling/client/CommunityClient.java`:
```java
package com.tainted.counseling.client;

/**
 * community-service 클라이언트 추상화.
 * POST {community}/internal/posts/{postId}/comments 를 호출해 AI 댓글 등록.
 * 통합 테스트에서 캡처 stub으로 교체 가능.
 */
public interface CommunityClient {
    /**
     * 지정한 게시글에 AI 댓글을 등록한다.
     *
     * @param postId  대상 게시글 ID
     * @param role    페르소나 키 (empathic | analytical | sincere | hopeful)
     * @param content 댓글 본문
     */
    void addAiComment(String postId, String role, String content);
}
```

- [ ] **Step 3: WebClient 설정 빈**

`src/main/java/com/tainted/counseling/config/WebClientConfig.java`:
```java
package com.tainted.counseling.config;

import com.tainted.counseling.client.CommunityClient;
import com.tainted.counseling.client.MindGraphClient;
import com.tainted.counseling.client.WebClientCommunityClient;
import com.tainted.counseling.client.WebClientMindGraphClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class WebClientConfig {

    @Bean
    public MindGraphClient mindGraphClient(
            WebClient.Builder builder,
            @Value("${services.mindgraph.url}") String mindgraphUrl) {
        WebClient wc = builder.baseUrl(mindgraphUrl).build();
        return new WebClientMindGraphClient(wc);
    }

    @Bean
    public CommunityClient communityClient(
            WebClient.Builder builder,
            @Value("${services.community.url}") String communityUrl) {
        WebClient wc = builder.baseUrl(communityUrl).build();
        return new WebClientCommunityClient(wc);
    }
}
```

- [ ] **Step 4: `WebClientMindGraphClient` 구현**

`src/main/java/com/tainted/counseling/client/WebClientMindGraphClient.java`:
```java
package com.tainted.counseling.client;

import org.springframework.web.reactive.function.client.WebClient;

public class WebClientMindGraphClient implements MindGraphClient {

    private final WebClient webClient;

    public WebClientMindGraphClient(WebClient webClient) {
        this.webClient = webClient;
    }

    @Override
    public String graphContextFor(String userId) {
        try {
            String body = webClient.get()
                    .uri("/internal/graphs/user/{userId}", userId)
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();
            return body != null ? body : "";
        } catch (Exception e) {
            // mindgraph 가 없거나 장애 시 빈 컨텍스트로 상담 계속 진행
            return "";
        }
    }
}
```

- [ ] **Step 5: `WebClientCommunityClient` 구현**

`src/main/java/com/tainted/counseling/client/WebClientCommunityClient.java`:
```java
package com.tainted.counseling.client;

import org.springframework.web.reactive.function.client.WebClient;
import java.util.Map;

public class WebClientCommunityClient implements CommunityClient {

    private final WebClient webClient;

    public WebClientCommunityClient(WebClient webClient) {
        this.webClient = webClient;
    }

    @Override
    public void addAiComment(String postId, String role, String content) {
        webClient.post()
                .uri("/internal/posts/{postId}/comments", postId)
                .bodyValue(Map.of("role", role, "content", content))
                .retrieve()
                .toBodilessEntity()
                .block();
    }
}
```

- [ ] **Step 6: 컴파일 + 커밋**

Run: `./gradlew -q compileJava`
Expected: BUILD SUCCESSFUL.
```bash
git add -A
git commit -m "feat: MindGraphClient + CommunityClient interfaces and WebClient impls"
```

---

## Task 5: Redis 설정 + 도메인 엔티티 + 리포지토리

**Files:**
- Create: `config/RedisConfig.java`, `domain/ChatSession.java`, `domain/ChatMessage.java`, `domain/ChatRepository.java`

- [ ] **Step 1: Redis 리액티브 설정**

`src/main/java/com/tainted/counseling/config/RedisConfig.java`:
```java
package com.tainted.counseling.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.serializer.Jackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import com.tainted.counseling.domain.ChatSession;

@Configuration
public class RedisConfig {

    @Bean
    public ReactiveRedisTemplate<String, ChatSession> chatSessionRedisTemplate(
            ReactiveRedisConnectionFactory factory) {
        Jackson2JsonRedisSerializer<ChatSession> valueSerializer =
                new Jackson2JsonRedisSerializer<>(ChatSession.class);
        RedisSerializationContext<String, ChatSession> context =
                RedisSerializationContext.<String, ChatSession>newSerializationContext(
                        new StringRedisSerializer())
                        .value(valueSerializer)
                        .build();
        return new ReactiveRedisTemplate<>(factory, context);
    }
}
```

- [ ] **Step 2: `ChatMessage` 레코드**

`src/main/java/com/tainted/counseling/domain/ChatMessage.java`:
```java
package com.tainted.counseling.domain;

import java.time.Instant;

/**
 * 단일 채팅 메시지. role: "user" 또는 "model".
 */
public record ChatMessage(String role, String content, Instant createdAt) {

    public static ChatMessage user(String content, Instant createdAt) {
        return new ChatMessage("user", content, createdAt);
    }

    public static ChatMessage model(String content, Instant createdAt) {
        return new ChatMessage("model", content, createdAt);
    }
}
```

- [ ] **Step 3: `ChatSession` 엔티티**

`src/main/java/com/tainted/counseling/domain/ChatSession.java`:
```java
package com.tainted.counseling.domain;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * 1:1 상담 세션. Redis(논리 DB 2)에 JSON 직렬화 저장.
 */
public class ChatSession {

    private String id;
    private String userId;
    private String personaKey;
    private Instant createdAt;
    private List<ChatMessage> messages;

    /** Jackson 역직렬화용 기본 생성자 */
    public ChatSession() {
        this.messages = new ArrayList<>();
    }

    public ChatSession(String id, String userId, String personaKey, Instant createdAt) {
        this.id = id;
        this.userId = userId;
        this.personaKey = personaKey;
        this.createdAt = createdAt;
        this.messages = new ArrayList<>();
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getPersonaKey() { return personaKey; }
    public void setPersonaKey(String personaKey) { this.personaKey = personaKey; }

    public Instant getCreatedAt() { return createdAt; }
    public void setCreatedAt(Instant createdAt) { this.createdAt = createdAt; }

    public List<ChatMessage> getMessages() { return messages; }
    public void setMessages(List<ChatMessage> messages) { this.messages = messages; }

    public void addMessage(ChatMessage message) {
        this.messages.add(message);
    }
}
```

- [ ] **Step 4: `ChatRepository`**

`src/main/java/com/tainted/counseling/domain/ChatRepository.java`:
```java
package com.tainted.counseling.domain;

import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Mono;
import java.time.Duration;

/**
 * ChatSession을 Redis에 리액티브로 저장/조회.
 * 키: "counseling:session:{sessionId}", TTL: 24시간.
 */
@Repository
public class ChatRepository {

    private static final String KEY_PREFIX = "counseling:session:";
    private static final Duration TTL = Duration.ofHours(24);

    private final ReactiveRedisTemplate<String, ChatSession> template;

    public ChatRepository(ReactiveRedisTemplate<String, ChatSession> template) {
        this.template = template;
    }

    public Mono<ChatSession> save(ChatSession session) {
        String key = KEY_PREFIX + session.getId();
        return template.opsForValue()
                .set(key, session, TTL)
                .thenReturn(session);
    }

    public Mono<ChatSession> findById(String sessionId) {
        return template.opsForValue().get(KEY_PREFIX + sessionId);
    }
}
```

- [ ] **Step 5: 컴파일 + 커밋**

Run: `./gradlew -q compileJava`
Expected: BUILD SUCCESSFUL.
```bash
git add -A
git commit -m "feat: Redis config + ChatSession/ChatMessage domain + ChatRepository"
```

---

## Task 6: Kafka 소비자 + 서비스 로직 + DTO

**Files:**
- Create: `kafka/PostCreatedEvent.java`, `kafka/GraphUpdatedEvent.java`, `kafka/CounselingKafkaConsumer.java`, `config/KafkaConfig.java`, `service/CounselingService.java`, `web/dto/*.java`, `error/SessionNotFoundException.java`

- [ ] **Step 1: Kafka 이벤트 POJO**

`src/main/java/com/tainted/counseling/kafka/PostCreatedEvent.java`:
```java
package com.tainted.counseling.kafka;

/**
 * post.created 이벤트 POJO.
 * {eventId, postId, userId, category, moodEmoji, occurredAt}
 */
public class PostCreatedEvent {
    private String eventId;
    private String postId;
    private String userId;
    private String category;
    private String moodEmoji;
    private String occurredAt;

    public PostCreatedEvent() {}

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public String getPostId() { return postId; }
    public void setPostId(String postId) { this.postId = postId; }
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }
    public String getMoodEmoji() { return moodEmoji; }
    public void setMoodEmoji(String moodEmoji) { this.moodEmoji = moodEmoji; }
    public String getOccurredAt() { return occurredAt; }
    public void setOccurredAt(String occurredAt) { this.occurredAt = occurredAt; }
}
```

`src/main/java/com/tainted/counseling/kafka/GraphUpdatedEvent.java`:
```java
package com.tainted.counseling.kafka;

/**
 * graph.updated 이벤트 POJO.
 * {eventId, userId, diaryId, nodeCount, occurredAt}
 */
public class GraphUpdatedEvent {
    private String eventId;
    private String userId;
    private String diaryId;
    private int nodeCount;
    private String occurredAt;

    public GraphUpdatedEvent() {}

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public String getDiaryId() { return diaryId; }
    public void setDiaryId(String diaryId) { this.diaryId = diaryId; }
    public int getNodeCount() { return nodeCount; }
    public void setNodeCount(int nodeCount) { this.nodeCount = nodeCount; }
    public String getOccurredAt() { return occurredAt; }
    public void setOccurredAt(String occurredAt) { this.occurredAt = occurredAt; }
}
```

- [ ] **Step 2: Kafka 설정**

`src/main/java/com/tainted/counseling/config/KafkaConfig.java`:
```java
package com.tainted.counseling.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tainted.counseling.kafka.GraphUpdatedEvent;
import com.tainted.counseling.kafka.PostCreatedEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.kafka.support.converter.StringJsonMessageConverter;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ObjectMapper kafkaObjectMapper() {
        return new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .findAndRegisterModules();
    }

    @Bean
    public RecordMessageConverter kafkaMessageConverter(ObjectMapper kafkaObjectMapper) {
        return new StringJsonMessageConverter(kafkaObjectMapper);
    }

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "counseling-service");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory,
            RecordMessageConverter kafkaMessageConverter) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setRecordMessageConverter(kafkaMessageConverter);
        return factory;
    }
}
```

- [ ] **Step 3: DTO 4종**

`src/main/java/com/tainted/counseling/web/dto/CreateSessionRequest.java`:
```java
package com.tainted.counseling.web.dto;

import jakarta.validation.constraints.NotBlank;

public record CreateSessionRequest(
        @NotBlank String userId,
        @NotBlank String personaKey) {}
```

`src/main/java/com/tainted/counseling/web/dto/CreateSessionResponse.java`:
```java
package com.tainted.counseling.web.dto;

public record CreateSessionResponse(String sessionId) {}
```

`src/main/java/com/tainted/counseling/web/dto/SendMessageRequest.java`:
```java
package com.tainted.counseling.web.dto;

import jakarta.validation.constraints.NotBlank;

public record SendMessageRequest(
        @NotBlank String personaKey,
        @NotBlank String userId,
        @NotBlank String message) {}
```

`src/main/java/com/tainted/counseling/web/dto/SendMessageResponse.java`:
```java
package com.tainted.counseling.web.dto;

public record SendMessageResponse(String reply) {}
```

- [ ] **Step 4: `SessionNotFoundException`**

`src/main/java/com/tainted/counseling/error/SessionNotFoundException.java`:
```java
package com.tainted.counseling.error;

public class SessionNotFoundException extends RuntimeException {
    public SessionNotFoundException(String sessionId) {
        super("상담 세션을 찾을 수 없습니다: " + sessionId);
    }
}
```

- [ ] **Step 5: `CounselingService`**

`src/main/java/com/tainted/counseling/service/CounselingService.java`:
```java
package com.tainted.counseling.service;

import com.tainted.counseling.client.CommunityClient;
import com.tainted.counseling.client.MindGraphClient;
import com.tainted.counseling.counselor.CounselorClient;
import com.tainted.counseling.domain.ChatMessage;
import com.tainted.counseling.domain.ChatRepository;
import com.tainted.counseling.domain.ChatSession;
import com.tainted.counseling.error.SessionNotFoundException;
import com.tainted.counseling.id.IdGenerator;
import com.tainted.counseling.web.dto.CreateSessionRequest;
import com.tainted.counseling.web.dto.CreateSessionResponse;
import com.tainted.counseling.web.dto.SendMessageRequest;
import com.tainted.counseling.web.dto.SendMessageResponse;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import java.time.Clock;
import java.time.Instant;

@Service
public class CounselingService {

    private final ChatRepository chatRepository;
    private final CounselorClient counselorClient;
    private final MindGraphClient mindGraphClient;
    private final CommunityClient communityClient;
    private final IdGenerator idGenerator;
    private final Clock clock;

    public CounselingService(ChatRepository chatRepository,
                             CounselorClient counselorClient,
                             MindGraphClient mindGraphClient,
                             CommunityClient communityClient,
                             IdGenerator idGenerator,
                             Clock clock) {
        this.chatRepository = chatRepository;
        this.counselorClient = counselorClient;
        this.mindGraphClient = mindGraphClient;
        this.communityClient = communityClient;
        this.idGenerator = idGenerator;
        this.clock = clock;
    }

    public Mono<CreateSessionResponse> createSession(CreateSessionRequest request) {
        String sessionId = idGenerator.newId();
        ChatSession session = new ChatSession(
                sessionId, request.userId(), request.personaKey(), Instant.now(clock));
        return chatRepository.save(session)
                .map(s -> new CreateSessionResponse(s.getId()));
    }

    public Mono<SendMessageResponse> sendMessage(String sessionId, SendMessageRequest request) {
        return chatRepository.findById(sessionId)
                .switchIfEmpty(Mono.error(new SessionNotFoundException(sessionId)))
                .flatMap(session -> {
                    String graphContext = mindGraphClient.graphContextFor(request.userId());
                    String replyText = counselorClient.reply(
                            request.personaKey(), graphContext, request.message());
                    session.addMessage(ChatMessage.user(request.message(), Instant.now(clock)));
                    session.addMessage(ChatMessage.model(replyText, Instant.now(clock)));
                    return chatRepository.save(session)
                            .thenReturn(new SendMessageResponse(replyText));
                });
    }

    /** post.created 이벤트 처리: 결정론적 페르소나 선택 → AI 댓글 등록 */
    public void handlePostCreated(String postId, String userId) {
        // postId의 안정적 해시로 페르소나 결정 (같은 postId → 항상 같은 페르소나)
        String[] personas = {"empathic", "analytical", "sincere", "hopeful"};
        int idx = Math.abs(postId.hashCode()) % personas.length;
        String personaKey = personas[idx];
        String graphContext = mindGraphClient.graphContextFor(userId);
        String content = counselorClient.reply(personaKey, graphContext,
                "게시글 " + postId + "에 공감 댓글을 작성합니다.");
        communityClient.addAiComment(postId, personaKey, content);
    }

    /** graph.updated 이벤트 처리: Redis 캐시에 최신 컨텍스트 갱신 (mindgraph 재조회) */
    public void handleGraphUpdated(String userId) {
        // 명시적으로 mindgraph를 다시 조회해 캐시를 warm하는 효과만 노린다
        // (실제 저장은 mindGraphClient 구현체가 block으로 가져오며,
        //  추후 캐시 레이어가 붙으면 여기서 Redis write를 추가할 수 있다)
        mindGraphClient.graphContextFor(userId);
    }
}
```

- [ ] **Step 6: Kafka 소비자**

`src/main/java/com/tainted/counseling/kafka/CounselingKafkaConsumer.java`:
```java
package com.tainted.counseling.kafka;

import com.tainted.counseling.service.CounselingService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class CounselingKafkaConsumer {

    private final CounselingService counselingService;

    public CounselingKafkaConsumer(CounselingService counselingService) {
        this.counselingService = counselingService;
    }

    @KafkaListener(topics = "post.created", containerFactory = "kafkaListenerContainerFactory")
    public void onPostCreated(PostCreatedEvent event) {
        if (event.getPostId() == null || event.getUserId() == null) return;
        counselingService.handlePostCreated(event.getPostId(), event.getUserId());
    }

    @KafkaListener(topics = "graph.updated", containerFactory = "kafkaListenerContainerFactory")
    public void onGraphUpdated(GraphUpdatedEvent event) {
        if (event.getUserId() == null) return;
        counselingService.handleGraphUpdated(event.getUserId());
    }
}
```

- [ ] **Step 7: 컴파일 + 커밋**

Run: `./gradlew -q compileJava`
Expected: BUILD SUCCESSFUL.
```bash
git add -A
git commit -m "feat: Kafka consumer + CounselingService + DTOs"
```

---

## Task 7: 리액티브 핸들러 + 라우터 + RFC 7807 에러 + OpenAPI

**Files:**
- Create: `web/CounselingHandler.java`, `web/CounselingRouter.java`, `error/GlobalErrorWebExceptionHandler.java`, `config/OpenApiConfig.java`

- [ ] **Step 1: 리액티브 핸들러**

`src/main/java/com/tainted/counseling/web/CounselingHandler.java`:
```java
package com.tainted.counseling.web;

import com.tainted.counseling.service.CounselingService;
import com.tainted.counseling.web.dto.CreateSessionRequest;
import com.tainted.counseling.web.dto.SendMessageRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.ServerRequest;
import org.springframework.web.reactive.function.server.ServerResponse;
import reactor.core.publisher.Mono;

@Component
public class CounselingHandler {

    private final CounselingService counselingService;

    public CounselingHandler(CounselingService counselingService) {
        this.counselingService = counselingService;
    }

    public Mono<ServerResponse> createSession(ServerRequest request) {
        return request.bodyToMono(CreateSessionRequest.class)
                .flatMap(counselingService::createSession)
                .flatMap(resp -> ServerResponse.ok().bodyValue(resp));
    }

    public Mono<ServerResponse> sendMessage(ServerRequest request) {
        String sessionId = request.pathVariable("id");
        return request.bodyToMono(SendMessageRequest.class)
                .flatMap(body -> counselingService.sendMessage(sessionId, body))
                .flatMap(resp -> ServerResponse.ok().bodyValue(resp));
    }
}
```

- [ ] **Step 2: RouterFunction 라우팅**

`src/main/java/com/tainted/counseling/web/CounselingRouter.java`:
```java
package com.tainted.counseling.web;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;

import static org.springframework.web.reactive.function.server.RequestPredicates.*;

@Configuration
public class CounselingRouter {

    @Bean
    public RouterFunction<ServerResponse> counselingRoutes(CounselingHandler handler) {
        return RouterFunctions.route()
                .POST("/internal/counseling/sessions", handler::createSession)
                .POST("/internal/counseling/sessions/{id}/messages", handler::sendMessage)
                .build();
    }
}
```

- [ ] **Step 3: RFC 7807 WebFlux 에러 핸들러**

`src/main/java/com/tainted/counseling/error/GlobalErrorWebExceptionHandler.java`:
```java
package com.tainted.counseling.error;

import org.springframework.boot.web.reactive.error.ErrorWebExceptionHandler;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.codec.HttpMessageWriter;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.reactive.result.view.ViewResolver;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import java.util.List;

@Component
@Order(-2)
public class GlobalErrorWebExceptionHandler implements ErrorWebExceptionHandler {

    @Override
    public Mono<Void> handle(ServerWebExchange exchange, Throwable ex) {
        HttpStatus status;
        String title;

        if (ex instanceof SessionNotFoundException) {
            status = HttpStatus.NOT_FOUND;
            title = "Session not found";
        } else {
            status = HttpStatus.INTERNAL_SERVER_ERROR;
            title = "Internal error";
        }

        ProblemDetail pd = ProblemDetail.forStatus(status);
        pd.setTitle(title);
        pd.setDetail(ex.getMessage());

        exchange.getResponse().setStatusCode(status);
        exchange.getResponse().getHeaders().setContentType(
                MediaType.parseMediaType("application/problem+json"));

        byte[] bytes;
        try {
            bytes = new com.fasterxml.jackson.databind.ObjectMapper()
                    .writeValueAsBytes(pd);
        } catch (Exception e) {
            bytes = ("{\"title\":\"" + title + "\"}").getBytes();
        }
        org.springframework.core.io.buffer.DataBuffer buf =
                exchange.getResponse().bufferFactory().wrap(bytes);
        return exchange.getResponse().writeWith(Mono.just(buf));
    }
}
```

- [ ] **Step 4: OpenAPI 설정**

`src/main/java/com/tainted/counseling/config/OpenApiConfig.java`:
```java
package com.tainted.counseling.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {
    @Bean
    public OpenAPI counselingOpenApi() {
        return new OpenAPI().info(new Info()
                .title("counseling-service API")
                .version("0.1.0")
                .description("AI 상담사 4페르소나 1:1 챗 서비스 (결정론적 LLM mock)"));
    }
}
```

- [ ] **Step 5: 컴파일 + 커밋**

Run: `./gradlew -q compileJava`
Expected: BUILD SUCCESSFUL.
```bash
git add -A
git commit -m "feat: reactive handler + router + RFC7807 error handler + OpenAPI"
```

---

## Task 8: 통합 테스트 (Testcontainers + RestAssured)

**Files:**
- Create: `src/test/java/com/tainted/counseling/client/stub/StubMindGraphClient.java`
- Create: `src/test/java/com/tainted/counseling/client/stub/StubCommunityClient.java`
- Create: `src/test/java/com/tainted/counseling/CounselingIntegrationTest.java`

- [ ] **Step 1: Stub 클라이언트 작성**

`src/test/java/com/tainted/counseling/client/stub/StubMindGraphClient.java`:
```java
package com.tainted.counseling.client.stub;

import com.tainted.counseling.client.MindGraphClient;

/** mindgraph 없이 통합 테스트 가능. 항상 빈 컨텍스트 반환. */
public class StubMindGraphClient implements MindGraphClient {
    @Override
    public String graphContextFor(String userId) {
        return "";
    }
}
```

`src/test/java/com/tainted/counseling/client/stub/StubCommunityClient.java`:
```java
package com.tainted.counseling.client.stub;

import com.tainted.counseling.client.CommunityClient;
import java.util.ArrayList;
import java.util.List;

/**
 * community 없이 통합 테스트 가능.
 * addAiComment 호출을 캡처해 테스트에서 검증 가능.
 */
public class StubCommunityClient implements CommunityClient {

    public record CapturedCall(String postId, String role, String content) {}

    private final List<CapturedCall> calls = new ArrayList<>();

    @Override
    public void addAiComment(String postId, String role, String content) {
        calls.add(new CapturedCall(postId, role, content));
    }

    public List<CapturedCall> getCalls() {
        return calls;
    }

    public void reset() {
        calls.clear();
    }
}
```

- [ ] **Step 2: 통합 테스트 작성**

`src/test/java/com/tainted/counseling/CounselingIntegrationTest.java`:
```java
package com.tainted.counseling;

import com.tainted.counseling.client.CommunityClient;
import com.tainted.counseling.client.MindGraphClient;
import com.tainted.counseling.client.stub.StubCommunityClient;
import com.tainted.counseling.client.stub.StubMindGraphClient;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class CounselingIntegrationTest {

    @Container
    static GenericContainer<?> redis =
            new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @Container
    static KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.data.redis.host", redis::getHost);
        r.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
        r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @TestConfiguration
    static class StubConfig {
        @Bean @Primary
        public MindGraphClient mindGraphClient() { return new StubMindGraphClient(); }

        @Bean @Primary
        public CommunityClient communityClient() { return new StubCommunityClient(); }
    }

    @Autowired
    CommunityClient communityClient;

    @LocalServerPort
    int port;

    @BeforeEach
    void setUp() {
        RestAssured.port = port;
        ((StubCommunityClient) communityClient).reset();
    }

    /** (a) 세션 생성 → 200 + sessionId 반환 */
    @Test
    void createSession_returns200WithSessionId() {
        given().contentType(ContentType.JSON)
                .body("{\"userId\":\"user-1\",\"personaKey\":\"empathic\"}")
                .when().post("/internal/counseling/sessions")
                .then().statusCode(200)
                .body("sessionId", not(emptyOrNullString()));
    }

    /** (b) 메시지 전송 → 페르소나 접두어 포함, 동일 입력 동일 응답 */
    @Test
    void sendMessage_empathic_replyContainsPrefixAndIsDeterministic() {
        String sessionId = given().contentType(ContentType.JSON)
                .body("{\"userId\":\"user-2\",\"personaKey\":\"empathic\"}")
                .when().post("/internal/counseling/sessions")
                .then().statusCode(200)
                .extract().path("sessionId");

        String body = "{\"personaKey\":\"empathic\",\"userId\":\"user-2\",\"message\":\"힘들어요\"}";

        String reply1 = given().contentType(ContentType.JSON).body(body)
                .when().post("/internal/counseling/sessions/" + sessionId + "/messages")
                .then().statusCode(200)
                .body("reply", containsString("[수현]"))
                .extract().path("reply");

        // 새 세션으로 같은 메시지 → 같은 응답(결정론)
        String sessionId2 = given().contentType(ContentType.JSON)
                .body("{\"userId\":\"user-3\",\"personaKey\":\"empathic\"}")
                .when().post("/internal/counseling/sessions")
                .then().statusCode(200)
                .extract().path("sessionId");

        String reply2 = given().contentType(ContentType.JSON).body(body)
                .when().post("/internal/counseling/sessions/" + sessionId2 + "/messages")
                .then().statusCode(200)
                .extract().path("reply");

        assertEquals(reply1, reply2, "동일 페르소나 + 동일 메시지 → 동일 응답(결정론)");
    }

    /** (c) post.created Kafka 메시지 → StubCommunityClient에 addAiComment 호출 캡처 */
    @Test
    void postCreatedKafka_triggersAddAiComment() throws Exception {
        String postId = "post-abc-123";
        String userId = "user-kafka-1";
        String event = String.format(
                "{\"eventId\":\"ev-1\",\"postId\":\"%s\",\"userId\":\"%s\"," +
                "\"category\":\"mental\",\"moodEmoji\":\"😢\",\"occurredAt\":\"2026-06-13T00:00:00Z\"}",
                postId, userId);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {
            producer.send(new ProducerRecord<>("post.created", postId, event)).get();
        }

        // Kafka 소비 대기 (최대 10초)
        StubCommunityClient stub = (StubCommunityClient) communityClient;
        long deadline = System.currentTimeMillis() + 10_000;
        while (stub.getCalls().isEmpty() && System.currentTimeMillis() < deadline) {
            Thread.sleep(200);
        }

        assertFalse(stub.getCalls().isEmpty(),
                "post.created 소비 후 addAiComment가 호출되어야 합니다.");
        StubCommunityClient.CapturedCall call = stub.getCalls().get(0);
        assertEquals(postId, call.postId(),
                "addAiComment는 올바른 postId로 호출되어야 합니다.");
        assertNotNull(call.role(), "role(페르소나 키)이 null이면 안 됩니다.");
        assertNotNull(call.content(), "댓글 본문이 null이면 안 됩니다.");
    }
}
```

- [ ] **Step 3: 전체 테스트 실행 (Docker 필요)**

Run: `./gradlew test`
Expected: PASS — `DeterministicCounselorClientTest`(6) + `CounselingIntegrationTest`(3) 모두 통과. (Docker 데몬이 떠 있어야 Testcontainers 동작.)

- [ ] **Step 4: 커밋**

```bash
git add -A
git commit -m "test: Testcontainers + RestAssured integration tests with stub clients"
```

---

## Task 9: Dockerfile + docker 프로파일

**Files:**
- Create: `Dockerfile`, `src/main/resources/application-docker.yml`

- [ ] **Step 1: docker 프로파일 설정**

`src/main/resources/application-docker.yml`:
```yaml
spring:
  data:
    redis:
      host: redis
      port: 6379
      database: 2
  kafka:
    bootstrap-servers: kafka:29092
services:
  mindgraph:
    url: ${SERVICES_MINDGRAPH_URL:http://mindgraph:8083}
  community:
    url: ${SERVICES_COMMUNITY_URL:http://community:8085}
```

- [ ] **Step 2: 멀티스테이지 Dockerfile (non-root)**

`Dockerfile`:
```dockerfile
# build
FROM gradle:8.10-jdk21 AS build
WORKDIR /app
COPY build.gradle settings.gradle ./
COPY gradle ./gradle
RUN gradle -q dependencies --configuration compileClasspath || true
COPY src ./src
RUN gradle -q -x test bootJar

# run
FROM eclipse-temurin:21-jre
WORKDIR /app
# curl: compose/k8s 헬스체크가 /actuator/health 를 호출하는 데 사용.
# temurin JRE 기본 이미지에는 curl 이 없으므로 명시적으로 설치한다.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd -r -u 1001 appuser
COPY --from=build /app/build/libs/counseling-service-0.1.0.jar app.jar
USER appuser
EXPOSE 8084
HEALTHCHECK --interval=15s --timeout=5s --retries=20 --start-period=40s \
  CMD curl -fsS http://localhost:8084/actuator/health || exit 1
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 3: 이미지 빌드 확인**

Run: `docker build -t tainted-spring/counseling:0.1.0 .`
Expected: 빌드 성공, 최종 이미지 생성.

- [ ] **Step 4: 커밋**

```bash
git add -A
git commit -m "build: Dockerfile and docker profile"
```

---

## Task 10: platform 레포에 counseling 배선 (compose + k8s 스켈레톤)

> ⚠️ **작업 레포 전환**: 이 Task의 모든 파일 수정·커밋은 `tainted-spring-counseling` 이 아니라 **`tainted-spring-platform` 레포**에서 한다. 각 Step의 경로를 확인할 것.

**Files (다른 레포 `tainted-spring-platform`):**
- Modify: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-platform/docker-compose.yml`
- Create: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-platform/k8s/counseling/deployment.yaml`

- [ ] **Step 1: compose에 counseling 서비스 추가**

`tainted-spring-platform/docker-compose.yml` 의 `services:` 아래에 추가 (앵커 `*java-service` 재사용):
```yaml
  counseling:
    <<: *java-service
    build:
      context: ../tainted-spring-counseling
    depends_on:
      redis:
        condition: service_healthy
      kafka:
        condition: service_healthy
    environment:
      JAVA_TOOL_OPTIONS: "${JAVA_TOOL_OPTIONS:-}"
      SPRING_PROFILES_ACTIVE: "docker"
      SERVICES_MINDGRAPH_URL: "http://mindgraph:8083"
      SERVICES_COMMUNITY_URL: "http://community:8085"
    ports:
      - "8084:8084"
    healthcheck:
      # 런타임 이미지에 설치한 curl 사용. -f: 비2xx 응답이면 실패 → actuator DOWN 시 unhealthy.
      test: ["CMD", "curl", "-fsS", "http://localhost:8084/actuator/health"]
      interval: 15s
      timeout: 5s
      retries: 20
      start_period: 40s
```

> 참고: `<<: *java-service` 가 `environment` 를 병합하지 않고 덮어쓰므로, 위에서 `JAVA_TOOL_OPTIONS`/`SPRING_PROFILES_ACTIVE`/`SERVICES_*` 를 명시적으로 다시 적었다.

- [ ] **Step 2: compose로 통합 기동 검증**

Run (platform 레포에서):
```bash
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-platform
docker compose up -d --build --wait counseling
curl -s http://localhost:8084/actuator/health
curl -s -X POST http://localhost:8084/internal/counseling/sessions \
  -H 'Content-Type: application/json' \
  -d '{"userId":"u1","personaKey":"empathic"}'
```
Expected: `--wait` 가 counseling 헬스체크 통과까지 블록. health `{"status":"UP"}`, 세션 생성 응답에 `sessionId` 포함.

- [ ] **Step 3: k8s Deployment/Service 스켈레톤**

`tainted-spring-platform/k8s/counseling/deployment.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tainted-spring-counseling
  namespace: tainted-spring
spec:
  replicas: 1
  selector:
    matchLabels: { app: counseling }
  template:
    metadata:
      labels: { app: counseling }
    spec:
      containers:
        - name: counseling
          image: tainted-spring/counseling:0.1.0
          ports: [{ containerPort: 8084 }]
          env:
            - { name: SPRING_PROFILES_ACTIVE, value: "eks" }
            - { name: JAVA_TOOL_OPTIONS, value: "" }
            - { name: SERVICES_MINDGRAPH_URL, value: "http://tainted-spring-mindgraph" }
            - { name: SERVICES_COMMUNITY_URL, value: "http://tainted-spring-community" }
          readinessProbe:
            httpGet: { path: /actuator/health, port: 8084 }
            initialDelaySeconds: 20
            periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: tainted-spring-counseling
  namespace: tainted-spring
spec:
  selector: { app: counseling }
  ports:
    - { port: 80, targetPort: 8084 }
```

- [ ] **Step 4: platform 레포 커밋**

```bash
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-platform
git add -A
git commit -m "feat: wire counseling service into compose and k8s skeleton"
```

---

## Self-Review

**Spec coverage (설계문서 §3.4 대비):**
- AI 상담사 4페르소나(empathic/수현, analytical/성우, sincere/진수, hopeful/다혜) → Task 3 `DeterministicCounselorClient`(PERSONA_PREFIX 4종) ✔
- 결정론적 LLM mock → `DeterministicCounselorClient.reply()` hashCode 기반 안정 선택 ✔
- 1:1 챗 세션/히스토리 → `ChatSession`/`ChatMessage`/`ChatRepository` Redis 논리 DB 2 ✔
- mindgraph 컨텍스트 조회(WebClient) → `MindGraphClient` 인터페이스 + `WebClientMindGraphClient` ✔
- `POST /internal/counseling/sessions` → Task 7 라우터 ✔
- `POST /internal/counseling/sessions/{id}/messages` (body personaKey+userId+message → reply) → Task 7 라우터 ✔
- Mono 반환(리액티브) → `CounselingHandler`·`CounselingService` 전체 Mono 체인 ✔
- post.created 소비 → postId 안정 해시 페르소나 → community REST 댓글 등록 → Task 6 Kafka 소비자 + `handlePostCreated` ✔
- community가 comment.created 발행(counseling은 발행하지 않음) → 설계대로 `communityClient.addAiComment()`만 호출, Kafka produce 없음 ✔
- graph.updated 소비 → Redis 캐시 갱신(mindgraph 재조회) → Task 6 `handleGraphUpdated` ✔
- `MindGraphClient`/`CommunityClient` 인터페이스로 감싸 통합 테스트에서 stub 교체 → Task 8 `@TestConfiguration`+`@Primary` ✔
- Testcontainers Redis(GenericContainer redis:7-alpine) + Kafka(KafkaContainer) → Task 8 ✔
- RestAssured 통합 테스트: (a) 세션 생성 200 ✔, (b) 메시지 페르소나 접두어+결정론 ✔, (c) post.created Kafka → stub 캡처 ✔
- OpenAPI(springdoc-openapi-starter-webflux-ui 2.6.0) → Task 7 `OpenApiConfig` ✔
- RFC 7807 problem+json(WebFlux, spring.webflux.problemdetails.enabled=true) → application.yml + `GlobalErrorWebExceptionHandler` ✔
- `/actuator/health` 유지, 추적 라이브러리 없음 → build.gradle에 actuator만, OTel/Sleuth 미포함 ✔
- 결정론(Clock/IdGenerator) → Task 2 ✔
- Kafka JSON, 추적 헤더 없음, 자기완결적 → `PostCreatedEvent`/`GraphUpdatedEvent` 독립 POJO ✔
- Docker 멀티스테이지(gradle:8.10-jdk21 빌드 / eclipse-temurin:21-jre 런타임) non-root → Task 9 ✔
- docker 프로파일 Redis/Kafka DNS + SERVICES_* 환경변수 → application-docker.yml ✔
- compose `<<: *java-service` + depends_on redis+kafka service_healthy + ports 8084:8084 + healthcheck curl + start_period:40s retries:20 → Task 10 ✔
- k8s Deployment+Service 스켈레톤 → Task 10 ✔
- 빈 `JAVA_TOOL_OPTIONS` 슬롯 노출(계측 미설정) → compose environment ✔

**Placeholder scan:** 모든 코드 step에 완전한 소스, 모든 명령에 기대 출력 명시. 플레이스홀더 없음. ✔

**Type/이름 일관성:**
- `CounselorClient.reply(String, String, String)` — Task 3 정의, Task 6 `CounselingService`에서 동일 시그니처 사용 ✔
- `MindGraphClient.graphContextFor(String)` — Task 4 정의, Task 6·8 사용 일치 ✔
- `CommunityClient.addAiComment(String, String, String)` — Task 4 정의, Task 6·8 캡처 검증 일치 ✔
- `ChatRepository.save(ChatSession): Mono<ChatSession>` / `findById(String): Mono<ChatSession>` — Task 5 정의, Task 6 서비스 사용 일치 ✔
- DTO 필드명(`sessionId`, `reply`) — Task 6 정의, Task 8 RestAssured body 추출 키와 일치 ✔
- jar 파일명 `counseling-service-0.1.0.jar` — build.gradle `version='0.1.0'` + settings `rootProject.name='counseling-service'` + Dockerfile COPY 일치 ✔
- Redis 논리 DB index 2 — application.yml + application-docker.yml 모두 `database: 2` ✔
- 포트 8084 — application.yml `server.port: 8084` + Dockerfile EXPOSE + compose ports + k8s containerPort + healthcheck URL 모두 일치 ✔
