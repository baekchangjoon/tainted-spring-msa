# tainted-spring-community Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 익명 게시판(글/댓글/좋아요)을 제공하고 `post.created`·`comment.created`·`mood.logged` 세 Kafka 이벤트를 발행하는 **레거시** `community-service`(Java 1.8 · Maven · Spring Boot 2.7.18 · MySQL)를 RestAssured/Testcontainers로 검증 가능하게 구현한다.

**Architecture:** Spring MVC REST 서비스(레거시 패턴). 글/댓글은 MySQL(`community` DB)에 저장하고, 상태 변경 후 spring-kafka로 JSON 이벤트를 발행한다(추적 헤더 없음, 자기완결적 페이로드). 표시명은 게시글의 `nickname`이며 내부 식별·이벤트 발행에는 `userId`를 쓴다. 시간/ID는 주입 가능한 `Clock`/`IdGenerator` 빈으로 결정론 확보. 추적/관측 라이브러리는 넣지 않음(actuator health만 유지). **Java 8 제약**: record/var/텍스트블록/switch식/패턴매칭 미사용, `javax.*` 네임스페이스 사용.

**Tech Stack:** Java 1.8, Maven, Spring Boot 2.7.18, Spring Data JPA, spring-kafka, springdoc-openapi-ui 1.7.0, MySQL 8.4, Kafka, Testcontainers 1.20.3, RestAssured.

---

## 사전 조건

- `tainted-spring-platform` 계획이 완료되어 있어야 통합 실행이 쉽다(단위/통합 테스트는 Testcontainers로 자체 인프라를 띄우므로 platform 없이도 테스트는 통과한다).
- **레포 위치**: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-community/` (모든 경로는 이 루트 기준).
- **포트**: 8085. **패키지 루트**: `com.tainted.community`. **데이터스토어**: MySQL DB `community` (Kafka 이벤트 발행).
- **로컬 사전 설치**: **Docker 데몬**(Task 8 통합 테스트의 Testcontainers, Task 9 이미지 빌드, Task 10 compose에 필요), **JDK 8**(eclipse-temurin 8 등), **Maven 3.9+**, **Git**. 단, 단위 테스트 `MockMoodEmojiTest`/`SeedDataTest` 같은 비-컨테이너 테스트는 Docker 없이도 동작한다.

> ⚠️ **Java 1.8 / Boot 2.7 주의**: 이 서비스는 레거시다. 코드 전반에서 `javax.persistence`/`javax.validation`(NOT `jakarta.*`), 평범한 클래스 + 명시적 생성자/게터(NOT `record`), `var` 미사용, springdoc 1.x(NOT 2.x), `mysql:mysql-connector-java`(NOT `com.mysql:mysql-connector-j`)를 사용한다. RFC 7807은 Boot 2.7에 `ProblemDetail` 클래스가 없으므로 직접 POJO + `@RestControllerAdvice`로 구현한다.

## File Structure

```
pom.xml
Dockerfile
src/main/resources/application.yml
src/main/resources/application-docker.yml
src/main/java/com/tainted/community/
  CommunityApplication.java
  config/DeterminismConfig.java          # Clock 빈
  config/OpenApiConfig.java
  id/IdGenerator.java                    # 인터페이스
  id/UuidIdGenerator.java                # 기본 구현 (평범한 클래스)
  domain/Post.java                       # JPA 엔티티 (javax.persistence)
  domain/PostRepository.java
  domain/Comment.java                    # JPA 엔티티 (javax.persistence)
  domain/CommentRepository.java
  event/CommunityEventPublisher.java     # spring-kafka 발행
  service/CommunityService.java          # 비즈니스 로직 + publish-after-save
  web/CommunityController.java           # /internal/posts/*
  web/dto/CreatePostRequest.java
  web/dto/CreatePostResponse.java
  web/dto/PostResponse.java
  web/dto/CreateCommentRequest.java
  web/dto/CommentResponse.java
  web/dto/LikeResponse.java
  error/PostNotFoundException.java
  error/GlobalExceptionHandler.java      # RFC 7807 (ProblemResponse POJO)
  error/ProblemResponse.java             # type/title/status/detail
  seed/SeedDataRunner.java               # CommandLineRunner, profile 'seed'
src/test/java/com/tainted/community/
  id/UuidIdGeneratorTest.java            # 단위
  CommunityIntegrationTest.java          # Testcontainers(MySQL+Kafka) + RestAssured
```

---

## Task 1: 레포 스캐폴드 + 빌드 가능한 빈 앱

**Files:**
- Create: `pom.xml`, `src/main/java/com/tainted/community/CommunityApplication.java`, `src/main/resources/application.yml`, `.gitignore`

- [ ] **Step 1: 디렉토리 생성 + git init**

Run:
```bash
mkdir -p /Users/changjoonbaek/workspace_claude-code/tainted-spring-community
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-community
git init
mkdir -p src/main/java/com/tainted/community src/main/resources src/test/java/com/tainted/community
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

> Boot 2.7.18 parent가 spring-kafka·mysql-connector-java·rest-assured 버전을 관리하므로 해당 의존성에는 버전을 명시하지 않는다. springdoc은 Boot 2.x용 1.x를 **명시적으로** 고정한다(2.x는 Boot 3 전용).

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
  <artifactId>community-service</artifactId>
  <version>0.1.0</version>
  <properties>
    <java.version>1.8</java.version>
  </properties>
  <dependencies>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-web</artifactId></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-data-jpa</artifactId></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-validation</artifactId></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-actuator</artifactId></dependency>
    <dependency><groupId>mysql</groupId><artifactId>mysql-connector-java</artifactId><scope>runtime</scope></dependency>
    <dependency><groupId>org.springframework.kafka</groupId><artifactId>spring-kafka</artifactId></dependency>
    <dependency><groupId>org.springdoc</groupId><artifactId>springdoc-openapi-ui</artifactId><version>1.7.0</version></dependency>

    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-test</artifactId><scope>test</scope></dependency>
    <dependency><groupId>org.springframework.kafka</groupId><artifactId>spring-kafka-test</artifactId><scope>test</scope></dependency>
    <dependency><groupId>org.testcontainers</groupId><artifactId>junit-jupiter</artifactId><version>1.20.3</version><scope>test</scope></dependency>
    <dependency><groupId>org.testcontainers</groupId><artifactId>mysql</artifactId><version>1.20.3</version><scope>test</scope></dependency>
    <dependency><groupId>org.testcontainers</groupId><artifactId>kafka</artifactId><version>1.20.3</version><scope>test</scope></dependency>
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

`src/main/java/com/tainted/community/CommunityApplication.java`:
```java
package com.tainted.community;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class CommunityApplication {
    public static void main(String[] args) {
        SpringApplication.run(CommunityApplication.class, args);
    }
}
```

`src/main/resources/application.yml`:
```yaml
server:
  port: 8085
spring:
  application:
    name: community-service
  datasource:
    url: jdbc:mysql://localhost:3306/community
    username: root
    password: rootpw
  jpa:
    hibernate:
      ddl-auto: update
    open-in-view: false
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
community:
  topics:
    post-created: post.created
    comment-created: comment.created
    mood-logged: mood.logged
management:
  endpoints:
    web:
      exposure:
        include: health
```

- [ ] **Step 5: 컴파일 확인**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS. (Java 1.8로 컴파일됨 — `mvn -version`에서 JDK 8 사용 확인.)

- [ ] **Step 6: 커밋**

```bash
git add -A
git commit -m "chore: scaffold community-service (buildable empty app)"
```

---

## Task 2: 결정론 빈 (Clock, IdGenerator) — TDD

**Files:**
- Create: `id/IdGenerator.java`, `id/UuidIdGenerator.java`, `config/DeterminismConfig.java`
- Test: `src/test/java/com/tainted/community/id/UuidIdGeneratorTest.java`

- [ ] **Step 1: 실패하는 단위 테스트 작성**

`src/test/java/com/tainted/community/id/UuidIdGeneratorTest.java`:
```java
package com.tainted.community.id;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class UuidIdGeneratorTest {

    private final UuidIdGenerator generator = new UuidIdGenerator();

    @Test
    void newIdIsNonBlank() {
        String id = generator.newId();
        assertNotNull(id);
        assertFalse(id.trim().isEmpty());
    }

    @Test
    void newIdIsUnique() {
        assertNotEquals(generator.newId(), generator.newId());
    }
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `mvn -q -Dtest=UuidIdGeneratorTest test`
Expected: FAIL — `UuidIdGenerator` 미존재로 컴파일 에러.

- [ ] **Step 3: `IdGenerator` 인터페이스**

`src/main/java/com/tainted/community/id/IdGenerator.java`:
```java
package com.tainted.community.id;

/** 식별자 생성 추상화. 테스트에서 결정론적 구현으로 교체 가능. */
public interface IdGenerator {
    String newId();
}
```

- [ ] **Step 4: 기본 UUID 구현 (평범한 클래스)**

`src/main/java/com/tainted/community/id/UuidIdGenerator.java`:
```java
package com.tainted.community.id;

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

- [ ] **Step 5: Clock 빈**

`src/main/java/com/tainted/community/config/DeterminismConfig.java`:
```java
package com.tainted.community.config;

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

- [ ] **Step 6: 테스트 통과 + 커밋**

Run: `mvn -q -Dtest=UuidIdGeneratorTest test`
Expected: PASS (2 tests).
```bash
git add -A
git commit -m "feat: add injectable Clock and IdGenerator beans"
```

---

## Task 3: 도메인 엔티티 (JPA, javax.persistence)

**Files:**
- Create: `domain/Post.java`, `domain/PostRepository.java`, `domain/Comment.java`, `domain/CommentRepository.java`

> ⚠️ `record` 미사용 — 모든 엔티티는 평범한 클래스에 보호된 기본 생성자 + 명시 생성자 + 게터를 둔다. import 는 `javax.persistence.*`(NOT `jakarta.*`).

- [ ] **Step 1: `Post` 엔티티**

`src/main/java/com/tainted/community/domain/Post.java`:
```java
package com.tainted.community.domain;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.time.Instant;

@Entity
@Table(name = "post")
public class Post {

    @Id
    @Column(length = 64)
    private String id;

    @Column(name = "user_id", nullable = false, length = 64)
    private String userId;

    @Column(nullable = false, length = 200)
    private String title;

    /** relationship/career/family/mental/daily */
    @Column(nullable = false, length = 32)
    private String category;

    @Column(nullable = false, length = 4000)
    private String content;

    @Column(name = "mood_emoji", length = 32)
    private String moodEmoji;

    @Column(nullable = false, length = 64)
    private String nickname;

    @Column(nullable = false)
    private int likes;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected Post() {
    }

    public Post(String id, String userId, String title, String category, String content,
                String moodEmoji, String nickname, int likes, Instant createdAt) {
        this.id = id;
        this.userId = userId;
        this.title = title;
        this.category = category;
        this.content = content;
        this.moodEmoji = moodEmoji;
        this.nickname = nickname;
        this.likes = likes;
        this.createdAt = createdAt;
    }

    public String getId() { return id; }
    public String getUserId() { return userId; }
    public String getTitle() { return title; }
    public String getCategory() { return category; }
    public String getContent() { return content; }
    public String getMoodEmoji() { return moodEmoji; }
    public String getNickname() { return nickname; }
    public int getLikes() { return likes; }
    public Instant getCreatedAt() { return createdAt; }

    public void incrementLikes() {
        this.likes = this.likes + 1;
    }
}
```

- [ ] **Step 2: `PostRepository`**

`src/main/java/com/tainted/community/domain/PostRepository.java`:
```java
package com.tainted.community.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface PostRepository extends JpaRepository<Post, String> {
    List<Post> findAllByOrderByCreatedAtDesc();
}
```

- [ ] **Step 3: `Comment` 엔티티**

`src/main/java/com/tainted/community/domain/Comment.java`:
```java
package com.tainted.community.domain;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.time.Instant;

@Entity
@Table(name = "comment")
public class Comment {

    @Id
    @Column(length = 64)
    private String id;

    @Column(name = "post_id", nullable = false, length = 64)
    private String postId;

    @Column(nullable = false, length = 64)
    private String author;

    /** empathic/analytical/sincere/hopeful */
    @Column(nullable = false, length = 32)
    private String role;

    @Column(nullable = false, length = 4000)
    private String content;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected Comment() {
    }

    public Comment(String id, String postId, String author, String role, String content, Instant createdAt) {
        this.id = id;
        this.postId = postId;
        this.author = author;
        this.role = role;
        this.content = content;
        this.createdAt = createdAt;
    }

    public String getId() { return id; }
    public String getPostId() { return postId; }
    public String getAuthor() { return author; }
    public String getRole() { return role; }
    public String getContent() { return content; }
    public Instant getCreatedAt() { return createdAt; }
}
```

- [ ] **Step 4: `CommentRepository`**

`src/main/java/com/tainted/community/domain/CommentRepository.java`:
```java
package com.tainted.community.domain;

import org.springframework.data.jpa.repository.JpaRepository;

public interface CommentRepository extends JpaRepository<Comment, String> {
}
```

- [ ] **Step 5: 컴파일 + 커밋**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.
```bash
git add -A
git commit -m "feat: Post and Comment JPA entities and repositories"
```

---

## Task 4: Kafka 이벤트 발행자

**Files:**
- Create: `event/CommunityEventPublisher.java`

> 이벤트는 JSON 문자열로 직렬화하여 `KafkaTemplate<String,String>`으로 발행한다(추적 헤더 없음, 자기완결적). 직렬화는 Jackson `ObjectMapper`(Boot가 제공)로 한다. record 미사용 — 페이로드는 `LinkedHashMap`으로 조립한다(필드 순서 안정).

- [ ] **Step 1: 발행자 구현**

`src/main/java/com/tainted/community/event/CommunityEventPublisher.java`:
```java
package com.tainted.community.event;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tainted.community.domain.Comment;
import com.tainted.community.domain.Post;
import com.tainted.community.id.IdGenerator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * community 가 발행하는 세 이벤트(post.created / comment.created / mood.logged)를
 * JSON 문자열로 Kafka 에 발행한다. 추적 헤더 없음, 페이로드는 자기완결적.
 */
@Component
public class CommunityEventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final IdGenerator idGenerator;
    private final Clock clock;
    private final String postCreatedTopic;
    private final String commentCreatedTopic;
    private final String moodLoggedTopic;

    public CommunityEventPublisher(KafkaTemplate<String, String> kafkaTemplate,
                                   ObjectMapper objectMapper,
                                   IdGenerator idGenerator,
                                   Clock clock,
                                   @Value("${community.topics.post-created}") String postCreatedTopic,
                                   @Value("${community.topics.comment-created}") String commentCreatedTopic,
                                   @Value("${community.topics.mood-logged}") String moodLoggedTopic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.idGenerator = idGenerator;
        this.clock = clock;
        this.postCreatedTopic = postCreatedTopic;
        this.commentCreatedTopic = commentCreatedTopic;
        this.moodLoggedTopic = moodLoggedTopic;
    }

    /** post.created {eventId, postId, userId, category, moodEmoji, occurredAt} */
    public void publishPostCreated(Post post) {
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("eventId", idGenerator.newId());
        payload.put("postId", post.getId());
        payload.put("userId", post.getUserId());
        payload.put("category", post.getCategory());
        payload.put("moodEmoji", post.getMoodEmoji());
        payload.put("occurredAt", Instant.now(clock).toString());
        send(postCreatedTopic, post.getId(), payload);
    }

    /** mood.logged {eventId, userId, source, mood, score, occurredAt} (source='community', mood=moodEmoji, score=0) */
    public void publishMoodLogged(Post post) {
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("eventId", idGenerator.newId());
        payload.put("userId", post.getUserId());
        payload.put("source", "community");
        payload.put("mood", post.getMoodEmoji());
        payload.put("score", 0);
        payload.put("occurredAt", Instant.now(clock).toString());
        send(moodLoggedTopic, post.getUserId(), payload);
    }

    /** comment.created {eventId, postId, commentId, postAuthorUserId, role, occurredAt} */
    public void publishCommentCreated(Comment comment, String postAuthorUserId) {
        Map<String, Object> payload = new LinkedHashMap<String, Object>();
        payload.put("eventId", idGenerator.newId());
        payload.put("postId", comment.getPostId());
        payload.put("commentId", comment.getId());
        payload.put("postAuthorUserId", postAuthorUserId);
        payload.put("role", comment.getRole());
        payload.put("occurredAt", Instant.now(clock).toString());
        send(commentCreatedTopic, comment.getPostId(), payload);
    }

    private void send(String topic, String key, Map<String, Object> payload) {
        try {
            String json = objectMapper.writeValueAsString(payload);
            kafkaTemplate.send(topic, key, json);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize event for topic " + topic, e);
        }
    }
}
```

- [ ] **Step 2: 컴파일 + 커밋**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.
```bash
git add -A
git commit -m "feat: kafka event publisher (post.created, comment.created, mood.logged)"
```

---

## Task 5: DTO + 예외 + RFC 7807 핸들러

**Files:**
- Create: `web/dto/CreatePostRequest.java`, `web/dto/CreatePostResponse.java`, `web/dto/PostResponse.java`, `web/dto/CreateCommentRequest.java`, `web/dto/CommentResponse.java`, `web/dto/LikeResponse.java`
- Create: `error/PostNotFoundException.java`, `error/ProblemResponse.java`, `error/GlobalExceptionHandler.java`

> ⚠️ DTO 도 `record` 미사용 — 평범한 클래스 + 게터/세터(요청)·생성자(응답). 검증 애너테이션은 `javax.validation.constraints.*`.

- [ ] **Step 1: 요청 DTO 2종 (필드 + 게터/세터, javax.validation)**

`src/main/java/com/tainted/community/web/dto/CreatePostRequest.java`:
```java
package com.tainted.community.web.dto;

import javax.validation.constraints.NotBlank;

public class CreatePostRequest {

    @NotBlank
    private String userId;
    @NotBlank
    private String title;
    @NotBlank
    private String category;
    @NotBlank
    private String content;
    private String moodEmoji;
    @NotBlank
    private String nickname;

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }
    public String getCategory() { return category; }
    public void setCategory(String category) { this.category = category; }
    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }
    public String getMoodEmoji() { return moodEmoji; }
    public void setMoodEmoji(String moodEmoji) { this.moodEmoji = moodEmoji; }
    public String getNickname() { return nickname; }
    public void setNickname(String nickname) { this.nickname = nickname; }
}
```

`src/main/java/com/tainted/community/web/dto/CreateCommentRequest.java`:
```java
package com.tainted.community.web.dto;

import javax.validation.constraints.NotBlank;

public class CreateCommentRequest {

    @NotBlank
    private String author;
    @NotBlank
    private String role;
    @NotBlank
    private String content;

    public String getAuthor() { return author; }
    public void setAuthor(String author) { this.author = author; }
    public String getRole() { return role; }
    public void setRole(String role) { this.role = role; }
    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }
}
```

- [ ] **Step 2: 응답 DTO 4종 (생성자 + 게터)**

`src/main/java/com/tainted/community/web/dto/CreatePostResponse.java`:
```java
package com.tainted.community.web.dto;

public class CreatePostResponse {

    private final String postId;

    public CreatePostResponse(String postId) {
        this.postId = postId;
    }

    public String getPostId() { return postId; }
}
```

`src/main/java/com/tainted/community/web/dto/PostResponse.java`:
```java
package com.tainted.community.web.dto;

public class PostResponse {

    private final String id;
    private final String userId;
    private final String title;
    private final String category;
    private final String content;
    private final String moodEmoji;
    private final String nickname;
    private final int likes;
    private final String createdAt;

    public PostResponse(String id, String userId, String title, String category, String content,
                        String moodEmoji, String nickname, int likes, String createdAt) {
        this.id = id;
        this.userId = userId;
        this.title = title;
        this.category = category;
        this.content = content;
        this.moodEmoji = moodEmoji;
        this.nickname = nickname;
        this.likes = likes;
        this.createdAt = createdAt;
    }

    public String getId() { return id; }
    public String getUserId() { return userId; }
    public String getTitle() { return title; }
    public String getCategory() { return category; }
    public String getContent() { return content; }
    public String getMoodEmoji() { return moodEmoji; }
    public String getNickname() { return nickname; }
    public int getLikes() { return likes; }
    public String getCreatedAt() { return createdAt; }
}
```

`src/main/java/com/tainted/community/web/dto/CommentResponse.java`:
```java
package com.tainted.community.web.dto;

public class CommentResponse {

    private final String commentId;
    private final String postId;
    private final String role;

    public CommentResponse(String commentId, String postId, String role) {
        this.commentId = commentId;
        this.postId = postId;
        this.role = role;
    }

    public String getCommentId() { return commentId; }
    public String getPostId() { return postId; }
    public String getRole() { return role; }
}
```

`src/main/java/com/tainted/community/web/dto/LikeResponse.java`:
```java
package com.tainted.community.web.dto;

public class LikeResponse {

    private final String postId;
    private final int likes;

    public LikeResponse(String postId, int likes) {
        this.postId = postId;
        this.likes = likes;
    }

    public String getPostId() { return postId; }
    public int getLikes() { return likes; }
}
```

- [ ] **Step 3: 예외 + ProblemResponse POJO**

`src/main/java/com/tainted/community/error/PostNotFoundException.java`:
```java
package com.tainted.community.error;

public class PostNotFoundException extends RuntimeException {
    public PostNotFoundException(String message) {
        super(message);
    }
}
```

> Boot 2.7 에는 `ProblemDetail` 클래스가 없으므로 RFC 7807 본문을 담는 작은 POJO 를 직접 만든다.

`src/main/java/com/tainted/community/error/ProblemResponse.java`:
```java
package com.tainted.community.error;

/** RFC 7807 application/problem+json 본문. Boot 2.7 에는 내장 ProblemDetail 이 없어 직접 정의. */
public class ProblemResponse {

    private final String type;
    private final String title;
    private final int status;
    private final String detail;

    public ProblemResponse(String type, String title, int status, String detail) {
        this.type = type;
        this.title = title;
        this.status = status;
        this.detail = detail;
    }

    public String getType() { return type; }
    public String getTitle() { return title; }
    public int getStatus() { return status; }
    public String getDetail() { return detail; }
}
```

- [ ] **Step 4: RFC 7807 예외 핸들러 (Content-Type 명시)**

`src/main/java/com/tainted/community/error/GlobalExceptionHandler.java`:
```java
package com.tainted.community.error;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final MediaType PROBLEM_JSON = MediaType.valueOf("application/problem+json");

    @ExceptionHandler(PostNotFoundException.class)
    public ResponseEntity<ProblemResponse> handlePostNotFound(PostNotFoundException ex) {
        ProblemResponse body = new ProblemResponse(
                "about:blank", "Post not found", HttpStatus.NOT_FOUND.value(), ex.getMessage());
        return ResponseEntity.status(HttpStatus.NOT_FOUND).contentType(PROBLEM_JSON).body(body);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ProblemResponse> handleValidation(MethodArgumentNotValidException ex) {
        String detail = ex.getBindingResult().getAllErrors().isEmpty()
                ? "invalid request"
                : ex.getBindingResult().getAllErrors().get(0).getDefaultMessage();
        ProblemResponse body = new ProblemResponse(
                "about:blank", "Validation failed", HttpStatus.BAD_REQUEST.value(), detail);
        return ResponseEntity.status(HttpStatus.BAD_REQUEST).contentType(PROBLEM_JSON).body(body);
    }
}
```

- [ ] **Step 5: 컴파일 + 커밋**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.
```bash
git add -A
git commit -m "feat: DTOs, exceptions, RFC7807 problem+json handler"
```

---

## Task 6: 서비스 계층 (publish-after-save)

**Files:**
- Create: `service/CommunityService.java`

> ID/시간은 주입 빈으로만 만든다(결정론). 글/댓글 저장 트랜잭션이 끝난 뒤 이벤트를 발행한다. `@Transactional` 메서드 안에서 발행자 호출은 save 직후에 둔다(테스트베드 범위상 Outbox 미사용; 트랜잭션 커밋 보장은 단일 메서드 내 save→flush 후 publish 순서로 충족). lambda/stream 은 Java 8 에서 허용되므로 list 매핑에 사용한다.

- [ ] **Step 1: 서비스 구현**

`src/main/java/com/tainted/community/service/CommunityService.java`:
```java
package com.tainted.community.service;

import com.tainted.community.domain.Comment;
import com.tainted.community.domain.CommentRepository;
import com.tainted.community.domain.Post;
import com.tainted.community.domain.PostRepository;
import com.tainted.community.error.PostNotFoundException;
import com.tainted.community.event.CommunityEventPublisher;
import com.tainted.community.id.IdGenerator;
import com.tainted.community.web.dto.CommentResponse;
import com.tainted.community.web.dto.CreateCommentRequest;
import com.tainted.community.web.dto.CreatePostRequest;
import com.tainted.community.web.dto.CreatePostResponse;
import com.tainted.community.web.dto.LikeResponse;
import com.tainted.community.web.dto.PostResponse;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class CommunityService {

    private final PostRepository postRepository;
    private final CommentRepository commentRepository;
    private final CommunityEventPublisher eventPublisher;
    private final IdGenerator idGenerator;
    private final Clock clock;

    public CommunityService(PostRepository postRepository,
                            CommentRepository commentRepository,
                            CommunityEventPublisher eventPublisher,
                            IdGenerator idGenerator,
                            Clock clock) {
        this.postRepository = postRepository;
        this.commentRepository = commentRepository;
        this.eventPublisher = eventPublisher;
        this.idGenerator = idGenerator;
        this.clock = clock;
    }

    @Transactional
    public CreatePostResponse createPost(CreatePostRequest request) {
        Post post = new Post(
                idGenerator.newId(),
                request.getUserId(),
                request.getTitle(),
                request.getCategory(),
                request.getContent(),
                request.getMoodEmoji(),
                request.getNickname(),
                0,
                Instant.now(clock));
        postRepository.saveAndFlush(post);
        eventPublisher.publishPostCreated(post);
        eventPublisher.publishMoodLogged(post);
        return new CreatePostResponse(post.getId());
    }

    @Transactional(readOnly = true)
    public List<PostResponse> listPosts() {
        return postRepository.findAllByOrderByCreatedAtDesc().stream()
                .map(this::toResponse)
                .collect(Collectors.toList());
    }

    @Transactional(readOnly = true)
    public PostResponse getPost(String id) {
        Post post = postRepository.findById(id)
                .orElseThrow(() -> new PostNotFoundException("post not found: " + id));
        return toResponse(post);
    }

    @Transactional
    public CommentResponse addComment(String postId, CreateCommentRequest request) {
        Post post = postRepository.findById(postId)
                .orElseThrow(() -> new PostNotFoundException("post not found: " + postId));
        Comment comment = new Comment(
                idGenerator.newId(),
                postId,
                request.getAuthor(),
                request.getRole(),
                request.getContent(),
                Instant.now(clock));
        commentRepository.saveAndFlush(comment);
        eventPublisher.publishCommentCreated(comment, post.getUserId());
        return new CommentResponse(comment.getId(), postId, comment.getRole());
    }

    @Transactional
    public LikeResponse like(String postId) {
        Post post = postRepository.findById(postId)
                .orElseThrow(() -> new PostNotFoundException("post not found: " + postId));
        post.incrementLikes();
        postRepository.saveAndFlush(post);
        return new LikeResponse(post.getId(), post.getLikes());
    }

    private PostResponse toResponse(Post post) {
        return new PostResponse(
                post.getId(),
                post.getUserId(),
                post.getTitle(),
                post.getCategory(),
                post.getContent(),
                post.getMoodEmoji(),
                post.getNickname(),
                post.getLikes(),
                post.getCreatedAt().toString());
    }
}
```

- [ ] **Step 2: 컴파일 + 커밋**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.
```bash
git add -A
git commit -m "feat: community service with publish-after-save"
```

---

## Task 7: 컨트롤러 + OpenAPI

**Files:**
- Create: `web/CommunityController.java`, `config/OpenApiConfig.java`

- [ ] **Step 1: 컨트롤러 (`/internal/posts/*`)**

`src/main/java/com/tainted/community/web/CommunityController.java`:
```java
package com.tainted.community.web;

import com.tainted.community.service.CommunityService;
import com.tainted.community.web.dto.CommentResponse;
import com.tainted.community.web.dto.CreateCommentRequest;
import com.tainted.community.web.dto.CreatePostRequest;
import com.tainted.community.web.dto.CreatePostResponse;
import com.tainted.community.web.dto.LikeResponse;
import com.tainted.community.web.dto.PostResponse;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.util.List;

@RestController
@RequestMapping("/internal/posts")
public class CommunityController {

    private final CommunityService communityService;

    public CommunityController(CommunityService communityService) {
        this.communityService = communityService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CreatePostResponse createPost(@Valid @RequestBody CreatePostRequest request) {
        return communityService.createPost(request);
    }

    @GetMapping
    public List<PostResponse> listPosts() {
        return communityService.listPosts();
    }

    @GetMapping("/{id}")
    public PostResponse getPost(@PathVariable String id) {
        return communityService.getPost(id);
    }

    @PostMapping("/{id}/comments")
    @ResponseStatus(HttpStatus.CREATED)
    public CommentResponse addComment(@PathVariable String id,
                                      @Valid @RequestBody CreateCommentRequest request) {
        return communityService.addComment(id, request);
    }

    @PostMapping("/{id}/like")
    public LikeResponse like(@PathVariable String id) {
        return communityService.like(id);
    }
}
```

- [ ] **Step 2: OpenAPI 설정**

`src/main/java/com/tainted/community/config/OpenApiConfig.java`:
```java
package com.tainted.community.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {
    @Bean
    public OpenAPI communityOpenApi() {
        return new OpenAPI().info(new Info()
                .title("community-service API")
                .version("0.1.0")
                .description("익명 게시판 — 글/댓글/좋아요, post.created/comment.created/mood.logged 발행"));
    }
}
```

- [ ] **Step 3: 컴파일 + Swagger UI 경로 확인**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS. (springdoc 1.7.0 이므로 기동 시 Swagger UI 는 `/swagger-ui.html` 에서 제공된다.)

- [ ] **Step 4: 커밋**

```bash
git add -A
git commit -m "feat: community controller and OpenAPI config"
```

---

## Task 8: 시드 데이터 로더 (profile 'seed', idempotent)

**Files:**
- Create: `seed/SeedDataRunner.java`

> profile `seed` 일 때만 동작하는 `CommandLineRunner`. 고정 문자열 id(`seed-1`..)로 idempotent — 이미 있으면 건너뛴다. peace_of_mind 시드를 재현(닉네임 '길을잃은갈매기' 취업 고민, '폭발직전화산' 멘탈 글 등). 5개 카테고리(relationship/career/family/mental/daily) × 2~3개 = 약 12개. record/var/텍스트블록 미사용 — 평범한 배열 + for 루프.

- [ ] **Step 1: 시드 러너 구현**

`src/main/java/com/tainted/community/seed/SeedDataRunner.java`:
```java
package com.tainted.community.seed;

import com.tainted.community.domain.Post;
import com.tainted.community.domain.PostRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Instant;

/**
 * profile 'seed' 에서만 동작. 고정 id 로 idempotent — 이미 존재하면 삽입하지 않는다.
 * peace_of_mind 시드 게시글 재현. 5개 카테고리에 걸쳐 약 12개.
 */
@Component
@Profile("seed")
public class SeedDataRunner implements CommandLineRunner {

    private final PostRepository postRepository;
    private final Clock clock;

    public SeedDataRunner(PostRepository postRepository, Clock clock) {
        this.postRepository = postRepository;
        this.clock = clock;
    }

    @Override
    public void run(String... args) {
        // {id, userId, title, category, content, moodEmoji, nickname}
        String[][] seeds = new String[][] {
                {"seed-1", "user-seabird", "이직 준비가 너무 막막해요", "career",
                        "면접에서 계속 떨어지니 길을 잃은 기분이에요.", "😔", "길을잃은갈매기"},
                {"seed-2", "user-volcano", "분노를 어떻게 다스리죠", "mental",
                        "사소한 일에도 폭발 직전이라 스스로가 무서워요.", "😡", "폭발직전화산"},
                {"seed-3", "user-river", "연인과 자꾸 싸워요", "relationship",
                        "대화만 하면 다투게 됩니다. 제가 문제일까요?", "😢", "잔잔한강물"},
                {"seed-4", "user-cloud", "가족과 거리감이 느껴져요", "family",
                        "명절마다 마음이 무거워집니다.", "😟", "떠도는구름"},
                {"seed-5", "user-leaf", "오늘은 그냥 평범했어요", "daily",
                        "특별할 것 없는 하루지만 기록해 둡니다.", "🙂", "흔들리는나뭇잎"},
                {"seed-6", "user-seabird", "퇴사 후 공백기가 두려워요", "career",
                        "이력서 공백을 어떻게 설명해야 할지 모르겠어요.", "😰", "길을잃은갈매기"},
                {"seed-7", "user-volcano", "불면증이 심해졌어요", "mental",
                        "새벽까지 생각이 멈추질 않습니다.", "😪", "폭발직전화산"},
                {"seed-8", "user-river", "짝사랑을 고백할까요", "relationship",
                        "용기가 안 나서 몇 달째 망설이고 있어요.", "😶", "잔잔한강물"},
                {"seed-9", "user-cloud", "부모님 기대가 부담스러워요", "family",
                        "기대에 못 미칠까 늘 불안합니다.", "😞", "떠도는구름"},
                {"seed-10", "user-leaf", "산책하니 기분이 나아졌어요", "daily",
                        "햇살 좋은 날 걷는 게 작은 위로가 됩니다.", "😊", "흔들리는나뭇잎"},
                {"seed-11", "user-meadow", "번아웃이 온 것 같아요", "mental",
                        "아무것도 하고 싶지 않은 날이 늘었어요.", "😩", "마른들판"},
                {"seed-12", "user-meadow", "동료와의 갈등이 힘들어요", "career",
                        "협업이 매번 스트레스로 다가옵니다.", "😣", "마른들판"}
        };

        Instant now = Instant.now(clock);
        for (int i = 0; i < seeds.length; i++) {
            String[] s = seeds[i];
            String id = s[0];
            if (postRepository.existsById(id)) {
                continue;
            }
            Post post = new Post(id, s[1], s[2], s[3], s[4], s[5], s[6], 0, now);
            postRepository.save(post);
        }
    }
}
```

- [ ] **Step 2: 컴파일 + 커밋**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.
```bash
git add -A
git commit -m "feat: idempotent seed data runner (profile 'seed')"
```

---

## Task 9: 통합 테스트 (Testcontainers MySQL+Kafka + RestAssured)

**Files:**
- Test: `src/test/java/com/tainted/community/CommunityIntegrationTest.java`

> ⚠️ 이벤트 POJO 도 `record` 미사용. 발행 검증은 Testcontainers 브로커에 raw `KafkaConsumer<String,String>`을 붙여 JSON 문자열을 받고, `ObjectMapper`로 `Map<String,Object>`로 파싱해 필드를 확인한다. var/텍스트블록 미사용.

- [ ] **Step 1: 통합 테스트 작성**

`src/test/java/com/tainted/community/CommunityIntegrationTest.java`:
```java
package com.tainted.community;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class CommunityIntegrationTest {

    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.4").withDatabaseName("community");

    @Container
    static KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    private final ObjectMapper objectMapper = new ObjectMapper();

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", mysql::getJdbcUrl);
        r.add("spring.datasource.username", mysql::getUsername);
        r.add("spring.datasource.password", mysql::getPassword);
        r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @LocalServerPort
    int port;

    @BeforeEach
    void setUp() {
        RestAssured.port = port;
    }

    private KafkaConsumer<String, String> newConsumer(String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-" + topic + "-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Collections.singletonList(topic));
        return consumer;
    }

    private Map<String, Object> pollOne(KafkaConsumer<String, String> consumer) throws Exception {
        long deadline = System.currentTimeMillis() + 20000L;
        while (System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                @SuppressWarnings("unchecked")
                Map<String, Object> parsed = objectMapper.readValue(record.value(), HashMap.class);
                return parsed;
            }
        }
        return null;
    }

    @Test
    void createPostReturns201AndPublishesPostCreatedAndMoodLogged() throws Exception {
        KafkaConsumer<String, String> postConsumer = newConsumer("post.created");
        KafkaConsumer<String, String> moodConsumer = newConsumer("mood.logged");
        try {
            String postId = given().contentType(ContentType.JSON)
                    .body("{\"userId\":\"user-1\",\"title\":\"t\",\"category\":\"career\","
                            + "\"content\":\"c\",\"moodEmoji\":\"😔\",\"nickname\":\"nick\"}")
                    .when().post("/internal/posts")
                    .then().statusCode(201)
                    .body("postId", not(emptyOrNullString()))
                    .extract().path("postId");

            Map<String, Object> postEvent = pollOne(postConsumer);
            assertNotNull(postEvent, "post.created 메시지가 발행되어야 한다");
            assertEquals(postId, postEvent.get("postId"));
            assertEquals("user-1", postEvent.get("userId"));
            assertEquals("career", postEvent.get("category"));

            Map<String, Object> moodEvent = pollOne(moodConsumer);
            assertNotNull(moodEvent, "mood.logged 메시지가 발행되어야 한다");
            assertEquals("community", moodEvent.get("source"));
            assertEquals("user-1", moodEvent.get("userId"));
        } finally {
            postConsumer.close();
            moodConsumer.close();
        }
    }

    @Test
    void addCommentPublishesCommentCreatedWithPostAuthorUserId() throws Exception {
        String postId = given().contentType(ContentType.JSON)
                .body("{\"userId\":\"author-9\",\"title\":\"t\",\"category\":\"mental\","
                        + "\"content\":\"c\",\"moodEmoji\":\"😡\",\"nickname\":\"nick\"}")
                .when().post("/internal/posts")
                .then().statusCode(201)
                .extract().path("postId");

        KafkaConsumer<String, String> commentConsumer = newConsumer("comment.created");
        try {
            given().contentType(ContentType.JSON)
                    .body("{\"author\":\"공감 수현\",\"role\":\"empathic\",\"content\":\"힘내세요\"}")
                    .when().post("/internal/posts/" + postId + "/comments")
                    .then().statusCode(201)
                    .body("postId", equalTo(postId))
                    .body("role", equalTo("empathic"));

            Map<String, Object> commentEvent = pollOne(commentConsumer);
            assertNotNull(commentEvent, "comment.created 메시지가 발행되어야 한다");
            assertEquals(postId, commentEvent.get("postId"));
            assertEquals("author-9", commentEvent.get("postAuthorUserId"));
            assertEquals("empathic", commentEvent.get("role"));
        } finally {
            commentConsumer.close();
        }
    }

    @Test
    void likeIncrementsAndGetReflectsIt() {
        String postId = given().contentType(ContentType.JSON)
                .body("{\"userId\":\"user-2\",\"title\":\"t\",\"category\":\"daily\","
                        + "\"content\":\"c\",\"moodEmoji\":\"🙂\",\"nickname\":\"nick\"}")
                .when().post("/internal/posts")
                .then().statusCode(201)
                .extract().path("postId");

        given().when().post("/internal/posts/" + postId + "/like")
                .then().statusCode(200)
                .body("likes", equalTo(1));

        given().when().get("/internal/posts/" + postId)
                .then().statusCode(200)
                .body("likes", greaterThanOrEqualTo(1));
    }
}
```

- [ ] **Step 2: 전체 테스트 실행 (Docker 필요)**

Run: `mvn -q test`
Expected: PASS — `UuidIdGeneratorTest`(2) + `CommunityIntegrationTest`(3) 모두 통과. (Docker 데몬이 떠 있어야 Testcontainers 가 MySQL+Kafka 컨테이너를 띄운다.)

- [ ] **Step 3: 커밋**

```bash
git add -A
git commit -m "test: Testcontainers(MySQL+Kafka) + RestAssured integration tests"
```

---

## Task 10: Dockerfile + docker 프로파일

**Files:**
- Create: `Dockerfile`, `src/main/resources/application-docker.yml`

- [ ] **Step 1: docker 프로파일 설정 (compose 네트워크 호스트명)**

> `${MYSQL_ROOT_PASSWORD:rootpw}` 값은 Task 11에서 compose `community` 서비스의 `environment:` 로 주입된다(미주입 시 기본값 `rootpw`). Kafka 는 compose 내부 리스너 `kafka:29092` 를 사용한다.

`src/main/resources/application-docker.yml`:
```yaml
spring:
  datasource:
    url: jdbc:mysql://mysql:3306/community
    username: root
    password: ${MYSQL_ROOT_PASSWORD:rootpw}
  kafka:
    bootstrap-servers: kafka:29092
```

- [ ] **Step 2: 멀티스테이지 Dockerfile (non-root, JDK 8)**

`Dockerfile`:
```dockerfile
# build
FROM maven:3.9-eclipse-temurin-8 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B -DskipTests package

# run
FROM eclipse-temurin:8-jre
WORKDIR /app
# curl: compose/k8s 헬스체크가 /actuator/health 를 호출하는 데 사용.
# temurin JRE 기본 이미지에는 curl 이 없으므로 명시적으로 설치한다.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd -r -u 1001 appuser
COPY --from=build /app/target/community-service-0.1.0.jar app.jar
USER appuser
EXPOSE 8085
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 3: 이미지 빌드 확인**

Run: `docker build -t tainted-spring/community:0.1.0 .`
Expected: 빌드 성공, 최종 이미지 생성(`community-service-0.1.0.jar` 포함).

- [ ] **Step 4: 커밋**

```bash
git add -A
git commit -m "build: Dockerfile and docker profile"
```

---

## Task 11: platform 레포에 community 배선 (compose + k8s 스켈레톤)

> ⚠️ **작업 레포 전환**: 이 Task의 모든 파일 수정·커밋은 `tainted-spring-community` 가 아니라 **`tainted-spring-platform` 레포**에서 한다. 각 Step의 `cd` 경로를 확인할 것.

**Files (다른 레포 `tainted-spring-platform`):**
- Modify: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-platform/docker-compose.yml`
- Create: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-platform/k8s/community/deployment.yaml`

- [ ] **Step 1: compose에 community 서비스 추가**

`tainted-spring-platform/docker-compose.yml` 의 `services:` 아래에 추가 (앵커 `*java-service` 재사용):
```yaml
  community:
    <<: *java-service
    build:
      context: ../tainted-spring-community
    depends_on:
      mysql:
        condition: service_healthy
      kafka:
        condition: service_healthy
    environment:
      JAVA_TOOL_OPTIONS: "${JAVA_TOOL_OPTIONS:-}"
      SPRING_PROFILES_ACTIVE: "docker"
      MYSQL_ROOT_PASSWORD: "${MYSQL_ROOT_PASSWORD:-rootpw}"
    ports:
      - "8085:8085"
    healthcheck:
      # 런타임 이미지에 설치한 curl 사용. -f: 비2xx 응답이면 실패 → actuator DOWN 시 unhealthy.
      test: ["CMD", "curl", "-fsS", "http://localhost:8085/actuator/health"]
      interval: 15s
      timeout: 5s
      retries: 20
      start_period: 40s
```

> 참고: `<<: *java-service` 가 `environment` 를 병합하지 않고 덮어쓰므로, 위에서 `JAVA_TOOL_OPTIONS`/`SPRING_PROFILES_ACTIVE`/`MYSQL_ROOT_PASSWORD` 를 명시적으로 다시 적었다.

- [ ] **Step 2: compose로 통합 기동 검증**

Run (platform 레포에서):
```bash
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-platform
docker compose up -d --build --wait community
curl -s http://localhost:8085/actuator/health
curl -s -X POST http://localhost:8085/internal/posts \
  -H 'Content-Type: application/json' \
  -d '{"userId":"user-1","title":"t","category":"career","content":"c","moodEmoji":"😔","nickname":"nick"}'
```
Expected: `--wait` 가 community 헬스체크 통과까지 블록. health `{"status":"UP"}`, 글 작성 응답에 `postId` 포함(201).

- [ ] **Step 3: k8s Deployment/Service 스켈레톤**

`tainted-spring-platform/k8s/community/deployment.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tainted-spring-community
  namespace: tainted-spring
spec:
  replicas: 1
  selector:
    matchLabels: { app: community }
  template:
    metadata:
      labels: { app: community }
    spec:
      containers:
        - name: community
          image: tainted-spring/community:0.1.0
          ports: [{ containerPort: 8085 }]
          env:
            - { name: SPRING_PROFILES_ACTIVE, value: "eks" }
            - { name: JAVA_TOOL_OPTIONS, value: "" }
          readinessProbe:
            httpGet: { path: /actuator/health, port: 8085 }
            initialDelaySeconds: 20
            periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: tainted-spring-community
  namespace: tainted-spring
spec:
  selector: { app: community }
  ports:
    - { port: 80, targetPort: 8085 }
```

- [ ] **Step 4: platform 레포 커밋**

```bash
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-platform
git add -A
git commit -m "feat: wire community service into compose and k8s skeleton"
```

---

## Self-Review

**Spec coverage (설계문서 §3.5 / §4 대비):**
- 익명 게시판 글/댓글/좋아요 → Task 3 엔티티 + Task 6 서비스 + Task 7 컨트롤러 ✔
- `Post{id,userId,title,category,content,moodEmoji,nickname,likes,createdAt}` → Task 3 `Post`(평범한 클래스, javax.persistence) ✔
- `Comment{id,postId,author,role,content,createdAt}` → Task 3 `Comment` ✔
- 내부 API `POST/GET /internal/posts`, `GET /internal/posts/{id}`, `POST .../comments`, `POST .../like` → Task 7 ✔
- `post.created` + `mood.logged`(source='community', mood=moodEmoji, score=0) 발행 on create → Task 4·6 ✔
- `comment.created` 발행 시 `postAuthorUserId` = 글의 userId → Task 4·6(`addComment`에서 post.getUserId() 전달) ✔
- 이벤트 JSON, 추적 헤더 없음, 자기완결적, spring-kafka → Task 4 ✔
- 결정론(Clock/IdGenerator) → Task 2, 서비스/발행자에서 주입 사용 ✔
- 시드 데이터(profile 'seed', idempotent 고정 id, 5카테고리 ~12개, '길을잃은갈매기'/'폭발직전화산') → Task 8 ✔
- OpenAPI 노출(springdoc 1.x, /swagger-ui.html) → Task 7 ✔
- RFC 7807 problem+json (Boot 2.7 — ProblemResponse POJO + Content-Type 명시) → Task 5 ✔
- `/actuator/health` 유지, 추적 라이브러리 없음 → pom.xml에 actuator만, OTel/Sleuth/Micrometer Tracing 미포함 ✔
- Docker(멀티스테이지/non-root, JDK 8 베이스) + docker 프로파일 DNS(mysql:3306, kafka:29092) → Task 10 ✔
- compose 배선(빈 JAVA_TOOL_OPTIONS 슬롯) + k8s 스켈레톤, 포트 8085 → Task 11 ✔
- 통합 테스트(Testcontainers MySQL+Kafka, RestAssured, raw KafkaConsumer로 세 이벤트 검증) → Task 9 ✔

**Java 1.8 / Boot 2.7 제약 점검:**
- `record` 미사용 — 모든 엔티티/DTO/이벤트 페이로드는 평범한 클래스 또는 Map. ✔
- `var` 미사용, 텍스트블록/switch식/패턴매칭 미사용. lambda/stream 만 사용(Task 6 list 매핑). ✔
- `javax.persistence`·`javax.validation` 사용, `jakarta.*` 미사용. ✔
- `ProblemDetail` 미사용 — `ProblemResponse` POJO + `@RestControllerAdvice` + `ResponseEntity` + `application/problem+json` 명시. ✔
- springdoc `org.springdoc:springdoc-openapi-ui:1.7.0`(1.x), Swagger UI `/swagger-ui.html`. ✔
- MySQL 드라이버 `mysql:mysql-connector-java`(NOT `com.mysql:mysql-connector-j`). ✔
- spring-kafka/rest-assured 버전은 Boot 2.7.18 parent 관리(미명시). Testcontainers 1.20.3, MySQLContainer(mysql:8.4) + KafkaContainer. ✔
- parent `spring-boot-starter-parent:2.7.18`, `<java.version>1.8</java.version>`. ✔

**Placeholder scan:** 모든 코드 step에 완전한 소스, 모든 명령에 기대출력 명시. 플레이스홀더 없음. ✔

**Type/이름 일관성:**
- `IdGenerator.newId()` — Task 2 정의, Task 4·6·8 사용 일치 ✔
- `Clock` 빈 — Task 2 정의, Task 4·6·8 주입 일치 ✔
- `CommunityEventPublisher.publishPostCreated/publishMoodLogged/publishCommentCreated(Comment,String)` — Task 4 정의, Task 6 호출 시그니처 일치 ✔
- `Post.incrementLikes()` — Task 3 정의, Task 6 `like()` 사용 일치 ✔
- `PostRepository.findAllByOrderByCreatedAtDesc()` — Task 3 정의, Task 6 사용 일치 ✔
- 이벤트 JSON 필드(post.created: eventId/postId/userId/category/moodEmoji/occurredAt; comment.created: eventId/postId/commentId/postAuthorUserId/role/occurredAt; mood.logged: eventId/userId/source/mood/score/occurredAt) — Task 4 정의, Task 9 KafkaConsumer 검증 키와 일치 ✔
- DTO 필드명(`postId`,`likes`,`role`) — Task 5 정의, Task 9 RestAssured 검증 키와 일치 ✔
- jar 파일명 `community-service-0.1.0.jar` — pom `artifactId`+`version` 과 Dockerfile COPY 일치 ✔
- 포트 8085 — application.yml / Dockerfile EXPOSE / compose / k8s 일치 ✔
