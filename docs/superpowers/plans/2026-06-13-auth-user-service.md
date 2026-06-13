# tainted-spring-auth-user Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 소셜/게스트 로그인, 토큰 발급·검증(introspection), 익명 사용자 조회를 제공하는 `auth-user-service`(Java 17 · Maven · Spring Boot 3.3 · MySQL + Redis)를 RestAssured/Testcontainers로 검증 가능하게 구현한다.

**Architecture:** Spring MVC REST 서비스. 소셜 검증은 결정론적 `MockSocialVerifier`로 대체(외부 호출 없음). 사용자 계정은 MySQL(`authuser` DB)에 저장, 발급 토큰은 Redis(논리 DB 0)에 TTL과 함께 저장. 표시명은 항상 `"익명"`. 시간/ID는 주입 가능한 `Clock`/`IdGenerator` 빈으로 결정론 확보. 추적/관측 라이브러리는 넣지 않음(actuator health만 유지).

**Tech Stack:** Java 17, Maven, Spring Boot 3.3.5, Spring Data JPA, Spring Data Redis, springdoc-openapi 2.6, MySQL 8.4, Redis 7, Testcontainers, RestAssured.

---

## 사전 조건

- `tainted-spring-platform` 계획이 완료되어 있어야 통합 실행이 쉽다(단위/통합 테스트는 Testcontainers로 자체 인프라를 띄우므로 platform 없이도 테스트는 통과한다).
- **레포 위치**: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-auth-user/` (모든 경로는 이 루트 기준).
- **포트**: 8081. **패키지 루트**: `com.tainted.authuser`. **Redis 논리 DB**: 0.

## File Structure

```
pom.xml
Dockerfile
src/main/resources/application.yml
src/main/resources/application-docker.yml
src/main/java/com/tainted/authuser/
  AuthUserApplication.java
  config/DeterminismConfig.java        # Clock 빈
  config/OpenApiConfig.java
  id/IdGenerator.java                  # 인터페이스
  id/UuidIdGenerator.java              # 기본 구현
  social/SocialVerifier.java           # 인터페이스
  social/MockSocialVerifier.java       # 결정론적 구현
  social/ExternalIdentity.java         # 검증 결과
  domain/UserAccount.java              # JPA 엔티티
  domain/UserAccountRepository.java
  token/TokenService.java              # Redis 토큰 발급/조회
  service/AuthService.java             # 비즈니스 로직
  web/AuthController.java              # /api/v1/auth/*
  web/MeController.java                # /api/v1/me
  web/InternalAuthController.java      # /internal/*
  web/dto/...                          # 요청/응답 DTO
  error/InvalidTokenException.java
  error/UserNotFoundException.java
  error/GlobalExceptionHandler.java    # RFC 7807
src/test/java/com/tainted/authuser/
  social/MockSocialVerifierTest.java   # 단위
  AuthIntegrationTest.java             # Testcontainers + RestAssured
```

---

## Task 1: 레포 스캐폴드 + 빌드 가능한 빈 앱

**Files:**
- Create: `pom.xml`, `src/main/java/com/tainted/authuser/AuthUserApplication.java`, `src/main/resources/application.yml`, `.gitignore`

- [ ] **Step 1: 디렉토리 생성 + git init**

Run:
```bash
mkdir -p /Users/changjoonbaek/workspace_claude-code/tainted-spring-auth-user
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-auth-user
git init
mkdir -p src/main/java/com/tainted/authuser src/main/resources src/test/java/com/tainted/authuser
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
    <version>3.3.5</version>
    <relativePath/>
  </parent>
  <groupId>com.tainted</groupId>
  <artifactId>auth-user-service</artifactId>
  <version>0.1.0</version>
  <properties>
    <java.version>17</java.version>
  </properties>
  <dependencies>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-web</artifactId></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-data-jpa</artifactId></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-data-redis</artifactId></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-validation</artifactId></dependency>
    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-actuator</artifactId></dependency>
    <dependency><groupId>com.mysql</groupId><artifactId>mysql-connector-j</artifactId><scope>runtime</scope></dependency>
    <dependency><groupId>org.springdoc</groupId><artifactId>springdoc-openapi-starter-webmvc-ui</artifactId><version>2.6.0</version></dependency>

    <dependency><groupId>org.springframework.boot</groupId><artifactId>spring-boot-starter-test</artifactId><scope>test</scope></dependency>
    <dependency><groupId>org.testcontainers</groupId><artifactId>junit-jupiter</artifactId><version>1.20.3</version><scope>test</scope></dependency>
    <dependency><groupId>org.testcontainers</groupId><artifactId>mysql</artifactId><version>1.20.3</version><scope>test</scope></dependency>
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

`src/main/java/com/tainted/authuser/AuthUserApplication.java`:
```java
package com.tainted.authuser;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AuthUserApplication {
    public static void main(String[] args) {
        SpringApplication.run(AuthUserApplication.class, args);
    }
}
```

`src/main/resources/application.yml`:
```yaml
server:
  port: 8081
spring:
  application:
    name: auth-user-service
  datasource:
    url: jdbc:mysql://localhost:3306/authuser
    username: root
    password: rootpw
  jpa:
    hibernate:
      ddl-auto: update
    open-in-view: false
  data:
    redis:
      host: localhost
      port: 6379
      database: 0
auth:
  token-ttl-seconds: 3600
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
git commit -m "chore: scaffold auth-user-service (buildable empty app)"
```

---

## Task 2: 결정론 빈 (Clock, IdGenerator)

**Files:**
- Create: `id/IdGenerator.java`, `id/UuidIdGenerator.java`, `config/DeterminismConfig.java`

- [ ] **Step 1: `IdGenerator` 인터페이스**

`src/main/java/com/tainted/authuser/id/IdGenerator.java`:
```java
package com.tainted.authuser.id;

/** 식별자 생성 추상화. 테스트에서 결정론적 구현으로 교체 가능. */
public interface IdGenerator {
    String newId();
}
```

- [ ] **Step 2: 기본 UUID 구현**

`src/main/java/com/tainted/authuser/id/UuidIdGenerator.java`:
```java
package com.tainted.authuser.id;

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

`src/main/java/com/tainted/authuser/config/DeterminismConfig.java`:
```java
package com.tainted.authuser.config;

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

## Task 3: 결정론적 소셜 검증 (TDD)

**Files:**
- Create: `social/ExternalIdentity.java`, `social/SocialVerifier.java`, `social/MockSocialVerifier.java`, `error/InvalidTokenException.java`
- Test: `src/test/java/com/tainted/authuser/social/MockSocialVerifierTest.java`

- [ ] **Step 1: 실패하는 테스트 작성**

`src/test/java/com/tainted/authuser/social/MockSocialVerifierTest.java`:
```java
package com.tainted.authuser.social;

import com.tainted.authuser.error.InvalidTokenException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MockSocialVerifierTest {

    private final MockSocialVerifier verifier = new MockSocialVerifier();

    @Test
    void validTokenYieldsDeterministicExternalId() {
        ExternalIdentity a = verifier.verify("kakao", "valid-kakao-u1");
        ExternalIdentity b = verifier.verify("kakao", "valid-kakao-u1");
        assertEquals("kakao", a.provider());
        assertEquals("kakao:u1", a.externalId());
        assertEquals(a.externalId(), b.externalId(), "같은 입력은 같은 externalId");
    }

    @Test
    void unsupportedProviderRejected() {
        assertThrows(InvalidTokenException.class,
                () -> verifier.verify("google", "valid-google-u1"));
    }

    @Test
    void malformedTokenRejected() {
        assertThrows(InvalidTokenException.class,
                () -> verifier.verify("kakao", "garbage"));
    }
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `mvn -q -Dtest=MockSocialVerifierTest test`
Expected: FAIL — `MockSocialVerifier`, `ExternalIdentity`, `InvalidTokenException` 미존재로 컴파일 에러.

- [ ] **Step 3: 최소 구현**

`src/main/java/com/tainted/authuser/error/InvalidTokenException.java`:
```java
package com.tainted.authuser.error;

public class InvalidTokenException extends RuntimeException {
    public InvalidTokenException(String message) {
        super(message);
    }
}
```

`src/main/java/com/tainted/authuser/social/ExternalIdentity.java`:
```java
package com.tainted.authuser.social;

public record ExternalIdentity(String provider, String externalId) {}
```

`src/main/java/com/tainted/authuser/social/SocialVerifier.java`:
```java
package com.tainted.authuser.social;

public interface SocialVerifier {
    /** providerToken 을 검증하고 외부 신원을 반환. 실패 시 InvalidTokenException. */
    ExternalIdentity verify(String provider, String providerToken);
}
```

`src/main/java/com/tainted/authuser/social/MockSocialVerifier.java`:
```java
package com.tainted.authuser.social;

import com.tainted.authuser.error.InvalidTokenException;
import org.springframework.stereotype.Component;
import java.util.Set;

/**
 * 결정론적 소셜 검증 mock. 외부 호출 없음.
 * 유효 토큰 형식: "valid-<provider>-<seed>"  →  externalId = "<provider>:<seed>".
 */
@Component
public class MockSocialVerifier implements SocialVerifier {

    private static final Set<String> SUPPORTED = Set.of("kakao", "naver", "toss");

    @Override
    public ExternalIdentity verify(String provider, String providerToken) {
        if (!SUPPORTED.contains(provider)) {
            throw new InvalidTokenException("unsupported provider: " + provider);
        }
        String prefix = "valid-" + provider + "-";
        if (providerToken == null || !providerToken.startsWith(prefix)) {
            throw new InvalidTokenException("malformed token for provider: " + provider);
        }
        String seed = providerToken.substring(prefix.length());
        if (seed.isBlank()) {
            throw new InvalidTokenException("empty seed");
        }
        return new ExternalIdentity(provider, provider + ":" + seed);
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `mvn -q -Dtest=MockSocialVerifierTest test`
Expected: PASS (3 tests).

- [ ] **Step 5: 커밋**

```bash
git add -A
git commit -m "feat: deterministic mock social verifier"
```

---

## Task 4: 사용자 계정 도메인 (JPA)

**Files:**
- Create: `domain/UserAccount.java`, `domain/UserAccountRepository.java`

- [ ] **Step 1: 엔티티**

`src/main/java/com/tainted/authuser/domain/UserAccount.java`:
```java
package com.tainted.authuser.domain;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "user_account",
       uniqueConstraints = @UniqueConstraint(columnNames = {"social_provider", "external_id"}))
public class UserAccount {

    @Id
    @Column(length = 64)
    private String id;

    @Column(name = "social_provider", nullable = false, length = 32)
    private String socialProvider;

    @Column(name = "external_id", nullable = false, length = 128)
    private String externalId;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected UserAccount() {}

    public UserAccount(String id, String socialProvider, String externalId, Instant createdAt) {
        this.id = id;
        this.socialProvider = socialProvider;
        this.externalId = externalId;
        this.createdAt = createdAt;
    }

    public String getId() { return id; }
    public String getSocialProvider() { return socialProvider; }
    public String getExternalId() { return externalId; }
    public Instant getCreatedAt() { return createdAt; }
}
```

- [ ] **Step 2: 리포지토리**

`src/main/java/com/tainted/authuser/domain/UserAccountRepository.java`:
```java
package com.tainted.authuser.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;

public interface UserAccountRepository extends JpaRepository<UserAccount, String> {
    Optional<UserAccount> findBySocialProviderAndExternalId(String socialProvider, String externalId);
}
```

- [ ] **Step 3: 컴파일 + 커밋**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.
```bash
git add -A
git commit -m "feat: UserAccount entity and repository"
```

---

## Task 5: 토큰 서비스 (Redis)

**Files:**
- Create: `token/TokenService.java`

- [ ] **Step 1: 토큰 서비스 구현**

`src/main/java/com/tainted/authuser/token/TokenService.java`:
```java
package com.tainted.authuser.token;

import com.tainted.authuser.id.IdGenerator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import java.time.Duration;
import java.util.Optional;

@Service
public class TokenService {

    private static final String PREFIX = "auth:token:";

    private final StringRedisTemplate redis;
    private final IdGenerator idGenerator;
    private final long ttlSeconds;

    public TokenService(StringRedisTemplate redis,
                        IdGenerator idGenerator,
                        @Value("${auth.token-ttl-seconds}") long ttlSeconds) {
        this.redis = redis;
        this.idGenerator = idGenerator;
        this.ttlSeconds = ttlSeconds;
    }

    /** userId 에 대한 새 토큰을 발급하고 Redis 에 TTL 과 함께 저장. */
    public String issue(String userId) {
        String token = idGenerator.newId();
        redis.opsForValue().set(PREFIX + token, userId, Duration.ofSeconds(ttlSeconds));
        return token;
    }

    /** 토큰에 매핑된 userId 를 조회. 없거나 만료면 empty. */
    public Optional<String> resolveUserId(String token) {
        if (token == null || token.isBlank()) {
            return Optional.empty();
        }
        return Optional.ofNullable(redis.opsForValue().get(PREFIX + token));
    }
}
```

- [ ] **Step 2: 컴파일 + 커밋**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.
```bash
git add -A
git commit -m "feat: redis-backed token service"
```

---

## Task 6: 인증 서비스 + DTO

**Files:**
- Create: `web/dto/LoginRequest.java`, `web/dto/LoginResponse.java`, `web/dto/MeResponse.java`, `web/dto/VerifyRequest.java`, `web/dto/VerifyResponse.java`, `web/dto/UserResponse.java`
- Create: `error/UserNotFoundException.java`, `service/AuthService.java`

- [ ] **Step 1: DTO 6종**

`web/dto/LoginRequest.java`:
```java
package com.tainted.authuser.web.dto;

import jakarta.validation.constraints.NotBlank;

public record LoginRequest(@NotBlank String provider, @NotBlank String providerToken) {}
```

`web/dto/LoginResponse.java`:
```java
package com.tainted.authuser.web.dto;

public record LoginResponse(String accessToken, String displayName) {}
```

`web/dto/MeResponse.java`:
```java
package com.tainted.authuser.web.dto;

public record MeResponse(String userId, String displayName, String socialProvider) {}
```

`web/dto/VerifyRequest.java`:
```java
package com.tainted.authuser.web.dto;

import jakarta.validation.constraints.NotBlank;

public record VerifyRequest(@NotBlank String token) {}
```

`web/dto/VerifyResponse.java`:
```java
package com.tainted.authuser.web.dto;

public record VerifyResponse(boolean active, String userId, String socialProvider) {}
```

`web/dto/UserResponse.java`:
```java
package com.tainted.authuser.web.dto;

public record UserResponse(String id, String socialProvider, String createdAt) {}
```

- [ ] **Step 2: `UserNotFoundException`**

`src/main/java/com/tainted/authuser/error/UserNotFoundException.java`:
```java
package com.tainted.authuser.error;

public class UserNotFoundException extends RuntimeException {
    public UserNotFoundException(String message) {
        super(message);
    }
}
```

- [ ] **Step 3: `AuthService`**

`src/main/java/com/tainted/authuser/service/AuthService.java`:
```java
package com.tainted.authuser.service;

import com.tainted.authuser.domain.UserAccount;
import com.tainted.authuser.domain.UserAccountRepository;
import com.tainted.authuser.error.InvalidTokenException;
import com.tainted.authuser.error.UserNotFoundException;
import com.tainted.authuser.id.IdGenerator;
import com.tainted.authuser.social.ExternalIdentity;
import com.tainted.authuser.social.SocialVerifier;
import com.tainted.authuser.token.TokenService;
import com.tainted.authuser.web.dto.*;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.Clock;
import java.time.Instant;

@Service
public class AuthService {

    public static final String DISPLAY_NAME = "익명";

    private final UserAccountRepository repository;
    private final SocialVerifier socialVerifier;
    private final TokenService tokenService;
    private final IdGenerator idGenerator;
    private final Clock clock;

    public AuthService(UserAccountRepository repository, SocialVerifier socialVerifier,
                       TokenService tokenService, IdGenerator idGenerator, Clock clock) {
        this.repository = repository;
        this.socialVerifier = socialVerifier;
        this.tokenService = tokenService;
        this.idGenerator = idGenerator;
        this.clock = clock;
    }

    @Transactional
    public LoginResponse login(LoginRequest request) {
        ExternalIdentity identity = socialVerifier.verify(request.provider(), request.providerToken());
        UserAccount user = repository
                .findBySocialProviderAndExternalId(identity.provider(), identity.externalId())
                .orElseGet(() -> repository.save(new UserAccount(
                        idGenerator.newId(), identity.provider(), identity.externalId(), Instant.now(clock))));
        String token = tokenService.issue(user.getId());
        return new LoginResponse(token, DISPLAY_NAME);
    }

    @Transactional
    public LoginResponse guest() {
        UserAccount user = repository.save(new UserAccount(
                idGenerator.newId(), "guest", "guest:" + idGenerator.newId(), Instant.now(clock)));
        String token = tokenService.issue(user.getId());
        return new LoginResponse(token, DISPLAY_NAME);
    }

    @Transactional(readOnly = true)
    public MeResponse me(String token) {
        String userId = tokenService.resolveUserId(token)
                .orElseThrow(() -> new InvalidTokenException("invalid or expired token"));
        UserAccount user = repository.findById(userId)
                .orElseThrow(() -> new UserNotFoundException("user not found: " + userId));
        return new MeResponse(user.getId(), DISPLAY_NAME, user.getSocialProvider());
    }

    @Transactional(readOnly = true)
    public VerifyResponse verify(String token) {
        return tokenService.resolveUserId(token)
                .flatMap(repository::findById)
                .map(u -> new VerifyResponse(true, u.getId(), u.getSocialProvider()))
                .orElse(new VerifyResponse(false, null, null));
    }

    @Transactional(readOnly = true)
    public UserResponse getUser(String id) {
        UserAccount user = repository.findById(id)
                .orElseThrow(() -> new UserNotFoundException("user not found: " + id));
        return new UserResponse(user.getId(), user.getSocialProvider(), user.getCreatedAt().toString());
    }
}
```

- [ ] **Step 4: 컴파일 + 커밋**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.
```bash
git add -A
git commit -m "feat: auth service and DTOs"
```

---

## Task 7: 컨트롤러 + RFC 7807 에러 + OpenAPI

**Files:**
- Create: `web/AuthController.java`, `web/MeController.java`, `web/InternalAuthController.java`, `error/GlobalExceptionHandler.java`, `config/OpenApiConfig.java`
- Modify: `src/main/resources/application.yml`

- [ ] **Step 1: 컨트롤러 3종**

`src/main/java/com/tainted/authuser/web/AuthController.java`:
```java
package com.tainted.authuser.web;

import com.tainted.authuser.service.AuthService;
import com.tainted.authuser.web.dto.LoginRequest;
import com.tainted.authuser.web.dto.LoginResponse;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/auth")
public class AuthController {

    private final AuthService authService;

    public AuthController(AuthService authService) {
        this.authService = authService;
    }

    @PostMapping("/login")
    public LoginResponse login(@Valid @RequestBody LoginRequest request) {
        return authService.login(request);
    }

    @PostMapping("/guest")
    public LoginResponse guest() {
        return authService.guest();
    }
}
```

`src/main/java/com/tainted/authuser/web/MeController.java`:
```java
package com.tainted.authuser.web;

import com.tainted.authuser.error.InvalidTokenException;
import com.tainted.authuser.service.AuthService;
import com.tainted.authuser.web.dto.MeResponse;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/me")
public class MeController {

    private final AuthService authService;

    public MeController(AuthService authService) {
        this.authService = authService;
    }

    @GetMapping
    public MeResponse me(@RequestHeader(value = "Authorization", required = false) String authorization) {
        return authService.me(extractBearer(authorization));
    }

    private String extractBearer(String authorization) {
        if (authorization == null || !authorization.startsWith("Bearer ")) {
            throw new InvalidTokenException("missing bearer token");
        }
        return authorization.substring("Bearer ".length());
    }
}
```

`src/main/java/com/tainted/authuser/web/InternalAuthController.java`:
```java
package com.tainted.authuser.web;

import com.tainted.authuser.service.AuthService;
import com.tainted.authuser.web.dto.UserResponse;
import com.tainted.authuser.web.dto.VerifyRequest;
import com.tainted.authuser.web.dto.VerifyResponse;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/internal")
public class InternalAuthController {

    private final AuthService authService;

    public InternalAuthController(AuthService authService) {
        this.authService = authService;
    }

    @PostMapping("/auth/verify")
    public VerifyResponse verify(@Valid @RequestBody VerifyRequest request) {
        return authService.verify(request.token());
    }

    @GetMapping("/users/{id}")
    public UserResponse getUser(@PathVariable String id) {
        return authService.getUser(id);
    }
}
```

- [ ] **Step 2: RFC 7807 예외 핸들러**

`src/main/java/com/tainted/authuser/error/GlobalExceptionHandler.java`:
```java
package com.tainted.authuser.error;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(InvalidTokenException.class)
    public ProblemDetail handleInvalidToken(InvalidTokenException ex) {
        ProblemDetail pd = ProblemDetail.forStatus(HttpStatus.UNAUTHORIZED);
        pd.setTitle("Invalid token");
        pd.setDetail(ex.getMessage());
        return pd;
    }

    @ExceptionHandler(UserNotFoundException.class)
    public ProblemDetail handleUserNotFound(UserNotFoundException ex) {
        ProblemDetail pd = ProblemDetail.forStatus(HttpStatus.NOT_FOUND);
        pd.setTitle("User not found");
        pd.setDetail(ex.getMessage());
        return pd;
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidation(MethodArgumentNotValidException ex) {
        ProblemDetail pd = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        pd.setTitle("Validation failed");
        pd.setDetail(ex.getBindingResult().getAllErrors().isEmpty()
                ? "invalid request"
                : ex.getBindingResult().getAllErrors().get(0).getDefaultMessage());
        return pd;
    }
}
```

- [ ] **Step 3: OpenAPI 설정 + application.yml에 problemdetails 활성화**

`src/main/java/com/tainted/authuser/config/OpenApiConfig.java`:
```java
package com.tainted.authuser.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {
    @Bean
    public OpenAPI authUserOpenApi() {
        return new OpenAPI().info(new Info()
                .title("auth-user-service API")
                .version("0.1.0")
                .description("소셜/게스트 로그인, 토큰 검증, 사용자 조회"));
    }
}
```

`src/main/resources/application.yml` 의 `spring:` 블록에 추가 (datasource 위/아래 아무 곳):
```yaml
  mvc:
    problemdetails:
      enabled: true
```

- [ ] **Step 4: 컴파일 + 커밋**

Run: `mvn -q -DskipTests compile`
Expected: BUILD SUCCESS.
```bash
git add -A
git commit -m "feat: controllers, RFC7807 error handling, OpenAPI"
```

---

## Task 8: 통합 테스트 (Testcontainers + RestAssured)

**Files:**
- Test: `src/test/java/com/tainted/authuser/AuthIntegrationTest.java`

- [ ] **Step 1: 통합 테스트 작성**

`src/test/java/com/tainted/authuser/AuthIntegrationTest.java`:
```java
package com.tainted.authuser;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class AuthIntegrationTest {

    @Container
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.4").withDatabaseName("authuser");

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine").withExposedPorts(6379);

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", mysql::getJdbcUrl);
        r.add("spring.datasource.username", mysql::getUsername);
        r.add("spring.datasource.password", mysql::getPassword);
        r.add("spring.data.redis.host", redis::getHost);
        r.add("spring.data.redis.port", () -> redis.getMappedPort(6379));
    }

    @LocalServerPort
    int port;

    @BeforeEach
    void setUp() {
        RestAssured.port = port;
    }

    @Test
    void guestLoginThenMe() {
        String token = given().when().post("/api/v1/auth/guest")
                .then().statusCode(200)
                .body("displayName", equalTo("익명"))
                .body("accessToken", not(emptyOrNullString()))
                .extract().path("accessToken");

        given().header("Authorization", "Bearer " + token)
                .when().get("/api/v1/me")
                .then().statusCode(200)
                .body("displayName", equalTo("익명"))
                .body("socialProvider", equalTo("guest"))
                .body("userId", not(emptyOrNullString()));
    }

    @Test
    void socialLoginThenVerifyAndGetUser() {
        String token = given().contentType(ContentType.JSON)
                .body("{\"provider\":\"kakao\",\"providerToken\":\"valid-kakao-u1\"}")
                .when().post("/api/v1/auth/login")
                .then().statusCode(200)
                .extract().path("accessToken");

        String userId = given().contentType(ContentType.JSON)
                .body("{\"token\":\"" + token + "\"}")
                .when().post("/internal/auth/verify")
                .then().statusCode(200)
                .body("active", equalTo(true))
                .body("socialProvider", equalTo("kakao"))
                .extract().path("userId");

        given().when().get("/internal/users/" + userId)
                .then().statusCode(200)
                .body("socialProvider", equalTo("kakao"))
                .body("id", equalTo(userId));
    }

    @Test
    void invalidTokenReturnsProblemJson() {
        given().header("Authorization", "Bearer not-a-real-token")
                .when().get("/api/v1/me")
                .then().statusCode(401)
                .contentType("application/problem+json")
                .body("title", equalTo("Invalid token"));
    }

    @Test
    void verifyUnknownTokenReturnsInactive() {
        given().contentType(ContentType.JSON)
                .body("{\"token\":\"ghost\"}")
                .when().post("/internal/auth/verify")
                .then().statusCode(200)
                .body("active", equalTo(false));
    }

    @Test
    void unsupportedProviderRejectedWithProblemJson() {
        given().contentType(ContentType.JSON)
                .body("{\"provider\":\"google\",\"providerToken\":\"valid-google-u1\"}")
                .when().post("/api/v1/auth/login")
                .then().statusCode(401)
                .contentType("application/problem+json");
    }
}
```

- [ ] **Step 2: 전체 테스트 실행 (Docker 필요)**

Run: `mvn -q test`
Expected: PASS — `MockSocialVerifierTest`(3) + `AuthIntegrationTest`(5) 모두 통과. (Docker 데몬이 떠 있어야 Testcontainers 동작.)

- [ ] **Step 3: 커밋**

```bash
git add -A
git commit -m "test: Testcontainers + RestAssured integration tests"
```

---

## Task 9: Dockerfile + docker 프로파일

**Files:**
- Create: `Dockerfile`, `src/main/resources/application-docker.yml`

- [ ] **Step 1: docker 프로파일 설정 (compose 네트워크 호스트명)**

`src/main/resources/application-docker.yml`:
```yaml
spring:
  datasource:
    url: jdbc:mysql://mysql:3306/authuser
    username: root
    password: ${MYSQL_ROOT_PASSWORD:rootpw}
  data:
    redis:
      host: redis
      port: 6379
      database: 0
```

- [ ] **Step 2: 멀티스테이지 Dockerfile (non-root)**

`Dockerfile`:
```dockerfile
# build
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B -DskipTests package

# run
FROM eclipse-temurin:17-jre
WORKDIR /app
RUN useradd -r -u 1001 appuser
COPY --from=build /app/target/auth-user-service-0.1.0.jar app.jar
USER appuser
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 3: 이미지 빌드 확인**

Run: `docker build -t tainted-spring/auth-user:0.1.0 .`
Expected: 빌드 성공, 최종 이미지 생성.

- [ ] **Step 4: 커밋**

```bash
git add -A
git commit -m "build: Dockerfile and docker profile"
```

---

## Task 10: platform 레포에 auth-user 배선 (compose + k8s 스켈레톤)

**Files (다른 레포 `tainted-spring-platform`):**
- Modify: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-platform/docker-compose.yml`
- Create: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-platform/k8s/auth-user/deployment.yaml`

- [ ] **Step 1: compose에 auth-user 서비스 추가**

`tainted-spring-platform/docker-compose.yml` 의 `services:` 아래에 추가 (앵커 `*java-service` 재사용):
```yaml
  auth-user:
    <<: *java-service
    build:
      context: ../tainted-spring-auth-user
    depends_on:
      mysql:
        condition: service_healthy
      redis:
        condition: service_healthy
    environment:
      JAVA_TOOL_OPTIONS: "${JAVA_TOOL_OPTIONS:-}"
      SPRING_PROFILES_ACTIVE: "docker"
      MYSQL_ROOT_PASSWORD: "${MYSQL_ROOT_PASSWORD:-rootpw}"
    ports:
      - "8081:8081"
    healthcheck:
      test: ["CMD", "sh", "-c", "wget -qO- http://localhost:8081/actuator/health | grep -q UP"]
      interval: 15s
      timeout: 5s
      retries: 15
```

> 참고: `<<: *java-service` 가 `environment` 를 병합하지 않고 덮어쓰므로, 위에서 `JAVA_TOOL_OPTIONS`/`SPRING_PROFILES_ACTIVE` 를 명시적으로 다시 적었다.

- [ ] **Step 2: compose로 통합 기동 검증**

Run (platform 레포에서):
```bash
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-platform
docker compose up -d --build auth-user
sleep 60
curl -s http://localhost:8081/actuator/health
curl -s -X POST http://localhost:8081/api/v1/auth/guest
```
Expected: health `{"status":"UP"}`, guest 응답에 `accessToken`·`"displayName":"익명"` 포함.

- [ ] **Step 3: k8s Deployment/Service 스켈레톤**

`tainted-spring-platform/k8s/auth-user/deployment.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tainted-spring-auth-user
  namespace: tainted-spring
spec:
  replicas: 1
  selector:
    matchLabels: { app: auth-user }
  template:
    metadata:
      labels: { app: auth-user }
    spec:
      containers:
        - name: auth-user
          image: tainted-spring/auth-user:0.1.0
          ports: [{ containerPort: 8081 }]
          env:
            - { name: SPRING_PROFILES_ACTIVE, value: "eks" }
            - { name: JAVA_TOOL_OPTIONS, value: "" }
          readinessProbe:
            httpGet: { path: /actuator/health, port: 8081 }
            initialDelaySeconds: 20
            periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: tainted-spring-auth-user
  namespace: tainted-spring
spec:
  selector: { app: auth-user }
  ports:
    - { port: 80, targetPort: 8081 }
```

- [ ] **Step 4: platform 레포 커밋**

```bash
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-platform
git add -A
git commit -m "feat: wire auth-user service into compose and k8s skeleton"
```

---

## Self-Review

**Spec coverage (설계문서 §3.1 대비):**
- 소셜 로그인(카카오/네이버/토스) → Task 3 `MockSocialVerifier`(SUPPORTED) + Task 6 login ✔
- 게스트 진입 → Task 6 `guest()` + Task 7 `/api/v1/auth/guest` ✔
- 익명 식별자 발급 / 표시명 항상 "익명" → `AuthService.DISPLAY_NAME` ✔
- 토큰 검증(introspection) → `/internal/auth/verify` ✔
- `GET /internal/users/{id}` → Task 7 ✔
- MySQL(authuser) + Redis(db 0) → application.yml + Task 8 Testcontainers ✔
- OpenAPI 노출 → Task 7 `OpenApiConfig` + springdoc ✔
- RFC 7807 problem+json → Task 7 `GlobalExceptionHandler` + Task 8 검증 ✔
- `/actuator/health` 유지, 추적 라이브러리 없음 → pom.xml에 actuator만, OTel/Sleuth 미포함 ✔
- 결정론(Clock/IdGenerator) → Task 2 ✔
- Docker(멀티스테이지/non-root) + docker 프로파일 DNS → Task 9 ✔
- compose 배선(빈 JAVA_TOOL_OPTIONS 슬롯) + k8s 스켈레톤 → Task 10 ✔

**Placeholder scan:** 모든 코드 step에 완전한 소스, 모든 명령에 기대출력 명시. 플레이스홀더 없음. ✔

**Type/이름 일관성:**
- `IdGenerator.newId()` — Task 2 정의, Task 5·6에서 동일 시그니처 사용 ✔
- `TokenService.issue(String)` / `resolveUserId(String): Optional<String>` — Task 5 정의, Task 6 사용 일치 ✔
- `ExternalIdentity.provider()/externalId()` (record accessor) — Task 3 정의, Task 6 사용 일치 ✔
- `UserAccountRepository.findBySocialProviderAndExternalId` — Task 4 정의, Task 6 사용 일치 ✔
- DTO 필드명(`accessToken`,`displayName`,`socialProvider`,`userId`,`active`,`id`) — Task 6 정의, Task 8 RestAssured 검증 키와 일치 ✔
- jar 파일명 `auth-user-service-0.1.0.jar` — pom `artifactId`+`version` 과 Dockerfile COPY 일치 ✔
