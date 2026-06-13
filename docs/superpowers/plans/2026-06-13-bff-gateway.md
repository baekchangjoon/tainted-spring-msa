# tainted-spring-bff-gateway Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 외부 클라이언트의 유일한 진입점(`/api/v1/*`)으로서 단순 통과 라우팅(Spring Cloud Gateway 라우트)과 다운스트림 집계(WebClient + `Mono.zip`)를 함께 제공하고, Bearer 토큰을 `auth-user`에 위임 검증하는 `bff-gateway`(Java 21 · Gradle · Spring Boot 3.4 · Spring Cloud Gateway/WebFlux)를 WireMock + RestAssured로 검증 가능하게 구현한다.

**Architecture:** Spring Cloud Gateway(리액티브/WebFlux) 기반 엣지 서비스. 두 메커니즘이 **공존**한다 — (1) 단순 패스스루는 `RouteLocator` 빈의 Gateway 라우트로 path rewrite하여 다운스트림(`auth-user`/`community`/`counseling`/`diary` 목록·생성)에 전달, (2) 여러 서비스를 합치는 합성 응답(`GET /api/v1/diaries/{id}` = diary + mindgraph, `GET /api/v1/me/mood-trends` = analytics)은 `@RestController` + `WebClient`로 직접 처리. 라우트는 합성 컨트롤러 경로와 충돌하지 않도록 **명시적/구체적**으로 정의한다(예: `/api/v1/diaries`는 라우트, `/api/v1/diaries/{id}` GET은 컨트롤러). 인증은 `GlobalFilter`가 보호 라우트에서 Bearer 토큰을 추출해 `auth-user POST /internal/auth/verify`로 위임 검증하고, `active=false`면 401 problem+json, `active=true`면 `X-User-Id` 헤더를 붙여 통과시킨다. 다운스트림 base URL은 `services.<name>.url` 프로퍼티(환경변수 `SERVICES_<NAME>_URL`로 오버라이드)로 주입. 시간/ID는 주입 가능한 `Clock`/`IdGenerator` 빈으로 결정론 확보. **추적/관측 라이브러리는 넣지 않으며, BFF는 `traceparent` 등 추적 헤더를 전파하지 않는다**(actuator health만 유지).

**Tech Stack:** Java 21, Gradle 8.10, Spring Boot 3.4.1, Spring Cloud 2024.0.0(Gateway), Spring WebFlux, WebClient, springdoc-openapi-webflux 2.6, WireMock 3.9.1, RestAssured.

---

## 사전 조건

- `tainted-spring-platform` 계획 및 다운스트림 서비스(`auth-user`/`diary`/`mindgraph`/`counseling`/`community`/`analytics`) 계획이 완료되어 있어야 통합 실행(compose `--wait`)이 쉽다. 단위/통합 테스트는 다운스트림을 **WireMock으로 스텁**하므로 platform·다운스트림 없이도 테스트는 통과한다.
- **레포 위치**: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-bff-gateway/` (모든 경로는 이 루트 기준).
- **포트**: 8080(외부에 노출되는 유일한 서비스). **패키지 루트**: `com.tainted.bff`.
- **다운스트림 URL**(compose DNS, 환경변수로 오버라이드): auth-user `http://auth-user:8081`, diary `http://diary:8082`, mindgraph `http://mindgraph:8083`, counseling `http://counseling:8084`, community `http://community:8085`, analytics `http://analytics:8086`. 프로퍼티 키 `services.<name>.url`, 오버라이드 env `SERVICES_<NAME>_URL`(예: `SERVICES_AUTHUSER_URL`).
- **로컬 사전 설치**: **Docker 데몬**(Task 9 이미지 빌드, Task 10 compose에 필요), JDK 21, Gradle 8.10(wrapper 생성용), Git. 단, 단위 테스트 `AuthGatewayFilterTest`(Task 6) 및 통합 테스트(Task 8, WireMock은 in-JVM)는 Docker 없이도 동작한다.

## File Structure

```
build.gradle
settings.gradle
gradle/wrapper/gradle-wrapper.properties   # gradle wrapper --gradle-version 8.10 로 생성
gradlew, gradlew.bat
Dockerfile
.gitignore
src/main/resources/application.yml
src/main/resources/application-docker.yml
src/main/java/com/tainted/bff/
  BffGatewayApplication.java
  config/DeterminismConfig.java          # Clock 빈
  config/OpenApiConfig.java
  config/WebClientConfig.java            # 서비스별 WebClient 빈(키별)
  config/GatewayRoutesConfig.java        # RouteLocator (패스스루 라우트)
  id/IdGenerator.java                    # 인터페이스
  id/UuidIdGenerator.java                # 기본 구현
  auth/TokenVerifier.java                # auth-user introspection 호출 추상화
  auth/WebClientTokenVerifier.java       # WebClient 구현
  auth/VerificationResult.java           # active/userId/socialProvider
  auth/AuthGlobalFilter.java             # GlobalFilter: 보호 라우트 토큰 검증
  web/CompositeController.java           # 합성 엔드포인트(/diaries/{id}, /me/mood-trends)
  web/dto/...                            # 합성 응답 DTO
  error/GatewayExceptionHandler.java     # RFC 7807 @RestControllerAdvice
src/test/java/com/tainted/bff/
  auth/AuthGatewayFilterTest.java        # 단위(WireMock으로 verify 스텁)
  BffIntegrationTest.java                # WireMock + RestAssured (RANDOM_PORT)
```

---

## Task 1: 레포 스캐폴드 + 빌드 가능한 빈 앱

**Files:**
- Create: `settings.gradle`, `build.gradle`, `src/main/java/com/tainted/bff/BffGatewayApplication.java`, `src/main/resources/application.yml`, `.gitignore`
- Run: `gradle wrapper --gradle-version 8.10`

- [ ] **Step 1: 디렉토리 생성 + git init**

Run:
```bash
mkdir -p /Users/changjoonbaek/workspace_claude-code/tainted-spring-bff-gateway
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-bff-gateway
git init
mkdir -p src/main/java/com/tainted/bff src/main/resources src/test/java/com/tainted/bff
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
rootProject.name = 'bff-gateway'
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

ext {
    springCloudVersion = '2024.0.0'
}

dependencyManagement {
    imports {
        mavenBom "org.springframework.cloud:spring-cloud-dependencies:${springCloudVersion}"
    }
}

dependencies {
    // Spring Cloud Gateway 는 리액티브 — spring-boot-starter-web 를 넣지 않는다(WebFlux 와 충돌).
    implementation 'org.springframework.cloud:spring-cloud-starter-gateway'
    implementation 'org.springframework.boot:spring-boot-starter-webflux'
    implementation 'org.springframework.boot:spring-boot-starter-actuator'
    implementation 'org.springframework.boot:spring-boot-starter-validation'
    implementation 'org.springdoc:springdoc-openapi-starter-webflux-ui:2.6.0'

    testImplementation 'org.springframework.boot:spring-boot-starter-test'
    testImplementation 'io.projectreactor:reactor-test'
    testImplementation 'org.wiremock:wiremock-standalone:3.9.1'
    testImplementation 'io.rest-assured:rest-assured'
}

tasks.named('test') {
    useJUnitPlatform()
}

// jar 이름 고정: build/libs/bff-gateway-0.1.0.jar
tasks.named('bootJar') {
    archiveBaseName = 'bff-gateway'
}
```

- [ ] **Step 5: 메인 클래스 + 최소 설정**

`src/main/java/com/tainted/bff/BffGatewayApplication.java`:
```java
package com.tainted.bff;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class BffGatewayApplication {
    public static void main(String[] args) {
        SpringApplication.run(BffGatewayApplication.class, args);
    }
}
```

`src/main/resources/application.yml`:
```yaml
server:
  port: 8080
spring:
  application:
    name: bff-gateway
  webflux:
    problemdetails:
      enabled: true
services:
  authuser:
    url: http://localhost:8081
  diary:
    url: http://localhost:8082
  mindgraph:
    url: http://localhost:8083
  counseling:
    url: http://localhost:8084
  community:
    url: http://localhost:8085
  analytics:
    url: http://localhost:8086
management:
  endpoints:
    web:
      exposure:
        include: health
```

> 참고: 다운스트림 URL은 `services.<name>.url` 프로퍼티이며, Spring relaxed binding 으로 환경변수 `SERVICES_AUTHUSER_URL`, `SERVICES_DIARY_URL` 등으로 오버라이드된다. Gateway 라우트(Task 5)와 컨트롤러/필터(Task 4·6·7)가 모두 이 프로퍼티를 참조한다.

- [ ] **Step 6: Gradle wrapper 생성 (8.10 고정)**

Run:
```bash
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-bff-gateway
gradle wrapper --gradle-version 8.10
```
Expected: `gradlew`, `gradlew.bat`, `gradle/wrapper/gradle-wrapper.jar`, `gradle/wrapper/gradle-wrapper.properties` 생성. `gradle-wrapper.properties` 의 `distributionUrl` 에 `gradle-8.10-bin.zip` 포함.

- [ ] **Step 7: 컴파일 확인**

Run: `./gradlew compileJava`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 8: 커밋**

```bash
git add -A
git commit -m "chore: scaffold bff-gateway (buildable empty app)"
```

---

## Task 2: 결정론 빈 (Clock, IdGenerator)

**Files:**
- Create: `id/IdGenerator.java`, `id/UuidIdGenerator.java`, `config/DeterminismConfig.java`

- [ ] **Step 1: `IdGenerator` 인터페이스**

`src/main/java/com/tainted/bff/id/IdGenerator.java`:
```java
package com.tainted.bff.id;

/** 식별자 생성 추상화. 테스트에서 결정론적 구현으로 교체 가능. */
public interface IdGenerator {
    String newId();
}
```

- [ ] **Step 2: 기본 UUID 구현**

`src/main/java/com/tainted/bff/id/UuidIdGenerator.java`:
```java
package com.tainted.bff.id;

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

> 주의: 이 ID는 내부 생성용(예: 합성 응답 메타)이며, **추적 헤더(`traceparent`)나 상관관계 ID를 다운스트림에 전파하지 않는다**(계측 부재 원칙).

- [ ] **Step 3: Clock 빈**

`src/main/java/com/tainted/bff/config/DeterminismConfig.java`:
```java
package com.tainted.bff.config;

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

Run: `./gradlew compileJava`
Expected: BUILD SUCCESSFUL.
```bash
git add -A
git commit -m "feat: add injectable Clock and IdGenerator beans"
```

---

## Task 3: 서비스별 WebClient 빈

**Files:**
- Create: `config/WebClientConfig.java`

다운스트림 호출을 테스트에서 WireMock 으로 가리킬 수 있도록, 서비스 URL 프로퍼티를 주입받아 서비스별 `WebClient` 빈을 만든다.

- [ ] **Step 1: WebClient 빈 정의**

`src/main/java/com/tainted/bff/config/WebClientConfig.java`:
```java
package com.tainted.bff.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class WebClientConfig {

    @Bean
    WebClient authUserWebClient(WebClient.Builder builder,
                                @Value("${services.authuser.url}") String url) {
        return builder.baseUrl(url).build();
    }

    @Bean
    WebClient diaryWebClient(WebClient.Builder builder,
                             @Value("${services.diary.url}") String url) {
        return builder.baseUrl(url).build();
    }

    @Bean
    WebClient mindgraphWebClient(WebClient.Builder builder,
                                 @Value("${services.mindgraph.url}") String url) {
        return builder.baseUrl(url).build();
    }

    @Bean
    WebClient analyticsWebClient(WebClient.Builder builder,
                                 @Value("${services.analytics.url}") String url) {
        return builder.baseUrl(url).build();
    }
}
```

> `WebClient.Builder` 는 Spring Boot WebFlux 자동설정이 제공한다. 기본 빌더이므로 추적 인터셉터/필터는 추가되지 않는다(계측 부재).

- [ ] **Step 2: 컴파일 + 커밋**

Run: `./gradlew compileJava`
Expected: BUILD SUCCESSFUL.
```bash
git add -A
git commit -m "feat: per-service WebClient beans keyed by service URL"
```

---

## Task 4: 토큰 검증 위임 (TokenVerifier, TDD 기반 인터페이스)

**Files:**
- Create: `auth/VerificationResult.java`, `auth/TokenVerifier.java`, `auth/WebClientTokenVerifier.java`

BFF는 토큰을 직접 해석하지 않고 `auth-user POST /internal/auth/verify` 에 위임한다.

- [ ] **Step 1: 검증 결과 record**

`src/main/java/com/tainted/bff/auth/VerificationResult.java`:
```java
package com.tainted.bff.auth;

/** auth-user introspection 응답 매핑. active=false 면 userId/socialProvider 는 null. */
public record VerificationResult(boolean active, String userId, String socialProvider) {

    public static VerificationResult inactive() {
        return new VerificationResult(false, null, null);
    }
}
```

- [ ] **Step 2: 인터페이스**

`src/main/java/com/tainted/bff/auth/TokenVerifier.java`:
```java
package com.tainted.bff.auth;

import reactor.core.publisher.Mono;

/** Bearer 토큰을 auth-user 에 위임 검증. 호출 실패/비활성은 inactive 로 귀결. */
public interface TokenVerifier {
    Mono<VerificationResult> verify(String token);
}
```

- [ ] **Step 3: WebClient 구현**

`src/main/java/com/tainted/bff/auth/WebClientTokenVerifier.java`:
```java
package com.tainted.bff.auth;

import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.util.Map;

@Component
public class WebClientTokenVerifier implements TokenVerifier {

    private final WebClient authUserWebClient;

    public WebClientTokenVerifier(WebClient authUserWebClient) {
        this.authUserWebClient = authUserWebClient;
    }

    @Override
    public Mono<VerificationResult> verify(String token) {
        if (token == null || token.isBlank()) {
            return Mono.just(VerificationResult.inactive());
        }
        return authUserWebClient.post()
                .uri("/internal/auth/verify")
                .bodyValue(Map.of("token", token))
                .retrieve()
                .bodyToMono(VerificationResult.class)
                // 다운스트림 오류(비2xx/네트워크)는 비활성으로 처리 → 401 로 귀결.
                .onErrorReturn(VerificationResult.inactive())
                .defaultIfEmpty(VerificationResult.inactive());
    }
}
```

> `VerificationResult` 의 필드명(`active`,`userId`,`socialProvider`)은 auth-user `/internal/auth/verify` 응답 키와 일치한다(설계 §3.1). 모르는 추가 필드는 Jackson 기본(`FAIL_ON_UNKNOWN_PROPERTIES=false`)으로 무시되어 스키마 드리프트에 견딘다.

- [ ] **Step 4: 컴파일 + 커밋**

Run: `./gradlew compileJava`
Expected: BUILD SUCCESSFUL.
```bash
git add -A
git commit -m "feat: TokenVerifier delegating to auth-user introspection"
```

---

## Task 5: Gateway 패스스루 라우트 (RouteLocator)

**Files:**
- Create: `config/GatewayRoutesConfig.java`

단순 통과 라우팅을 `RouteLocator` 빈으로 정의한다. **합성 컨트롤러 경로(`/api/v1/diaries/{id}` GET, `/api/v1/me/mood-trends`)와 충돌하지 않도록 라우트를 구체적으로** 만든다.

- [ ] **Step 1: RouteLocator 빈**

`src/main/java/com/tainted/bff/config/GatewayRoutesConfig.java`:
```java
package com.tainted.bff.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.route.RouteLocator;
import org.springframework.cloud.gateway.route.builder.RouteLocatorBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GatewayRoutesConfig {

    @Bean
    public RouteLocator routes(RouteLocatorBuilder builder,
                               @Value("${services.authuser.url}") String authUserUrl,
                               @Value("${services.community.url}") String communityUrl,
                               @Value("${services.counseling.url}") String counselingUrl,
                               @Value("${services.diary.url}") String diaryUrl) {
        return builder.routes()
                // 공개 라우트: 로그인/게스트. auth 는 AuthGlobalFilter 의 PUBLIC 접두사.
                // /api/v1/auth/** -> auth-user 동일 경로 그대로 전달.
                .route("auth", r -> r.path("/api/v1/auth/**")
                        .uri(authUserUrl))
                // community 패스스루: 외부 /api/v1/community/** -> 내부 /internal/...
                // 단순/명시적 매핑: /api/v1/community/(rest) -> /internal/(rest)
                .route("community", r -> r.path("/api/v1/community/**")
                        .filters(f -> f.rewritePath(
                                "/api/v1/community/(?<seg>.*)", "/internal/${seg}"))
                        .uri(communityUrl))
                // counseling 패스스루: /api/v1/counseling/** -> /internal/counseling/**
                .route("counseling", r -> r.path("/api/v1/counseling/**")
                        .filters(f -> f.rewritePath(
                                "/api/v1/counseling/(?<seg>.*)", "/internal/counseling/${seg}"))
                        .uri(counselingUrl))
                // diary 목록/생성만 라우트(경로에 id 없음). {id} GET 은 CompositeController 가 처리.
                // 정확히 /api/v1/diaries 만 매칭(하위 세그먼트 제외) → 합성 경로와 비충돌.
                .route("diary-list-create", r -> r.path("/api/v1/diaries")
                        .filters(f -> f.rewritePath(
                                "/api/v1/diaries", "/internal/diaries"))
                        .uri(diaryUrl))
                .build();
    }
}
```

> **라우트 vs 컨트롤러 정밀도(precedence)**: `/api/v1/diaries` 는 정확 경로(`path` predicate 가 `**` 없이 정확 매칭)라서 하위 `/api/v1/diaries/{id}` 는 라우트에 잡히지 않고 `CompositeController`(Task 7)가 받는다. Spring Cloud Gateway 의 `HandlerMapping`(`RoutePredicateHandlerMapping`)은 `@RestController`(`RequestMappingHandlerMapping`)보다 **낮은 우선순위**이므로, 매핑되는 컨트롤러가 있으면 컨트롤러가 우선한다. 그래도 혼동을 줄이기 위해 라우트를 구체적으로 둔다.

- [ ] **Step 2: 컴파일 + 커밋**

Run: `./gradlew compileJava`
Expected: BUILD SUCCESSFUL.
```bash
git add -A
git commit -m "feat: gateway passthrough routes (auth/community/counseling/diary)"
```

---

## Task 6: 인증 GlobalFilter (TDD)

**Files:**
- Create: `auth/AuthGlobalFilter.java`
- Test: `src/test/java/com/tainted/bff/auth/AuthGatewayFilterTest.java`

보호 라우트에서 Bearer 토큰을 추출 → `TokenVerifier` 위임 → `active=false`/누락이면 401 problem+json, `active=true`면 다운스트림 요청에 `X-User-Id` 헤더를 추가해 통과. 공개 접두사: `/api/v1/auth/**`.

- [ ] **Step 1: 실패하는 단위 테스트 작성**

`src/test/java/com/tainted/bff/auth/AuthGatewayFilterTest.java`:
```java
package com.tainted.bff.auth;

import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class AuthGatewayFilterTest {

    private final TokenVerifier good = token ->
            Mono.just(new VerificationResult(true, "user-1", "kakao"));
    private final TokenVerifier bad = token ->
            Mono.just(VerificationResult.inactive());

    private GatewayFilterChain capturing(AtomicReference<ServerWebExchange> captured) {
        return exchange -> {
            captured.set(exchange);
            return Mono.empty();
        };
    }

    @Test
    void publicAuthPathBypassesVerification() {
        AuthGlobalFilter filter = new AuthGlobalFilter(token -> {
            throw new AssertionError("public path must not verify");
        });
        AtomicReference<ServerWebExchange> captured = new AtomicReference<>();
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.post("/api/v1/auth/login"));

        StepVerifier.create(filter.filter(exchange, capturing(captured)))
                .verifyComplete();
        assertNotNull(captured.get(), "public path forwarded");
    }

    @Test
    void validTokenForwardsWithUserIdHeader() {
        AuthGlobalFilter filter = new AuthGlobalFilter(good);
        AtomicReference<ServerWebExchange> captured = new AtomicReference<>();
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/v1/community/posts")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer good"));

        StepVerifier.create(filter.filter(exchange, capturing(captured)))
                .verifyComplete();
        assertEquals("user-1",
                captured.get().getRequest().getHeaders().getFirst("X-User-Id"));
    }

    @Test
    void inactiveTokenRejectedWith401() {
        AuthGlobalFilter filter = new AuthGlobalFilter(bad);
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/v1/community/posts")
                        .header(HttpHeaders.AUTHORIZATION, "Bearer bad"));

        StepVerifier.create(filter.filter(exchange, e -> Mono.empty()))
                .verifyComplete();
        assertEquals(HttpStatus.UNAUTHORIZED, exchange.getResponse().getStatusCode());
    }

    @Test
    void missingTokenRejectedWith401() {
        AuthGlobalFilter filter = new AuthGlobalFilter(good);
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/v1/community/posts"));

        StepVerifier.create(filter.filter(exchange, e -> Mono.empty()))
                .verifyComplete();
        assertEquals(HttpStatus.UNAUTHORIZED, exchange.getResponse().getStatusCode());
    }
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `./gradlew test --tests '*AuthGatewayFilterTest'`
Expected: FAIL — `AuthGlobalFilter` 미존재로 컴파일 에러.

- [ ] **Step 3: 최소 구현**

`src/main/java/com/tainted/bff/auth/AuthGlobalFilter.java`:
```java
package com.tainted.bff.auth;

import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;

@Component
public class AuthGlobalFilter implements GlobalFilter, Ordered {

    private static final String PUBLIC_PREFIX = "/api/v1/auth/";
    private static final String BEARER = "Bearer ";

    private final TokenVerifier tokenVerifier;

    public AuthGlobalFilter(TokenVerifier tokenVerifier) {
        this.tokenVerifier = tokenVerifier;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String path = exchange.getRequest().getURI().getPath();
        if (path.startsWith(PUBLIC_PREFIX)) {
            return chain.filter(exchange);
        }
        String token = extractBearer(exchange.getRequest());
        if (token == null) {
            return unauthorized(exchange, "missing bearer token");
        }
        return tokenVerifier.verify(token).flatMap(result -> {
            if (!result.active()) {
                return unauthorized(exchange, "invalid or expired token");
            }
            ServerWebExchange mutated = exchange.mutate()
                    .request(r -> r.header("X-User-Id", result.userId()))
                    .build();
            return chain.filter(mutated);
        });
    }

    private String extractBearer(ServerHttpRequest request) {
        String header = request.getHeaders().getFirst(HttpHeaders.AUTHORIZATION);
        if (header == null || !header.startsWith(BEARER)) {
            return null;
        }
        String token = header.substring(BEARER.length()).trim();
        return token.isEmpty() ? null : token;
    }

    private Mono<Void> unauthorized(ServerWebExchange exchange, String detail) {
        ProblemDetail pd = ProblemDetail.forStatus(HttpStatus.UNAUTHORIZED);
        pd.setTitle("Unauthorized");
        pd.setDetail(detail);
        String body = "{\"type\":\"about:blank\",\"title\":\"Unauthorized\",\"status\":401,\"detail\":\""
                + detail + "\"}";
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
        exchange.getResponse().getHeaders()
                .setContentType(MediaType.APPLICATION_PROBLEM_JSON);
        return exchange.getResponse()
                .writeWith(Mono.just(exchange.getResponse().bufferFactory().wrap(bytes)));
    }

    @Override
    public int getOrder() {
        // 라우팅 필터보다 먼저 실행되도록 높은 우선순위(낮은 값).
        return -1;
    }
}
```

> `X-User-Id` 만 추가하고 추적 헤더는 절대 추가하지 않는다. 401 본문은 RFC 7807 `application/problem+json` 형식을 직접 직렬화한다(Gateway 필터 단계에서는 `@RestControllerAdvice`가 적용되지 않으므로 명시적으로 작성).

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew test --tests '*AuthGatewayFilterTest'`
Expected: PASS (4 tests).

- [ ] **Step 5: 커밋**

```bash
git add -A
git commit -m "feat: auth GlobalFilter delegating to auth-user, adds X-User-Id"
```

---

## Task 7: 합성 컨트롤러 + RFC 7807 + OpenAPI

**Files:**
- Create: `web/dto/DiaryDetailResponse.java`, `web/dto/MoodTrendsResponse.java`, `web/CompositeController.java`, `error/GatewayExceptionHandler.java`, `config/OpenApiConfig.java`

- [ ] **Step 1: 합성 응답 DTO**

`src/main/java/com/tainted/bff/web/dto/DiaryDetailResponse.java`:
```java
package com.tainted.bff.web.dto;

import com.fasterxml.jackson.databind.JsonNode;

/** diary + mindgraph 집계 응답. 다운스트림 본문을 그대로 중첩. */
public record DiaryDetailResponse(JsonNode diary, JsonNode graph) {}
```

`src/main/java/com/tainted/bff/web/dto/MoodTrendsResponse.java`:
```java
package com.tainted.bff.web.dto;

import com.fasterxml.jackson.databind.JsonNode;

public record MoodTrendsResponse(String userId, JsonNode trends) {}
```

- [ ] **Step 2: 합성 컨트롤러**

`src/main/java/com/tainted/bff/web/CompositeController.java`:
```java
package com.tainted.bff.web;

import com.fasterxml.jackson.databind.JsonNode;
import com.tainted.bff.web.dto.DiaryDetailResponse;
import com.tainted.bff.web.dto.MoodTrendsResponse;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@RestController
public class CompositeController {

    private final WebClient diaryWebClient;
    private final WebClient mindgraphWebClient;
    private final WebClient analyticsWebClient;

    public CompositeController(WebClient diaryWebClient,
                               WebClient mindgraphWebClient,
                               WebClient analyticsWebClient) {
        this.diaryWebClient = diaryWebClient;
        this.mindgraphWebClient = mindgraphWebClient;
        this.analyticsWebClient = analyticsWebClient;
    }

    /** diary 상세 + 마음 그래프 집계. AuthGlobalFilter 통과 후 호출됨. */
    @GetMapping("/api/v1/diaries/{id}")
    public Mono<DiaryDetailResponse> diaryDetail(@PathVariable String id) {
        Mono<JsonNode> diary = diaryWebClient.get()
                .uri("/internal/diaries/{id}", id)
                .retrieve()
                .bodyToMono(JsonNode.class);
        Mono<JsonNode> graph = mindgraphWebClient.get()
                .uri("/internal/graphs/diary/{id}", id)
                .retrieve()
                .bodyToMono(JsonNode.class);
        return Mono.zip(diary, graph)
                .map(t -> new DiaryDetailResponse(t.getT1(), t.getT2()));
    }

    /** 무드 추이. userId 는 AuthGlobalFilter 가 검증해 붙인 X-User-Id 헤더에서 취득. */
    @GetMapping("/api/v1/me/mood-trends")
    public Mono<MoodTrendsResponse> moodTrends(@RequestHeader("X-User-Id") String userId) {
        return analyticsWebClient.get()
                .uri("/internal/analytics/mood/{userId}", userId)
                .retrieve()
                .bodyToMono(JsonNode.class)
                .map(trends -> new MoodTrendsResponse(userId, trends));
    }
}
```

- [ ] **Step 3: RFC 7807 예외 핸들러 (WebFlux)**

`src/main/java/com/tainted/bff/error/GatewayExceptionHandler.java`:
```java
package com.tainted.bff.error;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.server.ServerWebInputException;

@RestControllerAdvice
public class GatewayExceptionHandler {

    /** 다운스트림이 4xx/5xx 를 돌려준 경우, 해당 상태로 problem+json 변환. */
    @ExceptionHandler(WebClientResponseException.class)
    public ProblemDetail handleDownstream(WebClientResponseException ex) {
        ProblemDetail pd = ProblemDetail.forStatus(ex.getStatusCode());
        pd.setTitle("Downstream error");
        pd.setDetail("downstream returned " + ex.getStatusCode().value());
        return pd;
    }

    /** 필수 헤더/입력 누락(예: X-User-Id) → 400. */
    @ExceptionHandler(ServerWebInputException.class)
    public ProblemDetail handleInput(ServerWebInputException ex) {
        ProblemDetail pd = ProblemDetail.forStatus(HttpStatus.BAD_REQUEST);
        pd.setTitle("Invalid request");
        pd.setDetail(ex.getReason());
        return pd;
    }
}
```

> `spring.webflux.problemdetails.enabled=true`(Task 1 application.yml)로 Spring 의 기본 예외(404 등)도 problem+json 으로 직렬화된다. 위 핸들러는 합성 호출의 다운스트림 오류·입력 오류를 보강한다.

- [ ] **Step 4: OpenAPI 설정**

`src/main/java/com/tainted/bff/config/OpenApiConfig.java`:
```java
package com.tainted.bff.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {
    @Bean
    public OpenAPI bffOpenApi() {
        return new OpenAPI().info(new Info()
                .title("bff-gateway API")
                .version("0.1.0")
                .description("외부 진입점: 패스스루 라우팅 + diary/mindgraph/analytics 집계"));
    }
}
```

> 참고: Gateway 라우트 경로는 springdoc 가 자동 노출하지 않는다. springdoc-webflux 는 `@RestController`(합성 엔드포인트)의 OpenAPI 만 문서화한다. 이는 본 테스트베드 설계상 의도된 부분 문서화이다.

- [ ] **Step 5: 컴파일 + 커밋**

Run: `./gradlew compileJava`
Expected: BUILD SUCCESSFUL.
```bash
git add -A
git commit -m "feat: composite controller, RFC7807 handling, OpenAPI"
```

---

## Task 8: 통합 테스트 (WireMock + RestAssured)

**Files:**
- Test: `src/test/java/com/tainted/bff/BffIntegrationTest.java`

엣지 서비스이므로 다운스트림은 **Testcontainers 가 아니라 WireMock(in-JVM, 동적 포트)**으로 스텁한다. 하나의 WireMock 서버에 경로 기반 스텁을 두고, `@DynamicPropertySource` 로 모든 `services.*.url` 을 그 base URL 로 가리킨다. RestAssured 는 BFF 의 RANDOM_PORT 를 호출한다.

- [ ] **Step 1: 통합 테스트 작성**

`src/test/java/com/tainted/bff/BffIntegrationTest.java`:
```java
package com.tainted.bff;

import com.github.tomakehurst.wiremock.WireMockServer;
import io.restassured.RestAssured;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class BffIntegrationTest {

    static WireMockServer wireMock;

    @BeforeAll
    static void startWireMock() {
        wireMock = new WireMockServer(options().dynamicPort());
        wireMock.start();

        // auth-user: token 'good' -> active true, 그 외 -> active false.
        wireMock.stubFor(post(urlEqualTo("/internal/auth/verify"))
                .withRequestBody(matchingJsonPath("$.token", equalTo("good")))
                .willReturn(okJson(
                        "{\"active\":true,\"userId\":\"user-1\",\"socialProvider\":\"kakao\"}")));
        wireMock.stubFor(post(urlEqualTo("/internal/auth/verify"))
                .withRequestBody(matchingJsonPath("$.token", equalTo("bad")))
                .willReturn(okJson(
                        "{\"active\":false,\"userId\":null,\"socialProvider\":null}")));

        // diary 상세 + mindgraph 그래프 (합성 대상).
        wireMock.stubFor(get(urlEqualTo("/internal/diaries/d1"))
                .willReturn(okJson(
                        "{\"id\":\"d1\",\"userId\":\"user-1\",\"primaryEmotion\":\"calm\"}")));
        wireMock.stubFor(get(urlEqualTo("/internal/graphs/diary/d1"))
                .willReturn(okJson(
                        "{\"diaryId\":\"d1\",\"nodeCount\":3}")));

        // community 패스스루: 외부 /api/v1/community/posts -> 내부 /internal/posts.
        wireMock.stubFor(get(urlEqualTo("/internal/posts"))
                .willReturn(okJson(
                        "[{\"id\":\"p1\",\"title\":\"길을잃은갈매기\",\"category\":\"career\"}]")));
    }

    @AfterAll
    static void stopWireMock() {
        wireMock.stop();
    }

    @DynamicPropertySource
    static void downstreamUrls(DynamicPropertyRegistry r) {
        // 하나의 WireMock 으로 모든 다운스트림을 경로 기반 스텁 처리.
        String base = "http://localhost:" + wireMock.port();
        r.add("services.authuser.url", () -> base);
        r.add("services.diary.url", () -> base);
        r.add("services.mindgraph.url", () -> base);
        r.add("services.counseling.url", () -> base);
        r.add("services.community.url", () -> base);
        r.add("services.analytics.url", () -> base);
    }

    @LocalServerPort
    int port;

    @BeforeEach
    void setUp() {
        RestAssured.port = port;
    }

    @Test
    void compositeDiaryDetailMergesDiaryAndGraph() {
        given().header("Authorization", "Bearer good")
                .when().get("/api/v1/diaries/d1")
                .then().statusCode(200)
                .body("diary.id", equalTo("d1"))
                .body("diary.primaryEmotion", equalTo("calm"))
                .body("graph.diaryId", equalTo("d1"))
                .body("graph.nodeCount", equalTo(3));
    }

    @Test
    void protectedRouteWithBadTokenReturns401ProblemJson() {
        given().header("Authorization", "Bearer bad")
                .when().get("/api/v1/diaries/d1")
                .then().statusCode(401)
                .contentType("application/problem+json")
                .body("title", equalTo("Unauthorized"));
    }

    @Test
    void missingTokenReturns401() {
        given().when().get("/api/v1/diaries/d1")
                .then().statusCode(401)
                .contentType("application/problem+json");
    }

    @Test
    void passthroughCommunityRouteForwardsStubbedBody() {
        given().header("Authorization", "Bearer good")
                .when().get("/api/v1/community/posts")
                .then().statusCode(200)
                .body("[0].id", equalTo("p1"))
                .body("[0].title", equalTo("길을잃은갈매기"));
    }
}
```

> WireMock 스텁은 결정론적이다(고정 토큰/경로/본문). `okJson` 으로 `Content-Type: application/json` 을 보장하고, 패스스루 라우트는 community 의 `/internal/posts` 스텁 응답을 그대로 외부로 반환한다(rewrite `/api/v1/community/posts` → `/internal/posts`).

- [ ] **Step 2: 전체 테스트 실행 (Docker 불필요)**

Run: `./gradlew test`
Expected: PASS — `AuthGatewayFilterTest`(4) + `BffIntegrationTest`(4) 모두 통과. WireMock 은 in-JVM 이므로 Docker 데몬 불필요.

- [ ] **Step 3: 커밋**

```bash
git add -A
git commit -m "test: WireMock + RestAssured integration tests"
```

---

## Task 9: Dockerfile + docker 프로파일

**Files:**
- Create: `Dockerfile`, `src/main/resources/application-docker.yml`

- [ ] **Step 1: docker 프로파일 설정 (compose DNS)**

> 각 `services.*.url` 기본값을 compose DNS 로 두되, Task 10에서 compose `environment:` 의 `SERVICES_<NAME>_URL` 이 최종 오버라이드한다(미주입 시 아래 기본값 사용).

`src/main/resources/application-docker.yml`:
```yaml
services:
  authuser:
    url: http://auth-user:8081
  diary:
    url: http://diary:8082
  mindgraph:
    url: http://mindgraph:8083
  counseling:
    url: http://counseling:8084
  community:
    url: http://community:8085
  analytics:
    url: http://analytics:8086
```

- [ ] **Step 2: 멀티스테이지 Dockerfile (non-root)**

`Dockerfile`:
```dockerfile
# build
FROM gradle:8.10-jdk21 AS build
WORKDIR /app
COPY settings.gradle build.gradle ./
COPY src ./src
RUN gradle bootJar --no-daemon

# run
FROM eclipse-temurin:21-jre
WORKDIR /app
# curl: compose/k8s 헬스체크가 /actuator/health 를 호출하는 데 사용.
# temurin JRE 기본 이미지에는 curl 이 없으므로 명시적으로 설치한다.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd -r -u 1001 appuser
COPY --from=build /app/build/libs/bff-gateway-0.1.0.jar app.jar
USER appuser
EXPOSE 8080
HEALTHCHECK CMD curl -fsS http://localhost:8080/actuator/health || exit 1
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 3: 이미지 빌드 확인**

Run: `docker build -t tainted-spring/bff-gateway:0.1.0 .`
Expected: 빌드 성공, 최종 이미지 생성.

- [ ] **Step 4: 커밋**

```bash
git add -A
git commit -m "build: Dockerfile and docker profile"
```

---

## Task 10: platform 레포에 bff-gateway 배선 (compose + k8s 스켈레톤)

> ⚠️ **작업 레포 전환**: 이 Task의 모든 파일 수정·커밋은 `tainted-spring-bff-gateway` 가 아니라 **`tainted-spring-platform` 레포**에서 한다. 각 Step의 `cd` 경로를 확인할 것.

**Files (다른 레포 `tainted-spring-platform`):**
- Modify: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-platform/docker-compose.yml`
- Create: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-platform/k8s/bff-gateway/deployment.yaml`

- [ ] **Step 1: compose에 bff-gateway 서비스 추가**

`tainted-spring-platform/docker-compose.yml` 의 `services:` 아래에 추가 (앵커 `*java-service` 재사용). bff-gateway 는 다운스트림 6개가 healthy 해야 하므로 `depends_on` 에 모두 등재(각 서비스는 자체 배선에서 healthcheck 보유 가정):
```yaml
  bff-gateway:
    <<: *java-service
    build:
      context: ../tainted-spring-bff-gateway
    depends_on:
      auth-user:
        condition: service_healthy
      diary:
        condition: service_healthy
      mindgraph:
        condition: service_healthy
      counseling:
        condition: service_healthy
      community:
        condition: service_healthy
      analytics:
        condition: service_healthy
    environment:
      JAVA_TOOL_OPTIONS: "${JAVA_TOOL_OPTIONS:-}"
      SPRING_PROFILES_ACTIVE: "docker"
      SERVICES_AUTHUSER_URL: "http://auth-user:8081"
      SERVICES_DIARY_URL: "http://diary:8082"
      SERVICES_MINDGRAPH_URL: "http://mindgraph:8083"
      SERVICES_COUNSELING_URL: "http://counseling:8084"
      SERVICES_COMMUNITY_URL: "http://community:8085"
      SERVICES_ANALYTICS_URL: "http://analytics:8086"
    ports:
      - "8080:8080"
    healthcheck:
      # 런타임 이미지에 설치한 curl 사용. -f: 비2xx 응답이면 실패 → actuator DOWN 시 unhealthy.
      test: ["CMD", "curl", "-fsS", "http://localhost:8080/actuator/health"]
      interval: 15s
      timeout: 5s
      retries: 20
      start_period: 40s
```

> 참고: `<<: *java-service` 가 `environment` 를 병합하지 않고 덮어쓰므로, `JAVA_TOOL_OPTIONS`/`SPRING_PROFILES_ACTIVE` 및 `SERVICES_*_URL` 전부를 명시적으로 다시 적었다. bff 는 외부에 노출되는 유일한 서비스라 `8080:8080` 으로 호스트에 매핑한다.

- [ ] **Step 2: compose로 통합 기동 검증**

Run (platform 레포에서):
```bash
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-platform
docker compose up -d --build --wait bff-gateway
curl -s http://localhost:8080/actuator/health
```
Expected: `--wait` 가 다운스트림 6개 + bff-gateway 헬스체크 통과까지 블록. health `{"status":"UP"}`. (다운스트림이 모두 떠 있어야 `depends_on ... service_healthy` 통과.)

- [ ] **Step 3: k8s Deployment/Service 스켈레톤**

`tainted-spring-platform/k8s/bff-gateway/deployment.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tainted-spring-bff-gateway
  namespace: tainted-spring
spec:
  replicas: 1
  selector:
    matchLabels: { app: bff-gateway }
  template:
    metadata:
      labels: { app: bff-gateway }
    spec:
      containers:
        - name: bff-gateway
          image: tainted-spring/bff-gateway:0.1.0
          ports: [{ containerPort: 8080 }]
          env:
            - { name: SPRING_PROFILES_ACTIVE, value: "eks" }
            - { name: JAVA_TOOL_OPTIONS, value: "" }
            - { name: SERVICES_AUTHUSER_URL, value: "http://tainted-spring-auth-user" }
            - { name: SERVICES_DIARY_URL, value: "http://tainted-spring-diary" }
            - { name: SERVICES_MINDGRAPH_URL, value: "http://tainted-spring-mindgraph" }
            - { name: SERVICES_COUNSELING_URL, value: "http://tainted-spring-counseling" }
            - { name: SERVICES_COMMUNITY_URL, value: "http://tainted-spring-community" }
            - { name: SERVICES_ANALYTICS_URL, value: "http://tainted-spring-analytics" }
          readinessProbe:
            httpGet: { path: /actuator/health, port: 8080 }
            initialDelaySeconds: 20
            periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: tainted-spring-bff-gateway
  namespace: tainted-spring
spec:
  # 외부 진입점이지만 스켈레톤에서는 ClusterIP 로 둔다.
  # 실제 노출은 이 Service 앞단에 Ingress 를 두어 외부 트래픽을 라우팅한다(스켈레톤 비범위).
  type: ClusterIP
  selector: { app: bff-gateway }
  ports:
    - { port: 80, targetPort: 8080 }
```

- [ ] **Step 4: platform 레포 커밋**

```bash
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-platform
git add -A
git commit -m "feat: wire bff-gateway service into compose and k8s skeleton"
```

---

## Self-Review

**Spec coverage (설계문서 §3.8 / §2.1 / §4 대비):**
- 외부 진입점 `/api/v1/*`, 토큰 검증 위임, 화면용 응답 집계 → Task 5(라우트) + Task 6(필터) + Task 7(합성) ✔
- 패스스루 라우팅(Gateway 라우트): `/api/v1/auth/**`→auth-user, `/api/v1/community/**`→community, `/api/v1/counseling/**`→counseling, `/api/v1/diaries`(목록/생성)→diary → Task 5 `GatewayRoutesConfig` ✔
- 합성 엔드포인트(WebClient + Mono.zip): `GET /api/v1/diaries/{id}` = diary + mindgraph 병합, `GET /api/v1/me/mood-trends` = analytics → Task 7 `CompositeController` ✔
- 라우트 vs 컨트롤러 비충돌(정밀 경로 + 핸들러 매핑 우선순위) → Task 5 설명 + `/api/v1/diaries` 정확 매칭 ✔
- 인증 GlobalFilter: Bearer 추출 → `auth-user POST /internal/auth/verify` 위임, active=false→401 problem+json, active=true→`X-User-Id` 부착, 공개 `/api/v1/auth/**` → Task 4 `TokenVerifier` + Task 6 `AuthGlobalFilter` ✔
- 다운스트림을 인터페이스/서비스별 WebClient 빈 뒤로 캡슐화(테스트가 WireMock 으로 포인팅) → Task 3 `WebClientConfig` + Task 4 `TokenVerifier` ✔
- `services.<name>.url` 프로퍼티 + `SERVICES_<NAME>_URL` env 오버라이드, docker 프로파일 compose DNS → Task 1 application.yml + Task 9 application-docker.yml + Task 10 env ✔
- WebFlux RFC 7807 problem+json(`@RestControllerAdvice` + `spring.webflux.problemdetails.enabled=true`) → Task 1 + Task 7 `GatewayExceptionHandler` ✔
- springdoc OpenAPI(webflux-ui 2.6.0), actuator health 만 → Task 1 build.gradle + application.yml + Task 7 `OpenApiConfig` ✔
- 추적/관측 라이브러리 없음, `traceparent`/상관관계 ID 전파 없음(X-User-Id 만 추가) → 의존성 미포함 + Task 2/6 주석 명시 ✔
- 결정론(Clock/IdGenerator) → Task 2 ✔
- 다운스트림 Testcontainers 미사용, WireMock(in-JVM 동적 포트) + RestAssured(RANDOM_PORT) + `@DynamicPropertySource` → Task 8 ✔
- Spring Cloud Gateway 는 리액티브 — `spring-boot-starter-web` 미포함(WebFlux only) → Task 1 build.gradle 주석 ✔
- Docker(멀티스테이지/non-root/curl 설치/HEALTHCHECK curl /actuator/health) → Task 9 ✔
- compose 배선(빈 JAVA_TOOL_OPTIONS 슬롯, depends_on 6개 service_healthy, 8080:8080, start_period 40s retries 20) + k8s 스켈레톤(ClusterIP + Ingress 주석) → Task 10 ✔

**Placeholder scan:** 모든 코드 step에 완전한 소스, 모든 명령에 기대출력 명시. 플레이스홀더 없음. ✔

**Type/이름 일관성:**
- `IdGenerator.newId()` — Task 2 정의(추적 헤더 전파 안 함 명시) ✔
- `TokenVerifier.verify(String): Mono<VerificationResult>` — Task 4 정의, Task 6 `AuthGlobalFilter` 사용 일치 ✔
- `VerificationResult.active()/userId()/socialProvider()` (record accessor) — Task 4 정의, Task 6 사용 + Task 8 WireMock 스텁 키와 일치 ✔
- WebClient 빈 이름(`authUserWebClient`,`diaryWebClient`,`mindgraphWebClient`,`analyticsWebClient`) — Task 3 정의, Task 4·7 생성자 주입 이름 일치 ✔
- `services.*.url` 프로퍼티 키 — Task 1/3/5/8/9/10 전부 동일 키 사용 일치 ✔
- 다운스트림 경로(`/internal/auth/verify`, `/internal/diaries/{id}`, `/internal/graphs/diary/{id}`, `/internal/analytics/mood/{userId}`, community `/internal/posts`) — Task 4·5·7 호출과 Task 8 WireMock 스텁 경로 일치, 설계 §3 내부 API 와 일치 ✔
- `X-User-Id` 헤더 — Task 6 필터가 부착, Task 7 `moodTrends` 가 소비 일치 ✔
- jar 파일명 `bff-gateway-0.1.0.jar` — build.gradle `bootJar.archiveBaseName`+`version` 과 Dockerfile COPY 일치 ✔
- 포트 8080 — application.yml / Dockerfile EXPOSE·HEALTHCHECK / compose ports·healthcheck / k8s 일치 ✔
