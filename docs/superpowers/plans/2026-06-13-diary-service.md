# tainted-spring-diary Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 일기 CRUD, 서버측 시뮬레이션 KMS envelope 암호화(AES/GCM), 감정·활력 메타데이터 저장, `diary.created` Kafka 이벤트 발행(publish-after-commit)을 제공하는 `diary-service`(Java 23 · Gradle 8.12 · Spring Boot 3.4.x · PostgreSQL)를 RestAssured/Testcontainers로 검증 가능하게 구현한다.

**Architecture:** Spring MVC REST 서비스. 일기 제목·본문은 항상 암호화하여 저장(per-entry 랜덤 DEK + AES/GCM, DEK 자체는 설정 KEK로 다시 암호화 → envelope encryption 시뮬레이션). 복호화 엔드포인트를 통해 mindgraph가 콜백 조회 가능. 시간/ID는 주입 가능한 `Clock`/`IdGenerator` 빈으로 결정론 확보. `diary.created` 이벤트는 **publish-after-commit** 패턴 적용 — `@Transactional` 서비스가 DB 저장 후 Spring `ApplicationEvent`를 발행하고, `@TransactionalEventListener(phase=AFTER_COMMIT)` 리스너가 `KafkaTemplate`으로 `diary.created` JSON을 송신한다. 이 순서 덕분에 DB 커밋이 완료된 뒤에만 Kafka 메시지가 나가므로, mindgraph가 곧바로 `/internal/diaries/{id}/content`를 호출해도 데이터가 반드시 존재한다. 추적/관측 라이브러리는 넣지 않음(actuator health만 유지).

**Tech Stack:** Java 23, Gradle 8.12 (Groovy DSL), Spring Boot 3.4.1, Spring Data JPA, spring-kafka, springdoc-openapi 2.6.0, PostgreSQL 16, Testcontainers(PostgreSQL + Kafka), RestAssured.

---

## 사전 조건

- `tainted-spring-platform` 계획이 완료되어 있어야 통합 실행이 쉽다(단위/통합 테스트는 Testcontainers로 자체 인프라를 띄우므로 platform 없이도 테스트는 통과한다).
- **레포 위치**: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-diary/` (모든 경로는 이 루트 기준).
- **포트**: 8082. **패키지 루트**: `com.tainted.diary`. **데이터스토어**: PostgreSQL DB `diary`.
- **로컬 사전 설치**: JDK 23, Gradle 8.12+ (Gradle 8.12 이상이라야 JDK 23에서 Gradle 자체 실행 가능 — 8.10은 JDK 22까지만 지원), Git, 그리고 **Docker 데몬**(Task 7 통합 테스트의 Testcontainers, Task 8 이미지 빌드, Task 9 compose에 필요). 단, 단위 테스트 `EnvelopeCryptoServiceTest`(Task 3)는 Docker 없이도 동작한다.

## File Structure

```
build.gradle
settings.gradle
Dockerfile
src/main/resources/application.yml
src/main/resources/application-docker.yml
src/main/java/com/tainted/diary/
  DiaryApplication.java
  config/DeterminismConfig.java          # Clock 빈
  config/OpenApiConfig.java
  config/KafkaConfig.java                # KafkaTemplate 설정
  id/IdGenerator.java                    # 인터페이스
  id/UuidIdGenerator.java                # 기본 구현
  crypto/EnvelopeCryptoService.java      # AES/GCM envelope 암/복호화
  crypto/CipherResult.java               # 단일 텍스트 암호화 결과 record (단위테스트용)
  crypto/EnvelopeResult.java             # 제목+본문 단일 DEK 암호화 결과 record
  domain/DiaryEntry.java                 # JPA 엔티티
  domain/DiaryEntryRepository.java
  event/DiaryCreatedEvent.java           # Spring ApplicationEvent (내부)
  event/DiaryCreatedKafkaPayload.java    # Kafka 직렬화 POJO
  event/DiaryEventListener.java          # @TransactionalEventListener AFTER_COMMIT
  service/DiaryService.java              # 비즈니스 로직 (@Transactional)
  web/DiaryController.java               # /internal/diaries/*
  web/dto/CreateDiaryRequest.java
  web/dto/CreateDiaryResponse.java
  web/dto/DiaryMetaResponse.java
  web/dto/DiaryContentResponse.java
  web/dto/UpdateDiaryRequest.java
  error/DiaryNotFoundException.java
  error/GlobalExceptionHandler.java      # RFC 7807
src/test/java/com/tainted/diary/
  crypto/EnvelopeCryptoServiceTest.java  # 단위 (Docker 불필요)
  DiaryIntegrationTest.java              # Testcontainers + RestAssured
```

---

## Task 1: 레포 스캐폴드 + Gradle 빌드 가능한 빈 앱

**Files:**
- Create: `build.gradle`, `settings.gradle`, `src/main/java/com/tainted/diary/DiaryApplication.java`, `src/main/resources/application.yml`, `.gitignore`

- [ ] **Step 1: 디렉토리 생성 + git init**

Run:
```bash
mkdir -p /Users/changjoonbaek/workspace_claude-code/tainted-spring-diary
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-diary
git init
mkdir -p src/main/java/com/tainted/diary src/main/resources src/test/java/com/tainted/diary
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
rootProject.name = 'diary-service'
```

- [ ] **Step 4: `build.gradle`**

`build.gradle`:
```groovy
plugins {
    id 'org.springframework.boot' version '3.4.1'
    id 'io.spring.dependency-management' version '1.1.7'
    id 'java'
}

group = 'com.tainted'
version = '0.1.0'

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(23)
    }
}

jar {
    archiveBaseName = 'diary-service'
}

repositories {
    mavenCentral()
}

dependencies {
    implementation 'org.springframework.boot:spring-boot-starter-web'
    implementation 'org.springframework.boot:spring-boot-starter-data-jpa'
    implementation 'org.springframework.boot:spring-boot-starter-validation'
    implementation 'org.springframework.boot:spring-boot-starter-actuator'
    implementation 'org.springframework.kafka:spring-kafka'
    implementation 'org.springdoc:springdoc-openapi-starter-webmvc-ui:2.6.0'
    runtimeOnly 'org.postgresql:postgresql'

    testImplementation 'org.springframework.boot:spring-boot-starter-test'
    testImplementation 'org.springframework.kafka:spring-kafka-test'
    testImplementation 'org.testcontainers:junit-jupiter:1.21.4'
    testImplementation 'org.testcontainers:postgresql:1.21.4'
    testImplementation 'org.testcontainers:kafka:1.21.4'
    testImplementation 'io.rest-assured:rest-assured'
}

test {
    useJUnitPlatform()
}
```

- [ ] **Step 5: Gradle Wrapper 생성**

Run:
```bash
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-diary
gradle wrapper --gradle-version 8.12
```
Expected: `gradlew`, `gradlew.bat`, `gradle/wrapper/gradle-wrapper.jar`, `gradle/wrapper/gradle-wrapper.properties` 생성.

이후 모든 빌드 명령은 `./gradlew` 사용.

- [ ] **Step 6: 메인 클래스 + 최소 설정**

`src/main/java/com/tainted/diary/DiaryApplication.java`:
```java
package com.tainted.diary;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DiaryApplication {
    public static void main(String[] args) {
        SpringApplication.run(DiaryApplication.class, args);
    }
}
```

`src/main/resources/application.yml`:
```yaml
server:
  port: 8082
spring:
  application:
    name: diary-service
  datasource:
    url: jdbc:postgresql://localhost:5432/diary
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
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
  mvc:
    problemdetails:
      enabled: true
diary:
  kek-base64: "YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoxMjM0NTY="
management:
  endpoints:
    web:
      exposure:
        include: health
```

> `diary.kek-base64` 기본값은 Base64(32 bytes UTF-8 문자열 `abcdefghijklmnopqrstuvwxyz123456`). 프로덕션에서는 반드시 환경변수로 교체해야 한다.

- [ ] **Step 7: 컴파일 확인**

Run: `./gradlew -q compileJava`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 8: 커밋**

```bash
git add -A
git commit -m "chore: scaffold diary-service (buildable empty app)"
```

---

## Task 2: 결정론 빈 (Clock, IdGenerator)

**Files:**
- Create: `id/IdGenerator.java`, `id/UuidIdGenerator.java`, `config/DeterminismConfig.java`

- [ ] **Step 1: `IdGenerator` 인터페이스**

`src/main/java/com/tainted/diary/id/IdGenerator.java`:
```java
package com.tainted.diary.id;

/** 식별자 생성 추상화. 테스트에서 결정론적 구현으로 교체 가능. */
public interface IdGenerator {
    String newId();
}
```

- [ ] **Step 2: 기본 UUID 구현**

`src/main/java/com/tainted/diary/id/UuidIdGenerator.java`:
```java
package com.tainted.diary.id;

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

`src/main/java/com/tainted/diary/config/DeterminismConfig.java`:
```java
package com.tainted.diary.config;

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

## Task 3: Envelope 암호화 서비스 (TDD)

**Files:**
- Create: `crypto/CipherResult.java`, `crypto/EnvelopeResult.java`, `crypto/EnvelopeCryptoService.java`
- Test: `src/test/java/com/tainted/diary/crypto/EnvelopeCryptoServiceTest.java`

- [ ] **Step 1: 실패하는 테스트 작성**

`src/test/java/com/tainted/diary/crypto/EnvelopeCryptoServiceTest.java`:
```java
package com.tainted.diary.crypto;

import org.junit.jupiter.api.Test;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

class EnvelopeCryptoServiceTest {

    // 테스트용 32바이트 KEK (Base64 인코딩)
    private static final String KEK_B64 =
            Base64.getEncoder().encodeToString("test-kek-32bytes-1234567890abcd!".getBytes());

    private final EnvelopeCryptoService crypto = new EnvelopeCryptoService(KEK_B64);

    @Test
    void roundTripAscii() {
        String plaintext = "Hello, diary!";
        CipherResult result = crypto.encrypt(plaintext);

        assertNotNull(result.ciphertext(), "ciphertext must not be null");
        assertNotNull(result.iv(), "iv must not be null");
        assertNotNull(result.encryptedDek(), "encryptedDek must not be null");
        assertNotNull(result.dekIv(), "dekIv must not be null");

        String decrypted = crypto.decrypt(
                result.ciphertext(), result.iv(),
                result.encryptedDek(), result.dekIv());
        assertEquals(plaintext, decrypted, "복호화 결과가 원문과 같아야 한다");
    }

    @Test
    void roundTripKorean() {
        String plaintext = "오늘 하루도 참 힘들었다. 그래도 괜찮아, 내일은 더 나을 거야. 🌱";
        CipherResult result = crypto.encrypt(plaintext);

        String decrypted = crypto.decrypt(
                result.ciphertext(), result.iv(),
                result.encryptedDek(), result.dekIv());
        assertEquals(plaintext, decrypted, "한국어 유니코드 텍스트 라운드트립 일치해야 한다");
    }

    @Test
    void eachEncryptProducesDifferentCiphertext() {
        String plaintext = "같은 입력";
        CipherResult r1 = crypto.encrypt(plaintext);
        CipherResult r2 = crypto.encrypt(plaintext);

        // 랜덤 DEK/IV 사용이므로 암호문은 매번 달라야 한다
        assertNotEquals(r1.ciphertext(), r2.ciphertext(),
                "랜덤 DEK/IV 덕분에 같은 입력도 암호문이 달라야 한다");

        // 양쪽 모두 복호화 성공해야 한다
        assertEquals(plaintext, crypto.decrypt(r1.ciphertext(), r1.iv(), r1.encryptedDek(), r1.dekIv()));
        assertEquals(plaintext, crypto.decrypt(r2.ciphertext(), r2.iv(), r2.encryptedDek(), r2.dekIv()));
    }

    @Test
    void encryptFieldsSharesOneDekForTitleAndContent() {
        String title = "오늘의 제목";
        String content = "오늘 하루의 본문 내용 🌱";
        EnvelopeResult env = crypto.encryptFields(title, content);

        // 제목·본문은 하나의 DEK로 암호화되므로, 동일한 encryptedDek/dekIv 로 양쪽 모두 복호화돼야 한다.
        assertEquals(title, crypto.decrypt(
                env.titleCipher(), env.titleIv(), env.encryptedDek(), env.dekIv()));
        assertEquals(content, crypto.decrypt(
                env.contentCipher(), env.contentIv(), env.encryptedDek(), env.dekIv()));
    }
}
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `./gradlew -q test --tests "com.tainted.diary.crypto.EnvelopeCryptoServiceTest"`
Expected: FAIL — `CipherResult`, `EnvelopeCryptoService` 미존재로 컴파일 에러.

- [ ] **Step 3: 최소 구현**

`src/main/java/com/tainted/diary/crypto/CipherResult.java`:
```java
package com.tainted.diary.crypto;

/**
 * 암호화 결과. ciphertext·iv는 텍스트 암호화 산출물,
 * encryptedDek·dekIv는 DEK 자체의 KEK 암호화 산출물.
 * 모든 필드는 Base64 문자열.
 */
public record CipherResult(
        String ciphertext,
        String iv,
        String encryptedDek,
        String dekIv
) {}
```

`src/main/java/com/tainted/diary/crypto/EnvelopeResult.java`:
```java
package com.tainted.diary.crypto;

/**
 * 제목·본문을 하나의 DEK로 함께 암호화한 결과.
 * DiaryEntry 는 encryptedDek 를 1개만 보관하므로, 두 필드가 같은 DEK로 암호화되어야
 * 동일 encryptedDek 로 양쪽을 복호화할 수 있다. 모든 필드는 Base64 문자열.
 */
public record EnvelopeResult(
        String titleCipher,
        String titleIv,
        String contentCipher,
        String contentIv,
        String encryptedDek,
        String dekIv
) {}
```

`src/main/java/com/tainted/diary/crypto/EnvelopeCryptoService.java`:
```java
package com.tainted.diary.crypto;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * 서버측 시뮬레이션 KMS Envelope Encryption.
 *
 * <ul>
 *   <li>per-entry 랜덤 256-bit DEK 생성 → AES/GCM으로 plaintext 암호화</li>
 *   <li>DEK 자체를 설정 KEK(256-bit)로 AES/GCM 암호화 → encryptedDek 저장</li>
 *   <li>복호화 시: encryptedDek → DEK 복원 → plaintext 복원</li>
 * </ul>
 *
 * 모든 바이너리 값은 Base64(URL-safe 아닌 기본) 문자열로 교환.
 */
@Component
public class EnvelopeCryptoService {

    private static final String AES_GCM = "AES/GCM/NoPadding";
    private static final int GCM_IV_LEN = 12;   // bytes
    private static final int GCM_TAG_LEN = 128;  // bits

    private final SecretKey kek;
    private final SecureRandom random = new SecureRandom();

    public EnvelopeCryptoService(@Value("${diary.kek-base64}") String kekBase64) {
        byte[] kekBytes = Base64.getDecoder().decode(kekBase64);
        if (kekBytes.length != 32) {
            throw new IllegalArgumentException(
                    "KEK must be 32 bytes (256 bits); got " + kekBytes.length);
        }
        this.kek = new SecretKeySpec(kekBytes, "AES");
    }

    /**
     * plaintext를 envelope 암호화하여 {@link CipherResult}를 반환한다.
     * DEK·IV가 매 호출마다 랜덤이므로 같은 입력도 암호문이 다르다.
     */
    public CipherResult encrypt(String plaintext) {
        try {
            // 1. 랜덤 DEK 생성
            KeyGenerator kg = KeyGenerator.getInstance("AES");
            kg.init(256, random);
            SecretKey dek = kg.generateKey();

            // 2. plaintext → DEK로 암호화
            byte[] contentIv = randomIv();
            byte[] ciphertext = aesGcmEncrypt(
                    plaintext.getBytes(StandardCharsets.UTF_8), dek, contentIv);

            // 3. DEK → KEK로 암호화
            byte[] dekIv = randomIv();
            byte[] encryptedDek = aesGcmEncrypt(dek.getEncoded(), kek, dekIv);

            return new CipherResult(
                    b64(ciphertext),
                    b64(contentIv),
                    b64(encryptedDek),
                    b64(dekIv)
            );
        } catch (Exception e) {
            throw new IllegalStateException("Encryption failed", e);
        }
    }

    /**
     * 제목·본문을 <b>하나의 DEK</b>로 함께 암호화한다.
     * DiaryEntry 가 encryptedDek 를 1개만 보관하므로 두 필드는 동일 DEK로 암호화돼야 하며,
     * 그래야 같은 encryptedDek/dekIv 로 양쪽 모두 복호화할 수 있다.
     * (IV는 필드마다 독립이므로 동일 DEK 재사용에도 GCM 안전성이 유지된다.)
     */
    public EnvelopeResult encryptFields(String title, String content) {
        try {
            // 1. 단일 랜덤 DEK 생성 (제목·본문 공용)
            KeyGenerator kg = KeyGenerator.getInstance("AES");
            kg.init(256, random);
            SecretKey dek = kg.generateKey();

            // 2. 제목·본문을 각각 독립 IV로, 그러나 동일 DEK로 암호화
            byte[] titleIv = randomIv();
            byte[] titleCipher = aesGcmEncrypt(title.getBytes(StandardCharsets.UTF_8), dek, titleIv);
            byte[] contentIv = randomIv();
            byte[] contentCipher = aesGcmEncrypt(content.getBytes(StandardCharsets.UTF_8), dek, contentIv);

            // 3. DEK를 KEK로 1회 암호화
            byte[] dekIv = randomIv();
            byte[] encryptedDek = aesGcmEncrypt(dek.getEncoded(), kek, dekIv);

            return new EnvelopeResult(
                    b64(titleCipher), b64(titleIv),
                    b64(contentCipher), b64(contentIv),
                    b64(encryptedDek), b64(dekIv)
            );
        } catch (Exception e) {
            throw new IllegalStateException("Field encryption failed", e);
        }
    }

    /**
     * envelope 복호화. encryptedDek → DEK 복원 → plaintext 복원.
     */
    public String decrypt(String ciphertext, String iv,
                          String encryptedDek, String dekIv) {
        try {
            // 1. KEK로 DEK 복호화
            byte[] dekBytes = aesGcmDecrypt(
                    b64d(encryptedDek), kek, b64d(dekIv));
            SecretKey dek = new SecretKeySpec(dekBytes, "AES");

            // 2. DEK로 plaintext 복호화
            byte[] plain = aesGcmDecrypt(b64d(ciphertext), dek, b64d(iv));
            return new String(plain, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("Decryption failed", e);
        }
    }

    // ── 내부 헬퍼 ──────────────────────────────────────────────────────────

    private byte[] aesGcmEncrypt(byte[] data, SecretKey key, byte[] iv) throws Exception {
        Cipher cipher = Cipher.getInstance(AES_GCM);
        cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_LEN, iv));
        return cipher.doFinal(data);
    }

    private byte[] aesGcmDecrypt(byte[] data, SecretKey key, byte[] iv) throws Exception {
        Cipher cipher = Cipher.getInstance(AES_GCM);
        cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_LEN, iv));
        return cipher.doFinal(data);
    }

    private byte[] randomIv() {
        byte[] iv = new byte[GCM_IV_LEN];
        random.nextBytes(iv);
        return iv;
    }

    private static String b64(byte[] bytes) {
        return Base64.getEncoder().encodeToString(bytes);
    }

    private static byte[] b64d(String s) {
        return Base64.getDecoder().decode(s);
    }
}
```

- [ ] **Step 4: 테스트 통과 확인**

Run: `./gradlew -q test --tests "com.tainted.diary.crypto.EnvelopeCryptoServiceTest"`
Expected: PASS (3 tests — roundTripAscii, roundTripKorean, eachEncryptProducesDifferentCiphertext).

- [ ] **Step 5: 커밋**

```bash
git add -A
git commit -m "feat: envelope crypto service (AES/GCM, TDD)"
```

---

## Task 4: DiaryEntry 도메인 (JPA)

**Files:**
- Create: `domain/DiaryEntry.java`, `domain/DiaryEntryRepository.java`

- [ ] **Step 1: 엔티티**

`src/main/java/com/tainted/diary/domain/DiaryEntry.java`:
```java
package com.tainted.diary.domain;

import jakarta.persistence.*;
import java.time.Instant;

/**
 * 일기 엔티티. 제목·본문은 envelope 암호화 형태로만 저장된다.
 * 복호화 값은 DB에 절대 기록되지 않는다.
 */
@Entity
@Table(name = "diary_entry")
public class DiaryEntry {

    @Id
    @Column(length = 64)
    private String id;

    @Column(name = "user_id", nullable = false, length = 64)
    private String userId;

    // 암호화된 제목
    @Column(name = "encrypted_title", nullable = false, columnDefinition = "TEXT")
    private String encryptedTitle;

    @Column(name = "title_iv", nullable = false, length = 32)
    private String titleIv;

    // 암호화된 본문
    @Column(name = "encrypted_content", nullable = false, columnDefinition = "TEXT")
    private String encryptedContent;

    @Column(name = "content_iv", nullable = false, length = 32)
    private String contentIv;

    // per-entry DEK (KEK로 암호화된 상태)
    @Column(name = "encrypted_dek", nullable = false, columnDefinition = "TEXT")
    private String encryptedDek;

    @Column(name = "dek_iv", nullable = false, length = 32)
    private String dekIv;

    @Column(name = "primary_emotion", nullable = false, length = 64)
    private String primaryEmotion;

    @Column(name = "energy_score", nullable = false)
    private int energyScore;

    @Column(name = "timestamp", nullable = false)
    private Instant timestamp;

    protected DiaryEntry() {}

    public DiaryEntry(String id, String userId,
                      String encryptedTitle, String titleIv,
                      String encryptedContent, String contentIv,
                      String encryptedDek, String dekIv,
                      String primaryEmotion, int energyScore,
                      Instant timestamp) {
        this.id = id;
        this.userId = userId;
        this.encryptedTitle = encryptedTitle;
        this.titleIv = titleIv;
        this.encryptedContent = encryptedContent;
        this.contentIv = contentIv;
        this.encryptedDek = encryptedDek;
        this.dekIv = dekIv;
        this.primaryEmotion = primaryEmotion;
        this.energyScore = energyScore;
        this.timestamp = timestamp;
    }

    // ── 갱신 메서드 ──────────────────────────────────────────────────────────

    public void update(String encryptedTitle, String titleIv,
                       String encryptedContent, String contentIv,
                       String encryptedDek, String dekIv,
                       String primaryEmotion, int energyScore) {
        this.encryptedTitle = encryptedTitle;
        this.titleIv = titleIv;
        this.encryptedContent = encryptedContent;
        this.contentIv = contentIv;
        this.encryptedDek = encryptedDek;
        this.dekIv = dekIv;
        this.primaryEmotion = primaryEmotion;
        this.energyScore = energyScore;
    }

    // ── Getters ─────────────────────────────────────────────────────────────

    public String getId() { return id; }
    public String getUserId() { return userId; }
    public String getEncryptedTitle() { return encryptedTitle; }
    public String getTitleIv() { return titleIv; }
    public String getEncryptedContent() { return encryptedContent; }
    public String getContentIv() { return contentIv; }
    public String getEncryptedDek() { return encryptedDek; }
    public String getDekIv() { return dekIv; }
    public String getPrimaryEmotion() { return primaryEmotion; }
    public int getEnergyScore() { return energyScore; }
    public Instant getTimestamp() { return timestamp; }
}
```

- [ ] **Step 2: 리포지토리**

`src/main/java/com/tainted/diary/domain/DiaryEntryRepository.java`:
```java
package com.tainted.diary.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;

public interface DiaryEntryRepository extends JpaRepository<DiaryEntry, String> {
    List<DiaryEntry> findByUserIdOrderByTimestampDesc(String userId);
}
```

- [ ] **Step 3: 컴파일 + 커밋**

Run: `./gradlew -q compileJava`
Expected: BUILD SUCCESSFUL.
```bash
git add -A
git commit -m "feat: DiaryEntry entity and repository"
```

---

## Task 5: Kafka 설정 + publish-after-commit 이벤트

**Files:**
- Create: `config/KafkaConfig.java`, `event/DiaryCreatedEvent.java`, `event/DiaryCreatedKafkaPayload.java`, `event/DiaryEventListener.java`

- [ ] **Step 1: Kafka 토픽 설정**

`src/main/java/com/tainted/diary/config/KafkaConfig.java`:
```java
package com.tainted.diary.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    /**
     * 브로커에 토픽이 없을 때 자동 생성. 이미 있으면 멱등적으로 무시.
     */
    @Bean
    public NewTopic diaryCreatedTopic() {
        return TopicBuilder.name("diary.created")
                .partitions(1)
                .replicas(1)
                .build();
    }
}
```

- [ ] **Step 2: 내부 ApplicationEvent**

`src/main/java/com/tainted/diary/event/DiaryCreatedEvent.java`:
```java
package com.tainted.diary.event;

/**
 * diary-service 내부에서만 사용하는 Spring ApplicationEvent 페이로드.
 * @TransactionalEventListener(AFTER_COMMIT) 이 이 이벤트를 수신해 Kafka로 전달한다.
 */
public record DiaryCreatedEvent(
        String eventId,
        String userId,
        String diaryId,
        String primaryEmotion,
        int energyScore,
        String occurredAt
) {}
```

- [ ] **Step 3: Kafka 직렬화 POJO**

`src/main/java/com/tainted/diary/event/DiaryCreatedKafkaPayload.java`:
```java
package com.tainted.diary.event;

/**
 * diary.created 토픽에 발행되는 JSON 페이로드.
 * 스키마 카탈로그: {eventId, userId, diaryId, primaryEmotion, energyScore, occurredAt}
 */
public record DiaryCreatedKafkaPayload(
        String eventId,
        String userId,
        String diaryId,
        String primaryEmotion,
        int energyScore,
        String occurredAt
) {}
```

- [ ] **Step 4: publish-after-commit 리스너**

`src/main/java/com/tainted/diary/event/DiaryEventListener.java`:
```java
package com.tainted.diary.event;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * DB 트랜잭션이 커밋된 이후에만 Kafka로 diary.created 를 발행한다.
 *
 * 이유: mindgraph 가 diary.created 를 소비한 직후 /internal/diaries/{id}/content 를
 * 호출하는데, 트랜잭션 커밋 전에 Kafka 메시지가 나가면 해당 일기를 조회할 수 없는
 * race condition 이 발생한다. AFTER_COMMIT phase 는 이 race 를 구조적으로 방지한다.
 *
 * 참고: AFTER_COMMIT 리스너는 활성 트랜잭션 밖에서 실행된다. KafkaTemplate 은
 * 트랜잭션 없이 독립적으로 send 를 수행한다.
 */
@Component
public class DiaryEventListener {

    private static final String TOPIC = "diary.created";

    private final KafkaTemplate<String, DiaryCreatedKafkaPayload> kafkaTemplate;

    public DiaryEventListener(KafkaTemplate<String, DiaryCreatedKafkaPayload> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void onDiaryCreated(DiaryCreatedEvent event) {
        DiaryCreatedKafkaPayload payload = new DiaryCreatedKafkaPayload(
                event.eventId(),
                event.userId(),
                event.diaryId(),
                event.primaryEmotion(),
                event.energyScore(),
                event.occurredAt()
        );
        // key = diaryId → 같은 일기의 이벤트가 같은 파티션으로 순서 보장
        kafkaTemplate.send(TOPIC, event.diaryId(), payload);
    }
}
```

- [ ] **Step 5: 컴파일 + 커밋**

Run: `./gradlew -q compileJava`
Expected: BUILD SUCCESSFUL.
```bash
git add -A
git commit -m "feat: Kafka config and publish-after-commit event listener"
```

---

## Task 6: 서비스 + DTO + 컨트롤러 + RFC 7807 에러 + OpenAPI

**Files:**
- Create: `web/dto/CreateDiaryRequest.java`, `web/dto/CreateDiaryResponse.java`, `web/dto/DiaryMetaResponse.java`, `web/dto/DiaryContentResponse.java`, `web/dto/UpdateDiaryRequest.java`
- Create: `error/DiaryNotFoundException.java`, `error/GlobalExceptionHandler.java`
- Create: `service/DiaryService.java`, `web/DiaryController.java`, `config/OpenApiConfig.java`

- [ ] **Step 1: DTO 5종**

`src/main/java/com/tainted/diary/web/dto/CreateDiaryRequest.java`:
```java
package com.tainted.diary.web.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record CreateDiaryRequest(
        @NotBlank String userId,
        @NotBlank String title,
        @NotBlank String content,
        @NotBlank String primaryEmotion,
        @NotNull @Min(1) @Max(10) Integer energyScore
) {}
```

`src/main/java/com/tainted/diary/web/dto/CreateDiaryResponse.java`:
```java
package com.tainted.diary.web.dto;

public record CreateDiaryResponse(
        String diaryId,
        String userId,
        String primaryEmotion,
        int energyScore,
        String timestamp
) {}
```

`src/main/java/com/tainted/diary/web/dto/DiaryMetaResponse.java`:
```java
package com.tainted.diary.web.dto;

/**
 * 메타데이터 응답 — 복호화된 제목/본문은 포함하지 않는다.
 */
public record DiaryMetaResponse(
        String diaryId,
        String userId,
        String primaryEmotion,
        int energyScore,
        String timestamp
) {}
```

`src/main/java/com/tainted/diary/web/dto/DiaryContentResponse.java`:
```java
package com.tainted.diary.web.dto;

/**
 * 서버측 복호화 후 반환되는 일기 본문 응답.
 */
public record DiaryContentResponse(
        String diaryId,
        String title,
        String content
) {}
```

`src/main/java/com/tainted/diary/web/dto/UpdateDiaryRequest.java`:
```java
package com.tainted.diary.web.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record UpdateDiaryRequest(
        @NotBlank String title,
        @NotBlank String content,
        @NotBlank String primaryEmotion,
        @NotNull @Min(1) @Max(10) Integer energyScore
) {}
```

- [ ] **Step 2: `DiaryNotFoundException` + 에러 핸들러**

`src/main/java/com/tainted/diary/error/DiaryNotFoundException.java`:
```java
package com.tainted.diary.error;

public class DiaryNotFoundException extends RuntimeException {
    public DiaryNotFoundException(String message) {
        super(message);
    }
}
```

`src/main/java/com/tainted/diary/error/GlobalExceptionHandler.java`:
```java
package com.tainted.diary.error;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(DiaryNotFoundException.class)
    public ProblemDetail handleDiaryNotFound(DiaryNotFoundException ex) {
        ProblemDetail pd = ProblemDetail.forStatus(HttpStatus.NOT_FOUND);
        pd.setTitle("Diary not found");
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

- [ ] **Step 3: `DiaryService` (비즈니스 로직)**

`src/main/java/com/tainted/diary/service/DiaryService.java`:
```java
package com.tainted.diary.service;

import com.tainted.diary.crypto.EnvelopeResult;
import com.tainted.diary.crypto.EnvelopeCryptoService;
import com.tainted.diary.domain.DiaryEntry;
import com.tainted.diary.domain.DiaryEntryRepository;
import com.tainted.diary.error.DiaryNotFoundException;
import com.tainted.diary.event.DiaryCreatedEvent;
import com.tainted.diary.id.IdGenerator;
import com.tainted.diary.web.dto.*;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Instant;

@Service
public class DiaryService {

    private final DiaryEntryRepository repository;
    private final EnvelopeCryptoService crypto;
    private final IdGenerator idGenerator;
    private final Clock clock;
    private final ApplicationEventPublisher eventPublisher;

    public DiaryService(DiaryEntryRepository repository,
                        EnvelopeCryptoService crypto,
                        IdGenerator idGenerator,
                        Clock clock,
                        ApplicationEventPublisher eventPublisher) {
        this.repository = repository;
        this.crypto = crypto;
        this.idGenerator = idGenerator;
        this.clock = clock;
        this.eventPublisher = eventPublisher;
    }

    /**
     * 일기를 저장하고 AFTER_COMMIT 이벤트를 예약한다.
     * 트랜잭션 커밋 후 {@link com.tainted.diary.event.DiaryEventListener} 가
     * diary.created 를 Kafka 로 발행한다.
     */
    @Transactional
    public CreateDiaryResponse create(CreateDiaryRequest request) {
        String diaryId = idGenerator.newId();
        Instant now = Instant.now(clock);

        // 제목·본문을 하나의 DEK로 암호화 → 단일 encryptedDek 로 양쪽 복호화 가능
        EnvelopeResult env = crypto.encryptFields(request.title(), request.content());

        DiaryEntry entry = new DiaryEntry(
                diaryId,
                request.userId(),
                env.titleCipher(), env.titleIv(),
                env.contentCipher(), env.contentIv(),
                env.encryptedDek(), env.dekIv(),
                request.primaryEmotion(),
                request.energyScore(),
                now
        );
        repository.save(entry);

        // 트랜잭션 AFTER_COMMIT 시 Kafka 발행 예약
        String eventId = idGenerator.newId();
        eventPublisher.publishEvent(new DiaryCreatedEvent(
                eventId,
                request.userId(),
                diaryId,
                request.primaryEmotion(),
                request.energyScore(),
                now.toString()
        ));

        return new CreateDiaryResponse(
                diaryId,
                request.userId(),
                request.primaryEmotion(),
                request.energyScore(),
                now.toString()
        );
    }

    @Transactional(readOnly = true)
    public DiaryMetaResponse getMeta(String diaryId) {
        DiaryEntry entry = findOrThrow(diaryId);
        return toMeta(entry);
    }

    /**
     * 서버측 복호화 — mindgraph 가 그래프 생성을 위해 호출한다.
     */
    @Transactional(readOnly = true)
    public DiaryContentResponse getContent(String diaryId) {
        DiaryEntry entry = findOrThrow(diaryId);

        // 제목·본문은 하나의 DEK로 암호화되어 있으므로, 동일 encryptedDek/dekIv 로 각각 복호화
        String title = crypto.decrypt(
                entry.getEncryptedTitle(), entry.getTitleIv(),
                entry.getEncryptedDek(), entry.getDekIv());
        String content = crypto.decrypt(
                entry.getEncryptedContent(), entry.getContentIv(),
                entry.getEncryptedDek(), entry.getDekIv());

        return new DiaryContentResponse(diaryId, title, content);
    }

    @Transactional
    public DiaryMetaResponse update(String diaryId, UpdateDiaryRequest request) {
        DiaryEntry entry = findOrThrow(diaryId);

        EnvelopeResult env = crypto.encryptFields(request.title(), request.content());

        entry.update(
                env.titleCipher(), env.titleIv(),
                env.contentCipher(), env.contentIv(),
                env.encryptedDek(), env.dekIv(),
                request.primaryEmotion(),
                request.energyScore()
        );

        return toMeta(entry);
    }

    @Transactional
    public void delete(String diaryId) {
        DiaryEntry entry = findOrThrow(diaryId);
        repository.delete(entry);
    }

    // ── 내부 헬퍼 ──────────────────────────────────────────────────────────

    private DiaryEntry findOrThrow(String diaryId) {
        return repository.findById(diaryId)
                .orElseThrow(() -> new DiaryNotFoundException("diary not found: " + diaryId));
    }

    private DiaryMetaResponse toMeta(DiaryEntry e) {
        return new DiaryMetaResponse(
                e.getId(),
                e.getUserId(),
                e.getPrimaryEmotion(),
                e.getEnergyScore(),
                e.getTimestamp().toString()
        );
    }
}
```

> **설계 주의**: 제목과 본문을 동일 DEK로 암호화하면 두 암호문이 동일 키 + 서로 다른 IV를 공유하게 된다. 이는 AES/GCM 에서는 허용되지만(IV가 다르면 안전), 위 구현에서는 명확성을 위해 **`titleResult`의 encryptedDek·dekIv만 저장하고 복호화 시 이를 재사용**하는 형태로 단순화하였다. 엄밀한 구현에서는 제목·본문 각각 독립 DEK를 써야 한다 — 테스트베드이므로 이 단순화를 허용한다.

- [ ] **Step 4: `DiaryController`**

`src/main/java/com/tainted/diary/web/DiaryController.java`:
```java
package com.tainted.diary.web;

import com.tainted.diary.service.DiaryService;
import com.tainted.diary.web.dto.*;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/internal/diaries")
public class DiaryController {

    private final DiaryService diaryService;

    public DiaryController(DiaryService diaryService) {
        this.diaryService = diaryService;
    }

    /** 일기 생성 — 201 Created */
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public CreateDiaryResponse create(@Valid @RequestBody CreateDiaryRequest request) {
        return diaryService.create(request);
    }

    /** 메타데이터 조회 (복호화 없음) */
    @GetMapping("/{id}")
    public DiaryMetaResponse getMeta(@PathVariable String id) {
        return diaryService.getMeta(id);
    }

    /** 서버측 복호화 본문 조회 — mindgraph 콜백용 */
    @GetMapping("/{id}/content")
    public DiaryContentResponse getContent(@PathVariable String id) {
        return diaryService.getContent(id);
    }

    /** 일기 수정 */
    @PutMapping("/{id}")
    public DiaryMetaResponse update(@PathVariable String id,
                                    @Valid @RequestBody UpdateDiaryRequest request) {
        return diaryService.update(id, request);
    }

    /** 일기 삭제 — 204 No Content */
    @DeleteMapping("/{id}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable String id) {
        diaryService.delete(id);
    }
}
```

- [ ] **Step 5: OpenAPI 설정**

`src/main/java/com/tainted/diary/config/OpenApiConfig.java`:
```java
package com.tainted.diary.config;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenApiConfig {
    @Bean
    public OpenAPI diaryOpenApi() {
        return new OpenAPI().info(new Info()
                .title("diary-service API")
                .version("0.1.0")
                .description("일기 CRUD, 서버측 envelope 암호화/복호화, diary.created 이벤트 발행"));
    }
}
```

- [ ] **Step 6: 컴파일 + 커밋**

Run: `./gradlew -q compileJava`
Expected: BUILD SUCCESSFUL.
```bash
git add -A
git commit -m "feat: diary service, controller, DTOs, RFC7807 error handling, OpenAPI"
```

---

## Task 7: 통합 테스트 (Testcontainers + RestAssured + Kafka 소비 검증)

**Files:**
- Test: `src/test/java/com/tainted/diary/DiaryIntegrationTest.java`

- [ ] **Step 1: 통합 테스트 작성**

`src/test/java/com/tainted/diary/DiaryIntegrationTest.java`:
```java
package com.tainted.diary;

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Testcontainers
class DiaryIntegrationTest {

    @Container
    static PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("diary")
                    .withUsername("postgres")
                    .withPassword("postgrespw");

    @Container
    static KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", postgres::getJdbcUrl);
        r.add("spring.datasource.username", postgres::getUsername);
        r.add("spring.datasource.password", postgres::getPassword);
        r.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @LocalServerPort
    int port;

    private KafkaConsumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        RestAssured.port = port;

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of("diary.created"));
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void createDiary_returns201WithDiaryId() {
        given()
            .contentType(ContentType.JSON)
            .body("""
                    {
                      "userId": "user-abc",
                      "title": "오늘의 일기",
                      "content": "오늘은 참 좋은 날이었다.",
                      "primaryEmotion": "JOY",
                      "energyScore": 8
                    }
                    """)
        .when()
            .post("/internal/diaries")
        .then()
            .statusCode(201)
            .body("diaryId", not(emptyOrNullString()))
            .body("userId", equalTo("user-abc"))
            .body("primaryEmotion", equalTo("JOY"))
            .body("energyScore", equalTo(8));
    }

    @Test
    void createThenGetContent_roundTripKorean() {
        // (a) 일기 생성
        String diaryId = given()
            .contentType(ContentType.JSON)
            .body("""
                    {
                      "userId": "user-kor",
                      "title": "한국어 제목 — 오늘도 수고했어",
                      "content": "마음이 복잡하지만 그래도 내일은 더 나을 거야. 🌱",
                      "primaryEmotion": "MELANCHOLY",
                      "energyScore": 4
                    }
                    """)
        .when()
            .post("/internal/diaries")
        .then()
            .statusCode(201)
            .extract().path("diaryId");

        // (b) 복호화 본문 조회 — DB 저장 후 AES/GCM 라운드트립 검증
        given()
            .when()
            .get("/internal/diaries/" + diaryId + "/content")
        .then()
            .statusCode(200)
            .body("diaryId", equalTo(diaryId))
            .body("title", equalTo("한국어 제목 — 오늘도 수고했어"))
            .body("content", equalTo("마음이 복잡하지만 그래도 내일은 더 나을 거야. 🌱"));
    }

    @Test
    void createDiary_publishesDiaryCreatedKafkaEvent() throws InterruptedException {
        // 일기 생성
        Map<String, Object> created = given()
            .contentType(ContentType.JSON)
            .body("""
                    {
                      "userId": "user-kafka",
                      "title": "Kafka 이벤트 테스트",
                      "content": "이 일기는 Kafka 이벤트 발행을 확인하기 위해 작성되었다.",
                      "primaryEmotion": "CURIOUS",
                      "energyScore": 7
                    }
                    """)
        .when()
            .post("/internal/diaries")
        .then()
            .statusCode(201)
            .extract().as(java.util.HashMap.class);

        String expectedDiaryId = (String) created.get("diaryId");
        String expectedUserId = (String) created.get("userId");

        // diary.created 토픽에서 메시지 수신 대기 (최대 10초)
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 10_000;
        while (records.isEmpty() && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(300)).forEach(records::add);
        }

        assertFalse(records.isEmpty(), "diary.created 토픽에 메시지가 없다");

        // 발행된 JSON 에 diaryId·userId·primaryEmotion·energyScore 포함 여부 검증
        String json = records.get(0).value();
        assertAll(
            () -> assertTrue(json.contains("\"diaryId\":\"" + expectedDiaryId + "\""),
                    "diaryId 포함 여부: " + json),
            () -> assertTrue(json.contains("\"userId\":\"" + expectedUserId + "\""),
                    "userId 포함 여부: " + json),
            () -> assertTrue(json.contains("\"primaryEmotion\":\"CURIOUS\""),
                    "primaryEmotion 포함 여부: " + json),
            () -> assertTrue(json.contains("\"energyScore\":7"),
                    "energyScore 포함 여부: " + json)
        );
    }

    @Test
    void getMeta_doesNotExposeDecryptedContent() {
        String diaryId = given()
            .contentType(ContentType.JSON)
            .body("""
                    {
                      "userId": "user-meta",
                      "title": "비밀 제목",
                      "content": "비밀 본문",
                      "primaryEmotion": "FEAR",
                      "energyScore": 3
                    }
                    """)
        .when()
            .post("/internal/diaries")
        .then()
            .statusCode(201)
            .extract().path("diaryId");

        // 메타 응답에 title·content 필드가 없어야 한다
        given()
            .when()
            .get("/internal/diaries/" + diaryId)
        .then()
            .statusCode(200)
            .body("diaryId", equalTo(diaryId))
            .body("primaryEmotion", equalTo("FEAR"))
            .body("title", nullValue())
            .body("content", nullValue());
    }

    @Test
    void unknownDiaryReturns404ProblemJson() {
        given()
            .when()
            .get("/internal/diaries/no-such-id")
        .then()
            .statusCode(404)
            .contentType("application/problem+json")
            .body("title", equalTo("Diary not found"));
    }
}
```

- [ ] **Step 2: 전체 테스트 실행 (Docker 필요)**

Run: `./gradlew -q test`
Expected: PASS — `EnvelopeCryptoServiceTest`(3) + `DiaryIntegrationTest`(5) 모두 통과. (Docker 데몬이 떠 있어야 Testcontainers 동작.)

- [ ] **Step 3: 커밋**

```bash
git add -A
git commit -m "test: Testcontainers + RestAssured integration tests (Postgres + Kafka)"
```

---

## Task 8: Dockerfile + docker 프로파일

**Files:**
- Create: `Dockerfile`, `src/main/resources/application-docker.yml`

- [ ] **Step 1: docker 프로파일 설정**

> `${POSTGRES_PASSWORD:postgrespw}` 의 값은 Task 9에서 compose `diary` 서비스의 `environment:` 로 주입된다(미주입 시 기본값 `postgrespw`). KEK 역시 환경변수로 오버라이드 가능하다.

`src/main/resources/application-docker.yml`:
```yaml
spring:
  datasource:
    url: jdbc:postgresql://postgres:5432/diary
    username: postgres
    password: ${POSTGRES_PASSWORD:postgrespw}
  kafka:
    bootstrap-servers: kafka:29092
diary:
  kek-base64: ${DIARY_KEK_BASE64:YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoxMjM0NTY=}
```

- [ ] **Step 2: 멀티스테이지 Dockerfile (non-root)**

`Dockerfile`:
```dockerfile
# ── build stage ──────────────────────────────────────────────────────────────
FROM gradle:8.12-jdk23 AS build
WORKDIR /app
# 의존성 캐시 레이어를 먼저 만들어 재빌드 시간 단축
COPY build.gradle settings.gradle ./
RUN gradle dependencies --no-daemon -q || true
COPY src ./src
RUN gradle bootJar --no-daemon -q -x test

# ── runtime stage ─────────────────────────────────────────────────────────────
FROM eclipse-temurin:23-jre
WORKDIR /app
# curl: compose/k8s 헬스체크가 /actuator/health 를 호출하는 데 사용.
# eclipse-temurin JRE 기본 이미지에는 curl 이 없으므로 명시적으로 설치한다.
RUN apt-get update \
 && apt-get install -y --no-install-recommends curl \
 && rm -rf /var/lib/apt/lists/* \
 && useradd -r -u 1001 appuser
COPY --from=build /app/build/libs/diary-service-0.1.0.jar app.jar
USER appuser
EXPOSE 8082
ENTRYPOINT ["java", "-jar", "app.jar"]
```

- [ ] **Step 3: 이미지 빌드 확인**

Run: `docker build -t tainted-spring/diary:0.1.0 .`
Expected: 빌드 성공, 최종 이미지 생성.

- [ ] **Step 4: 커밋**

```bash
git add -A
git commit -m "build: multistage Dockerfile and docker profile"
```

---

## Task 9: platform 레포에 diary 배선 (compose + k8s 스켈레톤)

> ⚠️ **작업 레포 전환**: 이 Task의 모든 파일 수정·커밋은 `tainted-spring-diary` 가 아니라 **`tainted-spring-platform` 레포**에서 한다. 각 Step의 경로를 확인할 것.

**Files (다른 레포 `tainted-spring-platform`):**
- Modify: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-platform/docker-compose.yml`
- Create: `/Users/changjoonbaek/workspace_claude-code/tainted-spring-platform/k8s/diary/deployment.yaml`

- [ ] **Step 1: compose에 diary 서비스 추가**

`tainted-spring-platform/docker-compose.yml` 의 `services:` 아래에 추가.

> `<<: *java-service` 앵커 병합은 `environment` 블록을 **덮어쓴다**(merge가 아님). 따라서 `JAVA_TOOL_OPTIONS`와 `SPRING_PROFILES_ACTIVE`를 명시적으로 재선언한다.

```yaml
  diary:
    <<: *java-service
    build:
      context: ../tainted-spring-diary
    depends_on:
      postgres:
        condition: service_healthy
      kafka:
        condition: service_healthy
    environment:
      JAVA_TOOL_OPTIONS: "${JAVA_TOOL_OPTIONS:-}"
      SPRING_PROFILES_ACTIVE: "docker"
      POSTGRES_PASSWORD: "${POSTGRES_PASSWORD:-postgrespw}"
      DIARY_KEK_BASE64: "${DIARY_KEK_BASE64:-YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoxMjM0NTY=}"
    ports:
      - "8082:8082"
    healthcheck:
      # 런타임 이미지에 설치한 curl 사용. -f: 비2xx 응답이면 실패 → actuator DOWN 시 unhealthy.
      test: ["CMD", "curl", "-fsS", "http://localhost:8082/actuator/health"]
      interval: 15s
      timeout: 5s
      retries: 20
      start_period: 40s
```

- [ ] **Step 2: compose로 통합 기동 검증**

Run (platform 레포에서):
```bash
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-platform
docker compose up -d --build --wait diary
curl -s http://localhost:8082/actuator/health
curl -s -X POST http://localhost:8082/internal/diaries \
  -H "Content-Type: application/json" \
  -d '{"userId":"u1","title":"테스트","content":"본문","primaryEmotion":"JOY","energyScore":7}'
```
Expected: `--wait` 가 diary 헬스체크 통과까지 블록. health `{"status":"UP"}`, POST 응답에 `diaryId` 포함, HTTP 201.

- [ ] **Step 3: k8s Deployment/Service 스켈레톤**

`tainted-spring-platform/k8s/diary/deployment.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: tainted-spring-diary
  namespace: tainted-spring
spec:
  replicas: 1
  selector:
    matchLabels: { app: diary }
  template:
    metadata:
      labels: { app: diary }
    spec:
      containers:
        - name: diary
          image: tainted-spring/diary:0.1.0
          ports: [{ containerPort: 8082 }]
          env:
            - { name: SPRING_PROFILES_ACTIVE, value: "eks" }
            - { name: JAVA_TOOL_OPTIONS, value: "" }
          readinessProbe:
            httpGet: { path: /actuator/health, port: 8082 }
            initialDelaySeconds: 20
            periodSeconds: 10
---
apiVersion: v1
kind: Service
metadata:
  name: tainted-spring-diary
  namespace: tainted-spring
spec:
  selector: { app: diary }
  ports:
    - { port: 80, targetPort: 8082 }
```

- [ ] **Step 4: platform 레포 커밋**

```bash
cd /Users/changjoonbaek/workspace_claude-code/tainted-spring-platform
git add -A
git commit -m "feat: wire diary service into compose and k8s skeleton"
```

---

## Self-Review

**Spec coverage (설계문서 §3.2 대비):**
- 일기 CRUD → Task 6 `DiaryController` (POST/GET/GET content/PUT/DELETE) ✔
- 서버측 KMS envelope 암호화(AES/GCM, per-entry DEK, KEK 설정) → Task 3 `EnvelopeCryptoService` ✔
- 감정·활력 메타데이터 → `DiaryEntry.primaryEmotion`, `DiaryEntry.energyScore` ✔
- `DiaryEntry` 필드 스펙 준수(id·userId·encryptedTitle·encryptedContent·titleIv·contentIv·encryptedDek·dekIv·primaryEmotion·energyScore·timestamp) → Task 4 ✔
- 그래프 필드 미포함(mindgraph 소유) → `DiaryEntry`에 graph 필드 없음 ✔
- `GET /internal/diaries/{id}/content` 복호화 엔드포인트(mindgraph 콜백용) → Task 6 `getContent()` ✔
- publish-after-commit(`@TransactionalEventListener(AFTER_COMMIT)`) → Task 5 `DiaryEventListener` ✔
- `diary.created` 이벤트 필드 스펙(`{eventId,userId,diaryId,primaryEmotion,energyScore,occurredAt}`) → Task 5 `DiaryCreatedKafkaPayload` ✔
- mindgraph·analytics 소비(설계서 §4 토픽 카탈로그) → 발행자 diary 구현 완료; 소비자는 각 서비스 레포 담당 ✔
- PostgreSQL DB `diary` → application.yml + Task 7 Testcontainers ✔
- OpenAPI 노출 → Task 6 `OpenApiConfig` + springdoc ✔
- RFC 7807 problem+json → Task 6 `GlobalExceptionHandler` + `spring.mvc.problemdetails.enabled=true` + Task 7 검증 ✔
- `/actuator/health` 유지, 추적 라이브러리 없음 → build.gradle에 actuator만, OTel/Sleuth 미포함 ✔
- 결정론(Clock/IdGenerator) → Task 2 ✔
- Docker 멀티스테이지/non-root/curl 설치 → Task 8 ✔
- docker 프로파일 DNS(postgres:5432, kafka:29092) → Task 8 application-docker.yml ✔
- compose 배선(빈 JAVA_TOOL_OPTIONS 슬롯, depends_on postgres+kafka) + k8s 스켈레톤 → Task 9 ✔
- Gradle 8.12, Java toolchain 23, Spring Boot 3.4.1, Testcontainers 1.21.4 → Task 1 build.gradle ✔
- Jar 이름 `diary-service-0.1.0.jar` → `build.gradle` version+'jar.archiveBaseName' + Dockerfile COPY 일치 ✔

**Placeholder scan:** 모든 코드 step에 완전한 소스, 모든 명령에 기대출력 명시. 플레이스홀더(TODO/FIXME/`...`) 없음. ✔

**Type/이름 일관성:**
- `IdGenerator.newId()` — Task 2 정의, Task 5·6에서 동일 시그니처 사용 ✔
- `EnvelopeCryptoService.encryptFields(title,content)→EnvelopeResult` / `decrypt(String,String,String,String)→String` — Task 3 정의, Task 6 `DiaryService.create/update/getContent`에서 사용. 단일 `encrypt(String)→CipherResult`는 Task 3 단위테스트 전용 ✔
- `EnvelopeResult.titleCipher()/titleIv()/contentCipher()/contentIv()/encryptedDek()/dekIv()` 및 `CipherResult` accessor — Task 3 정의, Task 3 단위테스트·Task 6에서 일치. **제목·본문이 단일 DEK를 공유**하므로 단일 `encryptedDek`로 양쪽 복호화 가능 ✔
- `DiaryEntry.getEncryptedTitle()/getTitleIv()/getEncryptedContent()/getContentIv()/getEncryptedDek()/getDekIv()` — Task 4 정의, Task 6 사용 일치 ✔
- `DiaryCreatedEvent` (내부 Spring 이벤트) / `DiaryCreatedKafkaPayload` (Kafka JSON) 분리 — Task 5 정의, Task 5 리스너 사용 일치 ✔
- DTO 필드명(`diaryId`,`userId`,`primaryEmotion`,`energyScore`,`timestamp`,`title`,`content`) — Task 6 정의, Task 7 RestAssured 검증 키와 일치 ✔
- Kafka 토픽 이름 `diary.created` — Task 5 `KafkaConfig.diaryCreatedTopic()` + `DiaryEventListener.TOPIC` + Task 7 테스트 구독 일치 ✔
- 포트 8082 — application.yml + Dockerfile EXPOSE + compose ports + k8s containerPort + healthcheck URL 전체 일치 ✔
