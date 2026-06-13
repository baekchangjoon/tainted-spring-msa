# 설계문서: tainted-spring-msa — 멘탈 웰니스 MSA 테스트베드

- **작성일**: 2026-06-13
- **상태**: 승인 대기 (브레인스토밍 산출물)
- **목적**: 품질 도구(quality tooling) 검증용 Spring Boot 마이크로서비스 테스트베드 설계

---

## 1. 배경과 목표

### 1.1 무엇을 만드는가
다양한 품질 도구를 검증하기 위한 **테스트베드용 Spring Boot MSA 백엔드**. 실제 제품을 운영하려는 것이 아니라, 도구가 분석/추적/테스트할 **현실적이고 이기종(heterogeneous)인 대상 시스템**을 제공하는 것이 목적이다.

가상 제품 도메인은 두 참조 앱을 합성한 **"마음쉼터(Maeum Shelter) — 익명 멘탈 웰니스 플랫폼"**:
- `baekchangjoon/mind-diary` (개인용 안드로이드): 익명 일기, KMS envelope 암호화, 일기→마음 그래프 변환, LLM 상담사.
- `baekchangjoon/peace_of_mind` (커뮤니티 웹): 익명 게시판, 4종 AI 상담사 페르소나 댓글, 1:1 상담 챗, 힐링 카드.

합성 유저스토리: *익명 사용자가 일기를 쓰면 마음 그래프로 변환되고, AI 상담사와 1:1 상담하며, 익명 커뮤니티에 고민을 올리면 AI 페르소나가 댓글을 단다. 감정/활력 추이가 집계되어 무드 트렌드로 제공된다.*

### 1.2 검증 대상 품질 도구 (설계 우선순위의 근거)
1. **관측성 / 분산추적 / APM** — 단, 테스트베드 자체는 **계측되지 않은(uninstrumented) 순수 타깃**으로 둔다. 추적 도구가 나중에 직접 계측(예: `-javaagent` 자동계측, out-of-process 수집)을 붙여 토폴로지를 복원하는지 검증한다.
2. **정적분석 / 코드품질** — Java 1.8~23, Maven/Gradle 혼용, MVC/WebFlux 등 다양한 패턴으로 파서 커버리지를 자극.
3. **out-of-process 블랙박스 API 테스트(RestAssured)** — 안정적·결정론적 REST 계약, OpenAPI 문서, 일관 에러 포맷을 제공.

### 1.3 핵심 설계 원칙
- **계측 부재(uninstrumented by design)**: OTel/Micrometer Tracing/Sleuth 의존성·코드 없음. REST 헤더·Kafka 헤더에 추적 컨텍스트(traceparent 등) 전파 없음. 컨테이너는 빈 `JAVA_TOOL_OPTIONS`/`-javaagent` 슬롯만 노출(기본 미설정).
  - **범위 명확화**: `spring-boot-starter-actuator`의 `/health`(liveness/readiness)는 **유지**한다(블랙박스 테스트가 기동 검증에 사용). 단 tracing/metrics exporter는 추가하지 않는다. 로깅은 **Spring Boot 기본 로깅 그대로** 두고, 상관관계 ID(correlation/trace id)나 MDC 주입을 하지 않는다 — 도구가 붙이기 전까지 완전한 "민낯" 상태.
- **결정론(determinism)**: 외부 LLM 호출과 그래프 변환을 결정론적 mock/규칙기반으로 대체. 같은 입력 → 항상 같은 출력. 블랙박스 테스트 재현성 확보.
- **현실적 이기종성(realistic heterogeneity)**: Java 버전 5종, 빌드 도구 2종, 동기/비동기 통신, 4종 저장소를 서비스에 분산.
- **멀티레포(multi-repo)**: 서비스마다 독립 레포. 대규모 조직의 실제 형태를 모사하고, 품질 도구가 레포 경계를 넘는 추적·계약 정합성을 다루도록 강제.
- **완전 독립 계약(no shared contract source)**: 공유 jar/스키마 레지스트리 없음. 각 서비스가 자기 DTO·이벤트 POJO를 자체 복제 보유. 스키마 드리프트가 자연 발생 → 도구가 탐지하는지 검증.
- **YAGNI**: 보안 취약점을 의도적으로 심지 않음(주 타깃 아님). Eureka 등 디스커버리 서버 추가 안 함(DNS 기반).

### 1.4 이번 작업의 범위
**이 문서는 설계까지다.** 실제 코드/레포 생성은 후속 단계(구현 계획 → 구현)에서 점진적으로 진행한다.

### 1.5 용어 정의 (Glossary)
- **계측 부재(uninstrumented)**: 애플리케이션이 추적/관측을 위한 코드·라이브러리를 전혀 갖지 않은 상태. 추적은 나중에 외부 도구가 붙인다.
- **결정론적(deterministic)**: 같은 입력이면 시간·실행 환경과 무관하게 항상 같은 출력. 블랙박스 테스트의 재현성을 위해 필요.
- **스키마 드리프트(schema drift)**: 같은 메시지/계약을 주고받는 서비스들이 서로 다른 버전의 필드 정의를 갖게 되어 발생하는 불일치. 공유 계약 출처가 없을 때 자연 발생한다.
- **out-of-process 블랙박스 테스트**: 대상 서비스의 내부 코드를 모른 채, 외부에서 HTTP API만 호출해 동작을 검증하는 방식(여기서는 RestAssured 기반).
- **이기종(heterogeneous)**: 서로 다른 Java 버전·빌드 도구·프레임워크가 한 시스템에 섞여 있는 상태.
- **BFF (Backend For Frontend)**: 외부 클라이언트 전용 진입 서비스. 여러 내부 서비스를 호출해 화면에 맞는 형태로 응답을 조합한다.
- **publish-after-commit**: DB 트랜잭션이 커밋된 뒤에 Kafka 이벤트를 발행하는 순서. 소비자가 아직 저장되지 않은 데이터를 조회하는 race를 막는다.

---

## 2. 시스템 아키텍처

### 2.1 토폴로지

```
                    ┌─────────────┐
   외부 클라이언트 ──REST──▶│ bff-gateway │ (외부 진입점, 집계/팬아웃)
   (앱/웹/RestAssured)      └──────┬──────┘
                                  │ REST (동기)
        ┌───────────┬─────────────┼──────────────┬───────────────┐
        ▼           ▼             ▼              ▼               ▼
  ┌──────────┐ ┌─────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐
  │auth-user │ │ diary   │ │ counseling │ │ community  │ │ mindgraph  │
  └────┬─────┘ └────┬────┘ └─────┬──────┘ └─────┬──────┘ └─────┬──────┘
   MySQL│Redis  Postgres      Redis           MySQL      Postgres│Redis
        │           │            ▲               │              ▲
        │      diary.created     │          post.created        │
        │           ▼            │               ▼              │
        │      ┌─────────────────┴───────────────────────────────┐
        │      │                  Kafka  (+ Zookeeper)            │
        │      └───┬──────────────────────┬────────────────┬─────┘
        │   graph.updated           mood.logged      comment.created
        │          ▼                      ▼                ▼
        │   (counseling 소비)      ┌────────────┐   ┌──────────────┐
        │                         │ analytics  │   │ notification │
        │                         └─────┬──────┘   └──────┬───────┘
        │                            Postgres           Redis
        └──────────────────────── 모든 서비스: 추적/계측 없음 ─────────────
```

- **동기(REST)**: 외부 → `bff-gateway` → 각 서비스. BFF는 인증 토큰 검증을 `auth-user`에 위임하고, 화면용 응답을 집계.
- **비동기(Kafka)**: 아래 §4 이벤트 카탈로그 참조.
- **추적 헤더 없음**: REST·Kafka 어디에도 추적 컨텍스트를 전파하지 않는다. 도구가 produce↔consume 인과관계, REST 호출 체인을 스스로 복원해야 한다.

### 2.2 기술 매트릭스

| 서비스 | Java | Spring Boot | 빌드 | 저장소 | HTTP 스타일 / 인터서비스 클라이언트 |
|---|---|---|---|---|---|
| bff-gateway | 21 | 3.4.x | Gradle | — | WebFlux BFF: Spring Cloud Gateway 라우팅 + `WebClient` 집계 핸들러 |
| auth-user-service | 17 | 3.3.x | Maven | MySQL + Redis | Spring MVC + `RestTemplate` |
| diary-service | 23 | 3.4.x | Gradle | Postgres | Spring MVC + `WebClient` |
| counseling-service | 21 | 3.4.x | Gradle | Redis | Spring WebFlux(리액티브) + `WebClient` |
| community-service | 1.8 | 2.7.x | Maven | MySQL | Spring MVC + `RestTemplate` (레거시 패턴) |
| mindgraph-service | 11 | 2.7.x | Maven | Postgres + Redis | Spring MVC + OpenFeign |
| notification-service | 17 | 3.3.x | Gradle | Redis | Kafka consumer 전용 (아웃바운드 REST 없음) |
| analytics-service | 23 | 3.4.x | Maven | Postgres | Spring MVC (조회 API) |

분산 결과: Java 1.8·11·17·21·23 각 1~2개, Maven 4 / Gradle 4, MVC vs WebFlux vs Gateway, RestTemplate vs WebClient vs Feign 공존. Boot 2.7(Java 1.8·11)과 Boot 3.x(Java 17·21·23)가 한 시스템에 섞여, 자동계측 에이전트의 이기종 후킹 커버리지와 정적분석 파서 커버리지를 동시에 자극.

---

## 3. 서비스별 책임 · 핵심 엔티티 · REST API

> 모든 API는 springdoc로 **OpenAPI 3 문서**를 노출하고, 에러는 **RFC 7807 `application/problem+json`** 일관 포맷을 사용한다. 각 서비스는 `/actuator/health`(liveness/readiness)를 제공한다.

### 3.1 auth-user-service (Java 17 · MySQL + Redis)
- **책임**: 소셜 로그인(카카오/네이버/토스/게스트), 익명 식별자 발급, 토큰 검증(introspection).
- **정책**: 표시명은 항상 `"익명"`, 내부 `userId`로만 추적.
- **엔티티**: `User{id, socialProvider, anonymousId, createdAt}`. 세션/토큰은 Redis.
- **내부 API**: `POST /internal/auth/verify`(introspection), `GET /internal/users/{id}`.

### 3.2 diary-service (Java 23 · Postgres)
- **책임**: 일기 CRUD, **시뮬레이션 KMS envelope 암호화**(서버측 DEK/KEK 암복호화), 감정/활력 메타데이터.
- **엔티티**: `DiaryEntry{id, userId, encryptedTitle, encryptedContent, titleIv, contentIv, encryptedDek, dekIv, primaryEmotion, energyScore, timestamp}`. **그래프는 diary가 보유하지 않는다**(mindgraph-service 소유). diary는 제목/본문만 암호화 저장한다.
- **흐름**: 일기 저장 트랜잭션 **커밋 후** `diary.created` 발행(publish-after-commit) — mindgraph가 곧이어 복호화 엔드포인트를 호출할 때 데이터가 이미 커밋돼 있도록 보장. (Outbox 패턴은 비범위.) mindgraph가 그래프 생성을 위해 호출하는 **서버측 복호화 엔드포인트** 제공.
- **내부 API**: `POST/GET/PUT/DELETE /internal/diaries`, `GET /internal/diaries/{id}`, `GET /internal/diaries/{id}/content`(복호화 본문).

### 3.3 mindgraph-service (Java 11 · Postgres + Redis)
- **책임**: `diary.created` 소비 → diary 본문(복호화)을 **규칙기반 결정론적 추출**로 마음 그래프 변환 → 저장 → `graph.updated` 발행. 최신 그래프 컨텍스트를 Redis 캐시.
- **엔티티**: `MindNode{id, group∈[STRESSOR,SUPPORTER,COPING_MECHANISM,EMOTION], intensity, sentiment∈[POSITIVE,NEGATIVE,NEUTRAL]}`, `MindLink{source, target, relationType∈[TRIGGERS,MITIGATES,ASSOCIATED_WITH]}`.
- **내부 API**: `GET /internal/graphs/user/{userId}`, `GET /internal/graphs/diary/{diaryId}`.

### 3.4 counseling-service (Java 21 · Redis · WebFlux)
- **책임**: AI 상담사 4페르소나(공감 수현 / 분석 성우 / 직설 진수 / 희망 다혜) 1:1 챗, **결정론적 LLM mock**. mindgraph 컨텍스트를 REST로 조회해 응답에 반영. `post.created` 소비 → 해당 게시글에 AI 댓글 생성 → community에 댓글 등록(REST 호출). (`comment.created` 이벤트는 등록을 받은 community가 발행한다.)
- **엔티티**: `ChatSession`, `ChatMessage{role∈[user,model], content}`. Redis에 세션/히스토리 저장.
- **내부 API**: `POST /internal/counseling/sessions`, `POST /internal/counseling/sessions/{id}/messages`.

### 3.5 community-service (Java 1.8 · MySQL)
- **책임**: 익명 게시판 — 글/댓글/좋아요.
- **엔티티**: `Post{id, userId, title, category∈[relationship,career,family,mental,daily], content, moodEmoji, nickname, likes, createdAt}`, `Comment{id, postId, author, role∈[empathic,analytical,sincere,hopeful], content, createdAt}`. (`userId`는 익명 표시명 `nickname`과 별개로 내부 식별·이벤트 발행에 사용.)
- **흐름**: `post.created`, `comment.created`, `mood.logged`(moodEmoji) 발행.
- **내부 API**: `POST/GET /internal/posts`, `POST /internal/posts/{id}/comments`, `POST /internal/posts/{id}/like`.

### 3.6 notification-service (Java 17 · Redis · consumer 전용)
- **책임**: `graph.updated`, `comment.created` 소비 → 알림/다이제스트 생성, Redis 기반 중복제거. **아웃바운드 REST 없음** — 알림 대상은 이벤트 페이로드의 식별자(`graph.updated.userId`, `comment.created.postAuthorUserId`)만으로 결정한다(추가 조회 불필요).
- **엔티티**: `Notification{userId, type, payload, dedupKey, createdAt}`.

### 3.7 analytics-service (Java 23 · Postgres)
- **책임**: `diary.created`, `post.created`, `mood.logged` 소비 → 감정/활력 시계열 집계. 조회 API 제공.
- **엔티티**: `MoodPoint{userId, source, mood, score, occurredAt}`, 집계 뷰.
- **내부 API**: `GET /internal/analytics/mood/{userId}`, `GET /internal/analytics/global`.

### 3.8 bff-gateway (Java 21 · WebFlux BFF)
- **책임**: 외부 진입점, 토큰 검증 위임, 화면용 응답 집계.
- **구현 방식**: 단순 통과(passthrough) 라우팅은 **Spring Cloud Gateway 라우트**로, 여러 서비스를 합치는 합성(composite) 엔드포인트(예: `GET /diaries/{id}` = diary + mindgraph)는 **`WebClient` 기반 집계 핸들러**로 처리한다. (순수 Gateway만으로는 응답 집계가 어렵기 때문.)
- **외부 API (`/api/v1`)**:
```
POST /auth/login            소셜 로그인        POST /auth/guest          게스트 진입
GET  /me                    내 정보
GET  /diaries               목록              POST /diaries             일기 작성
GET  /diaries/{id}          일기+그래프 집계 응답
POST /counseling/sessions   상담 세션 시작     POST /counseling/sessions/{id}/messages
GET  /community/posts       글 목록           POST /community/posts     글 작성
POST /community/posts/{id}/comments           POST /community/posts/{id}/like
GET  /me/mood-trends        무드 추이(analytics 집계)
```

---

## 4. Kafka 이벤트 카탈로그

**직렬화**: 모든 이벤트는 **JSON**으로 직렬화한다(스키마 레지스트리 없음, schemaless). JSON을 택한 이유: 발행자가 필드를 추가/이름변경해도 소비자가 즉시 깨지지 않고 조용히 무시·결측되어 **스키마 드리프트가 자연 발생**하기 때문 — 도구가 이를 탐지하는지 검증하기에 적합하다. 추적 헤더 없음. 각 서비스는 토픽명·이벤트 필드 규약을 **독립적으로 자기 코드의 POJO로 반영**(중앙 레지스트리·공유 스키마 없음).

> **스키마 드리프트 의도 범위**: 소비자는 모르는 필드를 무시(`FAIL_ON_UNKNOWN_PROPERTIES=false`)하고, 없는 필드는 null/기본값으로 처리하도록 둔다. 즉 "선택적 필드 추가/결측"으로 인한 느슨한 불일치를 허용한다. 타입 변경 같은 강한 파괴는 의도적으로 만들지 않는다(테스트베드가 항상 기동은 되어야 하므로).

각 이벤트는 **자기완결적(self-contained)**이다 — 소비자가 추가 조회 없이 처리할 수 있도록 필요한 식별자를 페이로드에 포함한다(특히 notification은 아웃바운드 REST가 없으므로 알림 대상 id가 페이로드에 있어야 한다).

| 토픽 | 발행자 | 소비자 | 키 페이로드 |
|---|---|---|---|
| `diary.created` | diary | mindgraph, analytics | `{eventId, userId, diaryId, primaryEmotion, energyScore, occurredAt}` |
| `graph.updated` | mindgraph | counseling, notification | `{eventId, userId, diaryId, nodeCount, occurredAt}` |
| `post.created` | community | counseling, analytics | `{eventId, postId, userId, category, moodEmoji, occurredAt}` |
| `comment.created` | community | notification | `{eventId, postId, commentId, postAuthorUserId, role, occurredAt}` |
| `mood.logged` | community | analytics | `{eventId, userId, source, mood, score, occurredAt}` |

### 4.1 대표 다단계 인과 체인 (추적 도구 검증의 핵심 시나리오)
```
POST /api/v1/diaries (외부)
  → bff-gateway → diary-service: 일기 저장
  → diary 발행: diary.created
      → mindgraph 소비 → GET /internal/diaries/{id}/content (REST 콜백)
          → 규칙기반 그래프 생성 → 저장 → 발행: graph.updated
              → counseling 소비 (상담 컨텍스트 갱신)
              → notification 소비 (알림 생성)
      → analytics 소비 (감정 집계)
```
하나의 외부 호출이 sync(REST)와 async(Kafka)를 여러 번 넘나들며 여러 레포 소속 서비스를 가로지른다. 추적 헤더가 없으므로 도구가 이 인과 그래프를 스스로 복원해야 한다.

---

## 5. 횡단 관심사

### 5.1 결정론적 Mock 전략
- **LLM (counseling)**: `CounselorClient` 인터페이스 뒤에 `DeterministicCounselorClient` — `(persona, 정렬된 graph 컨텍스트, userMessage)`를 입력으로 페르소나별 템플릿 + 안정적 해시 기반 선택으로 응답 생성. 실제 LLM 구현체는 비활성 프로파일로만 끼울 수 있게 둔다.
- **마음 그래프 변환 (mindgraph)**: LLM이 아닌 **규칙기반 키워드/감정 추출**(사전 매핑 테이블). 입력 동일 → 그래프 동일.
- **시간/난수**: 이벤트 `occurredAt`·`eventId`는 주입 가능한 `Clock`/`IdGenerator` 빈으로. 테스트 프로파일에서 고정 가능.

### 5.2 블랙박스 테스트(RestAssured) 친화 설계
- 모든 외부/내부 API에 OpenAPI 3 문서(springdoc) 노출 → 계약 기반 테스트 가능.
- 안정적 JSON 스키마, RFC 7807 일관 에러 포맷.
- `/actuator/health`로 기동 대기/검증.
- 테스트용 시드 데이터 로더(profile `seed`) — peace_of_mind 시드 게시글("길을잃은갈매기" 취업 고민 등) 재현. 구체 목표: **5개 카테고리 × 2~3개 = 약 10~15개 게시글**, 다양한 `moodEmoji` 포함. 로더는 **idempotent**(고정 id 기반 중복 방지)하여 재기동·반복 테스트에서 동일 상태를 보장한다.

### 5.3 멀티레포 구성
서비스 8개 + 지원 레포 1개. 각 서비스는 독립 레포이며 자기 DTO·이벤트 POJO를 자체 보유한다.
```
tainted-spring-bff-gateway      (Gradle, Java21)
tainted-spring-auth-user        (Maven,  Java17)
tainted-spring-diary            (Gradle, Java23)
tainted-spring-counseling       (Gradle, Java21)
tainted-spring-community        (Maven,  Java1.8)
tainted-spring-mindgraph        (Maven,  Java11)
tainted-spring-notification     (Gradle, Java17)
tainted-spring-analytics        (Maven,  Java23)
tainted-spring-platform         # docker-compose, k8s 매니페스트, .env, 부트스트랩 스크립트 (계약 미보유)
```
- **서비스 디스커버리**: 디스커버리 서버 없음. 다른 서비스의 base URL은 `application.yml`의 `services.<name>.url` 프로퍼티로 정의하고 **환경변수로 오버라이드**한다(예: `SERVICES_DIARY_URL`). 프로파일별 기본값 — 로컬/`docker`: compose DNS(`http://diary:8080`), `eks`: k8s Service DNS(`http://tainted-spring-diary`). 코드는 환경별 호스트를 모른 채 프로퍼티만 참조.
- **계약**: 공유 jar/스키마 레지스트리 없음(완전 독립). 스키마 드리프트가 자연 발생 → 도구가 produce/consume 양쪽 스키마를 교차 검증해야 함.

### 5.4 Docker Compose (로컬 실행환경) — `platform` 레포
- 인프라 컨테이너: `mysql`, `postgres`, `redis`, `kafka`, `zookeeper` (+ Kafka UI 선택).
- 서비스 8개 각각 컨테이너. `depends_on` + healthcheck로 기동 순서 제어.
- 빌드 컨텍스트는 형제 디렉토리(`../tainted-spring-diary`) 또는 로컬 레지스트리 이미지 pull. 개발자는 레포들을 한 워크스페이스 폴더에 나란히 클론.
- 각 컨테이너에 **빈 `JAVA_TOOL_OPTIONS`/`-javaagent` 슬롯**을 env로 노출(기본 미설정). 앱은 추적 무지.
- `.env`로 포트/크리덴셜 외부화.

### 5.5 EKS 배포 고려사항 (지금은 설계만)
- 12-factor: 모든 설정 env 주입, 상태는 외부 저장소(RDS/ElastiCache/MSK로 치환 가능하게 추상화).
- Spring 프로파일 `local`/`docker`/`eks`. 컨테이너 이미지는 멀티스테이지 빌드, non-root.
- 각 서비스 레포가 자체 이미지 빌드 → ECR. `platform` 레포의 k8s 매니페스트(Deployment/Service/ConfigMap/Secret 스켈레톤)가 배포. DNS 기반 디스커버리가 그대로 k8s Service로 이어짐.
- Java 1.8/11 서비스는 베이스 이미지(eclipse-temurin:8/11)만 다르고 패턴 동일.

### 5.6 정적분석용 의도적 다양성
- Java 1.8~23 언어기능 차이(람다/var/record/sealed/pattern matching/virtual threads), Maven 4 + Gradle 4, MVC/WebFlux, RestTemplate/WebClient/Feign 자연 분산.
- 보안 취약 코드는 강제로 심지 않음(주 타깃이 보안 아님). 버전·스타일 다양성으로 파서 커버리지 자극.

---

## 6. 비범위 (Out of Scope)
- 실제 LLM(Gemini 등) 연동 — 결정론적 mock으로 대체.
- 클라이언트측 KMS envelope 암호화 — 서버측 시뮬레이션으로 단순화.
- 추적/관측성 스택(OTel Collector, Jaeger, Tempo, Prometheus, Grafana) — 품질 도구가 가져와 붙임. 테스트베드 미포함.
- 디스커버리 서버(Eureka/Consul), API 인증 외 보안 강화, 의도적 취약점 주입.
- 실제 git 레포 생성·CI 파이프라인 — 후속 구현 단계.

## 7. 미해결/후속 결정 사항
- 구현 순서(어느 서비스부터, 의존성 기준) — 구현 계획 단계에서 확정.
- 각 서비스의 정확한 Spring Boot 패치 버전, 의존성 버전(SCA 다양성 정도) — 구현 계획 단계.
- `platform` 레포의 compose 빌드 방식(형제 디렉토리 vs 로컬 레지스트리) 최종 택일.
