# 마음쉼터 (Maeum Shelter) — Spring Boot MSA 테스트베드

품질 도구(분산추적/APM · 정적분석 · RestAssured 기반 out-of-process 블랙박스 API 테스트)를 검증하기 위한 **테스트베드용 백엔드**입니다. 실제 서비스 운영이 아니라, 도구가 분석·추적·테스트할 **현실적이고 이기종(heterogeneous)인 대상 시스템**을 제공하는 것이 목적입니다.

가상 제품 도메인은 두 참조 앱(`baekchangjoon/mind-diary`, `baekchangjoon/peace_of_mind`)을 합성한 **익명 멘탈 웰니스 플랫폼**입니다: *익명 사용자가 일기를 쓰면 마음 그래프로 변환되고, AI 상담사와 1:1 상담하며, 익명 커뮤니티에 고민을 올리면 AI 페르소나가 댓글을 단다.*

> **설계 의도 — 계측 부재(uninstrumented by design)**: 이 테스트베드는 일부러 OpenTelemetry/추적 헤더/상관관계 ID를 넣지 않았습니다. 품질 도구가 나중에 직접 계측(`-javaagent` 자동계측 등)을 붙여 이 멀티레포·이기종 토폴로지를 스스로 복원해내는지를 검증하기 위함입니다.

---

## 1. 아키텍처

```
                       ┌─────────────┐
 외부 클라이언트 ──REST──▶│ bff-gateway │ :8080  (외부 진입점, 인증 위임 + 집계 + 라우팅)
 (앱/웹/RestAssured)      └──────┬──────┘
                                │ REST (동기)
   ┌──────────┬────────────┬────┴───────┬─────────────┬───────────────┐
   ▼          ▼            ▼            ▼             ▼               ▼
auth-user   diary      counseling   community     mindgraph      (analytics/
 :8081      :8082        :8084        :8085          :8083        notification)
MySQL+Redis Postgres    Redis        MySQL        Postgres+Redis
   │          │            ▲            │              ▲
   │     diary.created     │       post.created        │
   │          ▼            │            ▼               │
   │     ┌────────────────┴──── Kafka (+ Zookeeper) ───┴────────┐
   │     │  diary.created · graph.updated · post.created ·       │
   │     │  comment.created · mood.logged   (JSON, 추적헤더 없음)  │
   │     └───┬───────────────────────┬───────────────────┬──────┘
   │   graph.updated            mood.logged         comment.created
   │         ▼                       ▼                    ▼
   │   counseling 소비          analytics :8086      notification :8087
   │                            Postgres             Redis (consumer-only)
```

### 서비스 매트릭스 (의도적 이기종성)

| 서비스 | 포트 | Java | 빌드 | Spring Boot | 저장소 | 역할 |
|---|---|---|---|---|---|---|
| **bff-gateway** | 8080 | 21 | Gradle | 3.4 (Cloud Gateway/WebFlux) | — | 외부 진입점·인증위임·집계·라우팅 |
| **auth-user** | 8081 | 17 | Maven | 3.3 | MySQL + Redis | 소셜/게스트 로그인·토큰 검증 |
| **diary** | 8082 | 23 | Gradle | 3.4 | PostgreSQL | 일기 CRUD·봉투암호화·`diary.created` |
| **mindgraph** | 8083 | 11 | Maven | 2.7 (OpenFeign) | PostgreSQL + Redis | 일기→마음그래프·`graph.updated` |
| **counseling** | 8084 | 21 | Gradle | 3.4 (WebFlux) | Redis | AI 상담사 4페르소나(결정론 mock) |
| **community** | 8085 | **1.8** | Maven | 2.7 | MySQL | 익명 게시판·`post/comment/mood` 이벤트 |
| **analytics** | 8086 | 23 | Maven | 3.4 | PostgreSQL | 감정/활력 집계·조회 |
| **notification** | 8087 | 17 | Gradle | 3.3 | Redis | 알림 생성·dedup (consumer-only) |

- **통신**: 동기 REST(BFF→서비스, mindgraph→diary Feign 콜백) + 비동기 Kafka(JSON, 추적헤더 없음).
- **결정론**: 외부 LLM 호출과 그래프 변환을 결정론적 mock/규칙기반으로 대체 — 같은 입력 → 같은 출력.

---

## 2. 멀티레포 구조

각 서비스는 **독립 레포**이며, 워크스페이스 루트(`~/workspace_claude-code/`) 아래 형제로 존재합니다. 공유 계약(스키마) 레포는 없습니다(각 서비스가 자기 DTO/이벤트 POJO를 자체 보유 → 스키마 드리프트가 자연 발생, 품질 도구 검증용).

```
~/workspace_claude-code/
├── tainted-spring-msa/            # 이 레포 — 설계/계획 문서 허브 (서비스 코드 없음)
│   └── docs/superpowers/{specs,plans}/
├── tainted-spring-platform/       # 인프라/오케스트레이션 (docker-compose, k8s 스켈레톤)
├── tainted-spring-bff-gateway/
├── tainted-spring-auth-user/
├── tainted-spring-diary/
├── tainted-spring-mindgraph/
├── tainted-spring-counseling/
├── tainted-spring-community/
├── tainted-spring-analytics/
└── tainted-spring-notification/
```

설계문서: [`docs/superpowers/specs/2026-06-13-mental-wellness-msa-testbed-design.md`](docs/superpowers/specs/2026-06-13-mental-wellness-msa-testbed-design.md)
서비스별 구현계획: [`docs/superpowers/plans/`](docs/superpowers/plans/)
진행상황 보고서: [`docs/PROJECT_STATUS.md`](docs/PROJECT_STATUS.md)

---

## 3. 사전 요구사항

- **Docker Desktop** (Compose v2.1+). 전체 스택 실행에는 이것만 있으면 됩니다(이미지가 각 서비스를 컨테이너 안에서 빌드).
- 개별 서비스를 **로컬에서** 빌드/테스트하려면 해당 JDK + 빌드도구가 필요합니다:
  - JDK **1.8 / 11 / 17 / 21 / 23**, Maven 3.9+, Gradle (diary는 8.12+, 그 외 Gradle 서비스는 8.10+)
  - 빌드 전 서비스에 맞는 JDK로 `export JAVA_HOME=$(/usr/libexec/java_home -v <버전>)`
  - macOS에서 `java_home -v 17`/`-v 21`이 다른 버전으로 오인 resolve될 수 있으니, `java -version`으로 확인 후 안 맞으면 JDK 홈 경로를 직접 지정하세요.

---

## 4. 전체 스택 실행 (권장)

`tainted-spring-platform` 레포의 Docker Compose가 인프라 5종 + 서비스 8개를 모두 띄웁니다.

```bash
cd ~/workspace_claude-code/tainted-spring-platform
cp .env.example .env          # 크리덴셜/포트 기본값
docker compose build          # 8개 서비스 이미지 빌드 (각자 자기 JDK 이미지로 컴파일)
docker compose up -d          # 인프라 + 서비스 기동 (헬스체크 의존성 순서대로)

docker compose ps             # 13개 컨테이너가 모두 (healthy) 인지 확인
```

기동 완료 후 외부 진입점은 **http://localhost:8080** (BFF) 입니다.

```bash
docker compose logs -f bff-gateway   # 특정 서비스 로그
docker compose down                  # 정리 (-v 붙이면 볼륨까지 삭제 → DB 초기화)
```

### 인프라만 / Kafka UI

```bash
make up      # 인프라 5종만 (mysql/postgres/redis/kafka/zookeeper)
make smoke   # 인프라 연결성 스모크 테스트
make down

docker compose --profile ui up -d kafka-ui   # Kafka UI → http://localhost:8090
```

### 커뮤니티 시드 데이터

`community` 서비스를 `seed` 프로파일로 띄우면 peace_of_mind 시드 게시글(예: "길을잃은갈매기" 취업 고민)이 채워집니다. compose에서 해당 서비스의 환경변수를 다음으로 오버라이드하세요:

```yaml
# tainted-spring-platform/docker-compose.yml 의 community 서비스
environment:
  SPRING_PROFILES_ACTIVE: "docker,seed"
```

---

## 5. API 사용 예시 (외부 = BFF :8080)

> `/api/v1/auth/**` 외의 모든 외부 호출은 `Authorization: Bearer <accessToken>` 가 필요합니다(BFF가 auth-user에 토큰 검증을 위임). 검증 실패 시 `401 application/problem+json`.

### 5.1 인증 — 게스트 진입 / 소셜 로그인

```bash
# 게스트 진입 → accessToken 발급
curl -s -X POST http://localhost:8080/api/v1/auth/guest
# → {"accessToken":"<uuid>","displayName":"익명"}

# 소셜 로그인 (mock): provider ∈ kakao|naver|toss, providerToken 형식 "valid-<provider>-<seed>"
curl -s -X POST http://localhost:8080/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"provider":"kakao","providerToken":"valid-kakao-u1"}'
# → {"accessToken":"<uuid>","displayName":"익명"}
```

이후 예시는 토큰을 변수에 담아 사용합니다:

```bash
TOKEN=$(curl -s -X POST http://localhost:8080/api/v1/auth/guest | sed 's/.*"accessToken":"\([^"]*\)".*/\1/')
echo "$TOKEN"
```

### 5.2 일기 — 작성 / 조회(일기+그래프 집계)

```bash
# 일기 작성 (energyScore 1~10). diary.created 이벤트가 발행되어 mindgraph·analytics가 비동기 소비.
curl -s -X POST http://localhost:8080/api/v1/diaries \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"userId":"u1","title":"오늘의 기록","content":"오늘 직장 상사 때문에 힘들었지만 운동하니 평온해졌다","primaryEmotion":"평온","energyScore":3}'
# → 201 {"diaryId":"<uuid>","userId":"u1","primaryEmotion":"평온","energyScore":3,"timestamp":"..."}

# 일기 상세 (BFF 합성: diary 본문 + mindgraph 그래프)
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8080/api/v1/diaries/<diaryId>
```

### 5.3 커뮤니티 — 글/댓글/좋아요

```bash
# 글 목록
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8080/api/v1/community/posts

# 글 작성. category ∈ relationship|career|family|mental|daily
curl -s -X POST http://localhost:8080/api/v1/community/posts \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"userId":"u1","title":"취업이 너무 막막해요","category":"career","content":"이력서 50개를 넣었는데...","moodEmoji":"😭","nickname":"길을잃은갈매기"}'
# → 201 {"postId":"<uuid>"}

# 댓글 작성. role ∈ empathic|analytical|sincere|hopeful
curl -s -X POST http://localhost:8080/api/v1/community/posts/<postId>/comments \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"author":"수현","role":"empathic","content":"많이 힘드셨겠어요."}'

# 좋아요
curl -s -X POST http://localhost:8080/api/v1/community/posts/<postId>/like \
  -H "Authorization: Bearer $TOKEN"
```

### 5.4 AI 상담 — 세션 시작 / 메시지

```bash
# personaKey ∈ empathic(수현) | analytical(성우) | sincere(진수) | hopeful(다혜)
SID=$(curl -s -X POST http://localhost:8080/api/v1/counseling/sessions \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"userId":"u1","personaKey":"empathic"}' | sed 's/.*"sessionId":"\([^"]*\)".*/\1/')

# 메시지 전송 → 결정론적 mock 응답(같은 입력은 항상 같은 답)
curl -s -X POST http://localhost:8080/api/v1/counseling/sessions/$SID/messages \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"personaKey":"empathic","userId":"u1","message":"요즘 너무 지쳐요"}'
# → {"reply":"<페르소나 프리픽스> ..."}
```

### 5.5 무드 추이 (analytics 집계)

```bash
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8080/api/v1/me/mood-trends
```

---

## 6. 내부 API (개별 서비스 직접 호출 — 블랙박스 테스트/디버깅용)

각 서비스는 `/internal/**` 경로와 **OpenAPI 문서**(`/swagger-ui.html`, `/v3/api-docs`), `/actuator/health` 를 노출합니다.

| 서비스 | 대표 내부 엔드포인트 |
|---|---|
| auth-user :8081 | `POST /internal/auth/verify` `{token}` · `GET /internal/users/{id}` |
| diary :8082 | `POST/GET/PUT/DELETE /internal/diaries` · `GET /internal/diaries/{id}/content`(서버측 복호화) |
| mindgraph :8083 | `GET /internal/graphs/user/{userId}` · `GET /internal/graphs/diary/{diaryId}` |
| counseling :8084 | `POST /internal/counseling/sessions` · `POST /internal/counseling/sessions/{id}/messages` |
| community :8085 | `POST/GET /internal/posts` · `POST /internal/posts/{id}/comments` · `POST /internal/posts/{id}/like` |
| analytics :8086 | `GET /internal/analytics/mood/{userId}` · `GET /internal/analytics/global` |
| notification :8087 | `GET /internal/notifications/{userId}` |

### 크로스 서비스 Kafka 체인 직접 확인 예시

```bash
# 1) 일기 직접 생성
DID=$(curl -s -X POST http://localhost:8082/internal/diaries -H 'Content-Type: application/json' \
  -d '{"userId":"u1","title":"테스트","content":"오늘 직장 상사 때문에 힘들었지만 운동하니 평온해졌다","primaryEmotion":"평온","energyScore":3}' \
  | sed 's/.*"diaryId":"\([^"]*\)".*/\1/')

sleep 5   # diary.created → mindgraph + analytics 비동기 소비 대기

# 2) mindgraph가 생성한 그래프 (STRESSOR 상사, COPING 운동, EMOTION 평온 + 링크)
curl -s http://localhost:8083/internal/graphs/diary/$DID

# 3) analytics가 집계한 무드 포인트
curl -s http://localhost:8086/internal/analytics/mood/u1
```

---

## 7. 개별 서비스 빌드/테스트 (로컬)

각 서비스 레포에서 해당 JDK로 빌드/테스트합니다(통합 테스트는 Testcontainers로 자체 인프라를 띄우므로 Docker 데몬 필요).

```bash
# 예: auth-user (Java 17, Maven)
cd ~/workspace_claude-code/tainted-spring-auth-user
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
mvn test

# 예: diary (Java 23, Gradle)
cd ~/workspace_claude-code/tainted-spring-diary
export JAVA_HOME=$(/usr/libexec/java_home -v 23)
./gradlew test
```

서비스별 JDK: auth-user 17 · diary 23 · mindgraph 11 · community 1.8 · counseling 21 · analytics 23 · notification 17 · bff-gateway 21.

---

## 8. AWS EKS (설계만)

`tainted-spring-platform/k8s/` 에 네임스페이스 + 서비스별 Deployment/Service **스켈레톤**이 있습니다(미배포). 서비스 디스커버리는 k8s Service DNS, 설정은 env 주입(12-factor), 이미지는 멀티스테이지·non-root. 상태저장소는 RDS/ElastiCache/MSK로 치환 가능하도록 추상화되어 있습니다.

---

## 9. 트러블슈팅

- **Testcontainers 통합테스트 실패 (Docker API 거부)**: Testcontainers는 **1.21.4** 이상이어야 Docker Desktop 29.x와 호환됩니다(1.20.x는 거부됨). Gradle + Boot 3 서비스는 Spring BOM이 버전을 강등하므로 `dependencyManagement`에 `testcontainers-bom:1.21.4` import가 들어 있습니다.
- **`java_home -v 17/21` 오인 resolve**: `java -version` 으로 확인하고 안 맞으면 JDK 홈 경로를 직접 `JAVA_HOME`에 지정하세요.
- **diary Gradle 빌드 실패(JDK 23)**: Gradle 8.10은 JDK 23에서 실행 불가 → diary는 Gradle **8.12+** 사용.
- **포트 충돌**: bff-gateway(8080)와 Kafka UI(호스트 8090)는 분리되어 있습니다. 8080~8087/3306/5432/6379/9092/2181 이 비어 있어야 합니다.
```
