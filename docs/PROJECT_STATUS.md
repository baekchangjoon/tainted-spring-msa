# 프로젝트 진행상황 보고서 — 마음쉼터 MSA 테스트베드

- **작성일**: 2026-06-13
- **상태**: ✅ **구현 완료 & 전체 스택 E2E 검증 통과**
- **범위**: 8개 마이크로서비스 + 인프라/오케스트레이션 레포, 멀티레포

---

## 1. 목표 요약

다양한 품질 도구를 검증하기 위한 테스트베드용 Spring Boot MSA 백엔드. 검증 대상 도구:
1. **관측성/분산추적/APM** — 테스트베드는 **의도적으로 미계측(uninstrumented)** 상태로 두고, 도구가 직접 계측을 붙여 토폴로지를 복원하는지 검증.
2. **정적분석/코드품질** — Java 1.8~23, Maven/Gradle, MVC/WebFlux 등 다양성으로 파서 커버리지 자극.
3. **out-of-process 블랙박스 API 테스트(RestAssured)** — 안정적·결정론적 REST 계약 + OpenAPI 제공.

---

## 2. 진행 과정 (브레인스토밍 → 검증)

| 단계 | 산출물 | 결과 |
|---|---|---|
| 1. 브레인스토밍·설계 | 설계문서 1건 | 8개 서비스, 멀티레포, 완전독립 계약, 계측부재 확정 |
| 2. 설계 검토 | — | Haiku 모델 이해도 테스트 + 정밀 검토 → 모순 5건 수정(이벤트 자기완결성·소유권 등), 용어집 추가 |
| 3. 구현 계획 | 계획문서 9건 | platform + 8서비스, task 단위 TDD, 실제 코드 포함 |
| 4. 계획 검토 | — | 7개 계획 Haiku 검토 + 정밀 검토 → 실제 버그·근거없는 추정 수정(봉투암호화 DEK·Gradle/JDK23·wget/nc·포트충돌 등) |
| 5. 구현 | 9개 레포 | 서비스별 implementer 서브에이전트 순차 실행, TDD |
| 6. 검증 | — | 서비스별 통합테스트 통과 + 전체 스택 E2E 통과 |

설계/계획 문서는 `docs/superpowers/{specs,plans}/` 에 있습니다.

---

## 3. 구현 결과

### 서비스별 상태 (전부 ✅ 완료)

| 레포 | Java | 빌드/Boot | 테스트 | 검증 |
|---|---|---|---|---|
| tainted-spring-platform | — | Compose | smoke OK | 인프라 5종 + k8s 스켈레톤 |
| tainted-spring-auth-user | 17 | Maven/3.3 | 8 통과 | health UP, guest→"익명" |
| tainted-spring-diary | 23 | Gradle/3.4 | 9 통과 | 봉투암호화 라운드트립, `diary.created` 발행 |
| tainted-spring-mindgraph | 11 | Maven/2.7 | 5 통과 | Feign 콜백→그래프→`graph.updated` |
| tainted-spring-community | 1.8 | Maven/2.7 | 5 통과 | 글/댓글/좋아요, 3종 이벤트 |
| tainted-spring-counseling | 21 | Gradle/3.4 | 9 통과 | 4페르소나 결정론 mock |
| tainted-spring-analytics | 23 | Maven/3.4 | 7 통과 | 이벤트 집계, idempotency |
| tainted-spring-notification | 17 | Gradle/3.3 | 4 통과 | Redis dedup, consumer-only |
| tainted-spring-bff-gateway | 21 | Gradle/3.4 | 8 통과 | 라우팅·집계·인증필터 |

**총 55개 자동화 테스트 통과** (Testcontainers + RestAssured). 각 서비스 Docker 이미지 빌드 성공, platform compose/k8s에 배선·커밋 완료.

### 전체 스택 E2E 검증 (PASS)

`docker compose up -d` 로 13개 컨테이너(인프라 5 + 서비스 8) 동시 기동 → 전부 `(healthy)`. 확인된 동작:
- BFF 외부 API: 게스트 로그인(→"익명"), 커뮤니티 passthrough, 일기 생성 정상.
- **크로스 서비스 Kafka 체인 실증**: `POST /diaries`(diary) → `diary.created` → **mindgraph**(별도 레포)가 그래프 생성(상사 STRESSOR→평온 TRIGGERS, 운동 COPING→평온 MITIGATES) + **analytics**(또 다른 레포)가 무드 집계. 서로 다른 레포·JVM·DB 간 비동기 연동 확인.

---

## 4. 요구사항 충족 매핑

| 요구사항 | 충족 |
|---|---|
| 최소 5개 이상 마이크로서비스 | ✅ 8개 |
| Redis/MySQL/Postgres/Kafka/Zookeeper 선택적 보유 | ✅ 서비스별 분산 |
| 외부→BFF→내부 서비스 API 전달 | ✅ bff-gateway 라우팅/집계 |
| Java 1.8~23 다양한 버전 | ✅ 1.8·11·17·21·23 |
| 로컬 Docker Compose 실행·통신 | ✅ 전체 스택 E2E 통과 |
| AWS EKS 배포 고려 | ✅ k8s 스켈레톤·12-factor (미배포) |
| REST + Kafka 통신 | ✅ 양쪽 모두 |
| mind-diary/peace_of_mind 유저스토리 활용 | ✅ 도메인·API 합성 |

---

## 5. 구현 중 발견·해결한 이슈 (정직 기록)

### 코드/설계 결함 (검토·구현 단계에서 수정)
- **diary 봉투암호화 단일-DEK 버그**: 제목·본문을 각각 다른 DEK로 암호화 후 한 DEK만 저장 → 복호화 실패. `encryptFields(title,content)`로 단일 DEK 공유하도록 수정(계획검토 단계).
- **bff GlobalFilter 갭**: Spring Cloud Gateway의 `GlobalFilter`는 라우트 요청에만 동작 → `@RestController` 합성 엔드포인트 미보호. 구현 중 `WebFilter`로 해결(actuator 401도 함께 수정).
- **mindgraph 추출기**: 단위테스트 스펙에 맞춰 `직장` 키워드 제거, Feign 클라이언트 `primary=false`로 테스트 스텁 우선.
- **notification**: `opsForSet().add()`는 `Long` 반환(계획의 `Boolean` 오타 수정).

### 환경 게이트 (실행 환경에서 발견)
- **Testcontainers 1.20.3 → 1.21.4**: Docker Desktop 29.x가 1.20.x의 docker-java API 버전을 거부. Gradle+Boot3 서비스는 `testcontainers-bom:1.21.4` 오버라이드 필요.
- **diary Gradle 8.10 → 8.12**: 8.10은 JDK 23에서 실행 불가.
- **community `mysql:mysql-connector-java`**: Boot 2.7 BOM이 구좌표 미관리 → 명시 버전(8.0.33) 필요.
- **JDK 21·23 미설치**: Temurin tarball로 `~/Library/Java/JavaVirtualMachines/`에 설치(sudo 불필요). `java_home -v 17/21` 오인 resolve 사례 있어 명시 경로 사용.
- **계획에서 사전 제거한 추정**: `wget`/`nc` 헬스체크(이미지에 없을 수 있음) → `curl`/`cub zk-ready`로 교체, 임의 `sleep` → `compose up --wait`, kafka-ui↔bff 포트충돌(8080) → kafka-ui 8090.

---

## 6. 남은 작업 / 다음 단계

- **원격 저장소**: 현재 각 레포는 로컬 git 커밋만 존재(원격 없음). GitHub 등에 push하려면 레포별 원격 생성·push 필요.
- **EKS 실제 배포**: k8s 매니페스트는 스켈레톤 수준. ECR 이미지 푸시, ConfigMap/Secret, Ingress, RDS/ElastiCache/MSK 연결 필요.
- **품질 도구 부착**: 본래 목적인 추적 에이전트/정적분석/블랙박스 도구를 이 테스트베드에 붙여 검증.
- **시드 데이터**: community를 `docker,seed` 프로파일로 띄우면 시드 게시글 채워짐.
- **선택적 보강**: notification dedup TTL(현재 무한 누적), counseling/bff의 결정론 빈 활용 등(테스트베드 동작에는 영향 없음).

---

## 7. 빠른 실행

```bash
cd ~/workspace_claude-code/tainted-spring-platform
cp .env.example .env && docker compose build && docker compose up -d
docker compose ps          # 13개 (healthy) 확인
# 외부 진입점: http://localhost:8080  (사용법·API 예시는 ../tainted-spring-msa/README.md)
docker compose down
```
