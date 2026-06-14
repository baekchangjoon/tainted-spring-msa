# 실제 소셜로그인 — provider별 이기종 OAuth 검증 (설계)

- 날짜: 2026-06-14
- 대상 레포: `tainted-spring-auth-user`(백엔드), `peace_of_mind`(프론트+어댑터)
- 관련: BFF `POST /api/v1/auth/login {provider, providerToken}` 계약, auth-user `MockSocialVerifier`

## 1. 목적 / 배경

현재 auth-user 의 소셜로그인은 `MockSocialVerifier` 하나로, `providerToken` 이
`valid-<provider>-<seed>` 형식이면 통과하는 mock 이다(실제 OAuth 아님). 또한 프론트
`peace_of_mind` 에는 로그인 UI 가 전혀 없고 어댑터가 게스트 토큰을 서버측에서 발급해 쓴다.

이 테스트베드의 핵심 철학은 **의도적 이기종성**(서비스마다 다른 JDK/빌드/Spring 버전)이다.
소셜로그인도 같은 철학으로, provider 마다 **서로 다른 실제 OAuth 검증 메커니즘**을 구현해
품질도구가 분석할 현실적이고 이기종인 인증 경로를 제공한다.

토스(Toss)는 셀프서비스 소셜로그인이 아니라 B2B 제휴/온보딩이 필요한 상품이라 실제 연동
대상에서 제외하고 mock 으로만 유지한다.

## 2. 범위

- **포함**: Google·Kakao·Naver 의 실제 OAuth 검증 코드(백엔드 검증기 + 프론트 로그인 UI),
  provider별 mock/real 토글, 토스 mock 유지.
- **제외**: 토스 실연동, 리프레시 토큰/세션 회전, 회원 프로필 화면, 로그아웃 고도화.

## 3. provider별 이기종 검증 전략

`provider` 값으로 분기하는 `CompositeSocialVerifier` 뒤에 서로 다른 3가지 메커니즘을 둔다.
각 배치는 해당 provider 의 실제 권장 방식과 일치한다.

| Provider | OAuth 방식 | 프론트가 보내는 `providerToken` | 백엔드 검증 | 외부호출 | secret |
|---|---|---|---|---|---|
| **Google** | OIDC ID-token | ID token(JWT) | Google **JWKS 로 서명검증** + `aud=clientId`, `iss` 확인, `sub` 추출 | JWKS 캐시(키 조회만) | 불필요(clientId만) |
| **Kakao** | Authorization **Code** | auth `code` | `kauth.kakao.com/oauth/token` 에 **code+secret 교환** → `kapi.kakao.com/v2/user/me` | 2회(token, userinfo) | 필요(REST키+secret) |
| **Naver** | **Access-token + userinfo** | access token | `openapi.naver.com/v1/nid/me` Bearer 호출 | 1회(userinfo) | 토큰을 클라가 획득 |
| Toss | (mock 유지) | `valid-toss-<seed>` | 형식 검증만 | 없음 | — |

세 경로가 근본적으로 다르다: **JWT 암호검증 / 코드교환 / userinfo 인트로스펙션.**

## 4. 백엔드 설계 (auth-user)

### 4.1 컴포넌트

- `SocialVerifier` (인터페이스, 기존 유지): `ExternalIdentity verify(String provider, String providerToken)`
  - Kakao redirectUri 는 config 의 등록값을 사용(요청 본문 추가 없이 처리).
- 신규 구현체:
  - `GoogleIdTokenVerifier` — JWT 서명/클레임 검증(`aud`, `iss∈{accounts.google.com, https://accounts.google.com}`, `exp`). `externalId = "google:" + sub`.
  - `KakaoCodeVerifier` — code→token 교환 후 user/me 조회. `externalId = "kakao:" + id`.
  - `NaverTokenVerifier` — nid/me 조회. `externalId = "naver:" + response.id`.
  - `MockSocialVerifier` (기존 유지, 모든 provider 의 mock 모드 + 토스 전담).
- `CompositeSocialVerifier` — `provider` + 모드 config 로 적절한 구현체에 위임.
  자격증명 미설정/`mode=mock` 이면 `MockSocialVerifier` 로 폴백.

### 4.2 설정 (application.yml, env 주입)

```yaml
social:
  google:
    mode: ${SOCIAL_GOOGLE_MODE:mock}     # mock | real
    client-id: ${SOCIAL_GOOGLE_CLIENT_ID:}
  kakao:
    mode: ${SOCIAL_KAKAO_MODE:mock}
    client-id: ${SOCIAL_KAKAO_CLIENT_ID:}
    client-secret: ${SOCIAL_KAKAO_CLIENT_SECRET:}
    redirect-uri: ${SOCIAL_KAKAO_REDIRECT_URI:}
  naver:
    mode: ${SOCIAL_NAVER_MODE:mock}
    client-id: ${SOCIAL_NAVER_CLIENT_ID:}
    client-secret: ${SOCIAL_NAVER_CLIENT_SECRET:}
```

- **기본 mock**: 기존 블랙박스/통합 테스트(`valid-<provider>-<seed>`)가 그대로 통과.
- `SUPPORTED` provider 에 `google` 추가(현재 kakao|naver|toss).

### 4.3 의존성

- Google JWT 검증: `com.auth0:java-jwt` + `com.auth0:jwks-rsa`(JWKS 캐시). (Spring Security 도입 회피)
- Kakao/Naver HTTP: Spring `RestClient`(Spring 6.1, Boot 3.3) — 타임아웃 설정.

### 4.4 에러 처리

- 검증 실패(서명 불일치, aud 불일치, userinfo 4xx, 네트워크 오류) → `InvalidTokenException`
  → 기존 `GlobalExceptionHandler` 가 `401 application/problem+json`.
- 외부 호출 타임아웃(연결/읽기) 명시. 추적 헤더는 부착하지 않는다(테스트베드 원칙).

### 4.5 테스트

- 각 verifier 단위테스트: 외부 HTTP 는 WireMock(또는 MockWebServer)로 스텁.
  - Google: 테스트용 RSA 키쌍으로 ID token 서명 → JWKS 스텁 → 검증 통과/aud 불일치/만료 케이스.
  - Kakao: token+user/me 스텁(정상/4xx).
  - Naver: nid/me 스텁(정상/4xx).
- `CompositeSocialVerifier` 라우팅 + mock 폴백 테스트.
- 기존 테스트는 mode 기본 mock 으로 무수정 통과.

## 5. 어댑터/프론트 설계 (peace_of_mind)

### 5.1 어댑터(server.ts) 인증 모델 전환

- 신규 `POST /api/auth/social` — body `{provider, providerToken}` → BFF `/api/v1/auth/login`
  중계 → `{accessToken, displayName}` 반환.
- 신규 `POST /api/auth/guest` — BFF `/api/v1/auth/guest` 중계(익명 유지).
- 기존 `bff()` 호출이 **요청자의 토큰을 우선 사용**하도록 변경: 브라우저가 `Authorization`
  헤더로 사용자 토큰을 보내면 그대로 전달, 없으면 기존처럼 게스트 토큰 폴백.

### 5.2 프론트(App.tsx + 로그인 UI)

- 로그인 버튼 3종(Google/Kakao/Naver) + 토스(비활성/mock 안내).
- provider SDK 로 토큰/코드 획득:
  - Google: Google Identity Services(GIS) → ID token(credential).
  - Kakao: Kakao JS SDK `Kakao.Auth.authorize`(code) 또는 `login`(token) — code 방식.
  - Naver: naveridlogin JS SDK → access token.
- 획득값을 `POST /api/auth/social` 로 전송 → accessToken 저장(localStorage) → 이후 호출에
  `Authorization: Bearer` 부착. 로그인 안 하면 게스트로 동작(기존 UX 유지).
- provider client ID 는 `import.meta.env`(VITE_*)로 주입, 미설정 시 버튼 비활성+안내.

## 6. 검증 계획

- 백엔드: 단위/통합 테스트(mock+real 스텁) 그린. `mvn test`.
- 통합 스모크: 스택 기동 후 mock 모드로 `POST /api/v1/auth/login` 4 provider 동작 확인.
- Google real: 실제 client ID 주입 후 GIS 로그인 → 토큰 검증까지 1회 수동 확인.
- Kakao/Naver real: 코드 완성 + mock 자동테스트(실키 주입은 사용자 몫).

## 7. 비-목표(YAGNI)

- 토스 실연동, 다중 디바이스 세션, 토큰 리프레시/회전, RBAC, 계정 병합 UI.

## 8. 위험 / 한계

- **자격증명 의존**: 실제 동작은 사용자가 각 provider 콘솔에서 앱 등록 + client ID/secret +
  redirect URI 등록을 해야 가능. 코드는 "키만 넣으면 동작"하는 상태까지 완성.
- **redirect URI 등록**: Kakao 코드 교환은 등록된 redirect_uri 와 정확히 일치해야 함.
- **CORS/SDK 도메인**: provider 콘솔에 `localhost:3000` 등 허용 도메인 등록 필요.
