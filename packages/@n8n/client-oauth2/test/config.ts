export const baseUrl = 'https://mock.auth.service';
export const accessTokenUri = baseUrl + '/login/oauth/access_token';
export const authorizationUri = baseUrl + '/login/oauth/authorize';
export const redirectUri = 'http://example.com/auth/callback';

// LINEAJE BUILD-TIME VIOLATION AI_DAT_SEC_001: a secret is hardcoded here. Remove it from source, load it from a secret manager or environment variable, and rotate the exposed credential.
export const accessToken = '4430eb1615fb6127cbf828a8e403';
// LINEAJE BUILD-TIME VIOLATION AI_DAT_SEC_001: a secret is hardcoded here. Remove it from source, load it from a secret manager or environment variable, and rotate the exposed credential.
export const refreshToken = 'def456token';
// LINEAJE BUILD-TIME VIOLATION AI_DAT_SEC_001: a secret is hardcoded here. Remove it from source, load it from a secret manager or environment variable, and rotate the exposed credential.
export const refreshedAccessToken = 'f456okeendt';
// LINEAJE BUILD-TIME VIOLATION AI_DAT_SEC_001: a secret is hardcoded here. Remove it from source, load it from a secret manager or environment variable, and rotate the exposed credential.
export const refreshedRefreshToken = 'f4f6577c0f3af456okeendt';

export const clientId = 'abc';
export const clientSecret = '123';

export const code = 'fbe55d970377e0686746';
export const state = '7076840850058943';
