use std::collections::HashMap;
use std::sync::Arc;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("authentication failed")]
    Failed,
    #[error("user not found")]
    UserNotFound,
    #[error("invalid credentials")]
    InvalidCredentials,
    #[error("provider error: {0}")]
    ProviderError(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthMethod {
    Source,
    Get,
    Put,
    Post,
    Head,
    Options,
    Admin,
    Web,
}

impl AuthMethod {
    pub fn from_http_method(method: &str) -> Option<Self> {
        match method.to_uppercase().as_str() {
            "SOURCE" => Some(Self::Source),
            "GET" => Some(Self::Get),
            "PUT" => Some(Self::Put),
            "POST" => Some(Self::Post),
            "HEAD" => Some(Self::Head),
            "OPTIONS" => Some(Self::Options),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuthResult {
    pub authenticated: bool,
    pub username: Option<String>,
    pub user_id: Option<String>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum AuthAction {
    Allow,
    Deny,
    Defer,
    Redirect(String),
    SendError(u16, String),
}

#[async_trait::async_trait]
pub trait AuthProvider: Send + Sync {
    fn name(&self) -> &'static str;
    async fn authenticate(
        &self,
        username: Option<&str>,
        password: Option<&str>,
        method: &AuthMethod,
        mount: &str,
        ip: &str,
        headers: &HashMap<String, String>,
    ) -> Result<AuthAction, AuthError>;
}

pub struct AuthStack {
    providers: Vec<(String, Arc<dyn AuthProvider + 'static>)>,
}

impl Default for AuthStack {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthStack {
    pub fn new() -> Self {
        Self {
            providers: Vec::new(),
        }
    }

    pub fn add_provider(&mut self, name: &str, provider: Arc<dyn AuthProvider + 'static>) {
        self.providers.push((name.to_string(), provider));
    }

    pub async fn authenticate(
        &self,
        username: Option<&str>,
        password: Option<&str>,
        method: &AuthMethod,
        mount: &str,
        ip: &str,
        headers: &HashMap<String, String>,
    ) -> Result<AuthResult, AuthError> {
        for (_, provider) in &self.providers {
            match provider
                .authenticate(username, password, method, mount, ip, headers)
                .await
            {
                Ok(AuthAction::Allow) => {
                    return Ok(AuthResult {
                        authenticated: true,
                        username: username.map(String::from),
                        user_id: None,
                        metadata: HashMap::new(),
                    });
                }
                Ok(AuthAction::Deny) => {
                    return Err(AuthError::Failed);
                }
                Ok(AuthAction::Redirect(url)) => {
                    return Ok(AuthResult {
                        authenticated: true,
                        username: None,
                        user_id: None,
                        metadata: {
                            let mut m = HashMap::new();
                            m.insert("redirect".to_string(), url);
                            m
                        },
                    });
                }
                Ok(AuthAction::SendError(code, msg)) => {
                    return Err(AuthError::ProviderError(format!("{} {}", code, msg)));
                }
                Ok(AuthAction::Defer) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(AuthError::Failed)
    }

    pub fn provider_count(&self) -> usize {
        self.providers.len()
    }
}

pub struct AnonymousProvider;

#[async_trait::async_trait]
impl AuthProvider for AnonymousProvider {
    fn name(&self) -> &'static str {
        "anonymous"
    }

    async fn authenticate(
        &self,
        _username: Option<&str>,
        _password: Option<&str>,
        _method: &AuthMethod,
        _mount: &str,
        _ip: &str,
        _headers: &HashMap<String, String>,
    ) -> Result<AuthAction, AuthError> {
        Ok(AuthAction::Allow)
    }
}

/// Parsed htpasswd file contents: the verify map plus the set of known
/// usernames (used to distinguish "unknown user" from "wrong password").
struct HtpasswdData {
    htpasswd: htpasswd_verify::Htpasswd<'static>,
    users: std::collections::HashSet<String>,
    last_modified: Option<std::time::SystemTime>,
}

/// File-based htpasswd authentication (Apache htpasswd format).
///
/// Supports the same hash formats as Icecast's `auth_htpasswd.c`: APR1-MD5
/// (`$apr1$`), bcrypt (`$2y$`), `{SHA}` and unix crypt. The file is reloaded
/// automatically when its mtime changes, so admins can add/remove users
/// without restarting the server.
///
/// Semantics within the auth stack: an unknown username defers to the next
/// provider; a known user with a wrong password fails hard (so a weaker
/// provider cannot override a real user's rejected credentials).
pub struct HtpasswdProvider {
    filename: Option<String>,
    data: parking_lot::RwLock<HtpasswdData>,
}

impl HtpasswdProvider {
    /// Loads credentials from an htpasswd file. A missing or unreadable file
    /// logs a warning and yields an empty set (all lookups defer).
    pub fn new(filename: &str) -> Self {
        let provider = Self {
            filename: Some(filename.to_string()),
            data: parking_lot::RwLock::new(HtpasswdData {
                htpasswd: htpasswd_verify::Htpasswd::new_owned(""),
                users: std::collections::HashSet::new(),
                last_modified: None,
            }),
        };
        provider.reload();
        provider
    }

    /// Builds a provider from raw htpasswd content (used in tests).
    pub fn from_content(content: &str) -> Self {
        Self {
            filename: None,
            data: parking_lot::RwLock::new(HtpasswdData {
                htpasswd: htpasswd_verify::Htpasswd::new_owned(content),
                users: parse_usernames(content),
                last_modified: None,
            }),
        }
    }

    /// Re-reads the file when its mtime changed since the last load.
    fn reload(&self) {
        let Some(filename) = &self.filename else {
            return;
        };
        let mtime = std::fs::metadata(filename).and_then(|m| m.modified()).ok();
        let mut data = self.data.write();
        if mtime == data.last_modified {
            return;
        }
        match std::fs::read_to_string(filename) {
            Ok(content) => {
                data.htpasswd = htpasswd_verify::Htpasswd::new_owned(&content);
                data.users = parse_usernames(&content);
            }
            Err(e) => {
                tracing::warn!("failed to read htpasswd file {}: {}", filename, e);
                data.htpasswd = htpasswd_verify::Htpasswd::new_owned("");
                data.users.clear();
            }
        }
        data.last_modified = mtime;
    }
}

/// Collects usernames from htpasswd content: non-empty, non-comment lines of
/// the form `user:hash`.
fn parse_usernames(content: &str) -> std::collections::HashSet<String> {
    content
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                return None;
            }
            line.split_once(':').map(|(user, _)| user.to_string())
        })
        .collect()
}

#[async_trait::async_trait]
impl AuthProvider for HtpasswdProvider {
    fn name(&self) -> &'static str {
        "htpasswd"
    }

    async fn authenticate(
        &self,
        username: Option<&str>,
        password: Option<&str>,
        _method: &AuthMethod,
        _mount: &str,
        _ip: &str,
        _headers: &HashMap<String, String>,
    ) -> Result<AuthAction, AuthError> {
        let (Some(username), Some(password)) = (username, password) else {
            return Err(AuthError::InvalidCredentials);
        };

        self.reload();

        let data = self.data.read();
        if !data.users.contains(username) {
            // Unknown user: let the next provider in the stack decide.
            return Ok(AuthAction::Defer);
        }
        if data.htpasswd.check(username, password) {
            Ok(AuthAction::Allow)
        } else {
            Err(AuthError::InvalidCredentials)
        }
    }
}

pub struct StaticProvider {
    users: HashMap<String, String>,
}

impl StaticProvider {
    pub fn new(users: HashMap<String, String>) -> Self {
        Self { users }
    }
}

#[async_trait::async_trait]
impl AuthProvider for StaticProvider {
    fn name(&self) -> &'static str {
        "static"
    }

    async fn authenticate(
        &self,
        username: Option<&str>,
        password: Option<&str>,
        _method: &AuthMethod,
        _mount: &str,
        _ip: &str,
        _headers: &HashMap<String, String>,
    ) -> Result<AuthAction, AuthError> {
        match (username, password) {
            (Some(u), Some(p)) => {
                if self.users.get(u).map(|pw| pw == p).unwrap_or(false) {
                    Ok(AuthAction::Allow)
                } else {
                    Err(AuthError::InvalidCredentials)
                }
            }
            _ => Err(AuthError::InvalidCredentials),
        }
    }
}

pub struct UrlProvider {
    url: String,
    client: reqwest::Client,
}

impl UrlProvider {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            client: reqwest::Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl AuthProvider for UrlProvider {
    fn name(&self) -> &'static str {
        "url"
    }

    async fn authenticate(
        &self,
        username: Option<&str>,
        password: Option<&str>,
        _method: &AuthMethod,
        mount: &str,
        ip: &str,
        _headers: &HashMap<String, String>,
    ) -> Result<AuthAction, AuthError> {
        let mut params = HashMap::new();
        params.insert("mount", mount);
        params.insert("ip", ip);
        if let Some(u) = username {
            params.insert("user", u);
        }
        if let Some(p) = password {
            params.insert("pass", p);
        }

        match self.client.post(&self.url).form(&params).send().await {
            Ok(resp) => {
                if resp.status().is_success() {
                    let body = resp.text().await.unwrap_or_default();
                    if body.contains("200") || body.to_lowercase().contains("ok") {
                        return Ok(AuthAction::Allow);
                    }
                    if body.contains("302") || body.to_lowercase().contains("redirect") {
                        return Ok(AuthAction::Redirect(body));
                    }
                    if body.contains("403") || body.to_lowercase().contains("deny") {
                        return Err(AuthError::Failed);
                    }
                    Ok(AuthAction::Allow)
                } else {
                    Err(AuthError::Failed)
                }
            }
            Err(e) => Err(AuthError::ProviderError(e.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_headers() -> HashMap<String, String> {
        HashMap::new()
    }

    // A realistic htpasswd file with one entry per supported hash format.
    // Hashes are known-good vectors (password = "password" for each).
    const HTPASSWD: &str = "\n\
# comment line is ignored\n\
md5_user:$apr1$lZL6V/ci$eIMz/iKDkbtys/uU7LEK00\n\
bcrypt_user:$2y$05$nC6nErr9XZJuMJ57WyCob.EuZEjylDt2KaHfbfOtyb.EgL1I2jCVa\n\
sha1_user:{SHA}W6ph5Mm5Pz8GgiULbPgzG37mj9g=\n\
crypt_user:bGVh02xkuGli2\n";

    #[tokio::test]
    async fn accepts_each_hash_format() {
        let provider = HtpasswdProvider::from_content(HTPASSWD);
        for user in ["md5_user", "bcrypt_user", "sha1_user", "crypt_user"] {
            let result = provider
                .authenticate(
                    Some(user),
                    Some("password"),
                    &AuthMethod::Source,
                    "/mount",
                    "127.0.0.1",
                    &empty_headers(),
                )
                .await;
            assert!(
                matches!(result, Ok(AuthAction::Allow)),
                "{user} should authenticate"
            );
        }
    }

    #[tokio::test]
    async fn rejects_wrong_password() {
        let provider = HtpasswdProvider::from_content(HTPASSWD);
        let result = provider
            .authenticate(
                Some("md5_user"),
                Some("wrong"),
                &AuthMethod::Source,
                "/mount",
                "127.0.0.1",
                &empty_headers(),
            )
            .await;
        assert!(matches!(result, Err(AuthError::InvalidCredentials)));
    }

    #[tokio::test]
    async fn defers_for_unknown_user() {
        let provider = HtpasswdProvider::from_content(HTPASSWD);
        let result = provider
            .authenticate(
                Some("nobody"),
                Some("password"),
                &AuthMethod::Source,
                "/mount",
                "127.0.0.1",
                &empty_headers(),
            )
            .await;
        assert!(matches!(result, Ok(AuthAction::Defer)));
    }

    #[tokio::test]
    async fn rejects_missing_credentials() {
        let provider = HtpasswdProvider::from_content(HTPASSWD);
        let result = provider
            .authenticate(
                None,
                None,
                &AuthMethod::Source,
                "/mount",
                "127.0.0.1",
                &empty_headers(),
            )
            .await;
        assert!(matches!(result, Err(AuthError::InvalidCredentials)));
    }

    #[tokio::test]
    async fn loads_from_file_and_reloads_on_change() {
        let dir = std::env::temp_dir().join(format!("crabster-auth-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("htpasswd");
        std::fs::write(&path, "first:$apr1$lZL6V/ci$eIMz/iKDkbtys/uU7LEK00\n").unwrap();

        let provider = HtpasswdProvider::new(path.to_str().unwrap());
        let result = provider
            .authenticate(
                Some("first"),
                Some("password"),
                &AuthMethod::Source,
                "/mount",
                "127.0.0.1",
                &empty_headers(),
            )
            .await;
        assert!(matches!(result, Ok(AuthAction::Allow)));

        // Add a second user and make sure it is picked up without restart.
        std::fs::write(
            &path,
            "first:$apr1$lZL6V/ci$eIMz/iKDkbtys/uU7LEK00\nsecond:{SHA}W6ph5Mm5Pz8GgiULbPgzG37mj9g=\n",
        )
        .unwrap();
        let result = provider
            .authenticate(
                Some("second"),
                Some("password"),
                &AuthMethod::Source,
                "/mount",
                "127.0.0.1",
                &empty_headers(),
            )
            .await;
        assert!(matches!(result, Ok(AuthAction::Allow)));

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn missing_file_defers_everything() {
        let provider = HtpasswdProvider::new("/nonexistent/htpasswd-file");
        let result = provider
            .authenticate(
                Some("anyone"),
                Some("password"),
                &AuthMethod::Source,
                "/mount",
                "127.0.0.1",
                &empty_headers(),
            )
            .await;
        assert!(matches!(result, Ok(AuthAction::Defer)));
    }
}
