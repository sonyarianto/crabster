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

pub struct HtpasswdProvider;

#[async_trait::async_trait]
impl AuthProvider for HtpasswdProvider {
    fn name(&self) -> &'static str {
        "htpasswd"
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
        Ok(AuthAction::Defer)
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
