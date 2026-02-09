use super::{Registry, etcd::EtcdRegistry, redis::RedisRegistry};
use crate::{AmberError, Result};
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct RegistryBuilder {
    backend: Option<String>,
    namespace: Option<String>,
    etcd_endpoints: Option<Vec<String>>,
    redis_url: Option<String>,
}

impl RegistryBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn backend(mut self, backend: impl Into<String>) -> Self {
        self.backend = Some(backend.into());
        self
    }

    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    pub fn etcd_endpoints(mut self, endpoints: Vec<String>) -> Self {
        self.etcd_endpoints = Some(endpoints);
        self
    }

    pub fn redis_url(mut self, url: impl Into<String>) -> Self {
        self.redis_url = Some(url.into());
        self
    }

    fn resolve_namespace(&self) -> Result<String> {
        let namespace = self
            .namespace
            .as_deref()
            .unwrap_or_default()
            .trim()
            .to_string();
        if namespace.is_empty() {
            return Err(AmberError::Config(
                "registry namespace cannot be empty".to_string(),
            ));
        }

        Ok(namespace)
    }

    fn resolve_backend(&self) -> Result<String> {
        let backend = self
            .backend
            .as_deref()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();

        if backend.is_empty() {
            return Err(AmberError::Config(
                "registry backend cannot be empty".to_string(),
            ));
        }

        Ok(backend)
    }

    pub async fn build(&self) -> Result<Arc<dyn Registry>> {
        let namespace = self.resolve_namespace()?;
        let backend = self.resolve_backend()?;

        match backend.as_str() {
            "etcd" => {
                let endpoints = self.etcd_endpoints.clone().ok_or_else(|| {
                    AmberError::Config("etcd endpoints are required for etcd backend".to_string())
                })?;

                if endpoints.is_empty() {
                    return Err(AmberError::Config(
                        "etcd endpoints cannot be empty for etcd backend".to_string(),
                    ));
                }

                let registry = EtcdRegistry::new(&endpoints, &namespace).await?;
                Ok(Arc::new(registry))
            }
            "redis" => {
                let url = self.redis_url.as_deref().unwrap_or_default().trim();
                if url.is_empty() {
                    return Err(AmberError::Config(
                        "redis url is required for redis backend".to_string(),
                    ));
                }

                let registry = RedisRegistry::new(url, &namespace).await?;
                Ok(Arc::new(registry))
            }
            other => Err(AmberError::Config(format!(
                "unsupported registry backend: {}",
                other
            ))),
        }
    }
}
