use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;

#[derive(Parser)]
#[command(name = "casteria", about = "Streaming media server")]
struct Cli {
    #[arg(short = 'c', long, value_name = "FILE")]
    config: Option<PathBuf>,

    #[arg(long, value_name = "PORT")]
    stream_port: Option<u16>,

    #[arg(long, value_name = "PORT")]
    api_port: Option<u16>,

    #[arg(long, value_name = "PATH")]
    db_path: Option<String>,
}

fn merge_config(base: &mut casteria::ServerConfig, cli: &Cli) {
    if let Some(p) = cli.stream_port {
        base.stream_port = p;
    }
    if let Some(p) = cli.api_port {
        base.api_port = p;
    }
    if let Some(d) = &cli.db_path {
        base.db_path = Some(d.clone());
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "casteria=info".into()),
        )
        .init();

    let cli = Cli::parse();
    let mut config = casteria::ServerConfig::default();

    if let Some(ref path) = cli.config {
        let content = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;
        let file_config: casteria::ServerConfig = toml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))?;
        config = file_config;
    } else {
        let default_path = PathBuf::from("casteria.toml");
        if default_path.exists() {
            let content = std::fs::read_to_string(&default_path)?;
            config = toml::from_str(&content)?;
        }
    }

    merge_config(&mut config, &cli);

    casteria::run_server(config).await
}
