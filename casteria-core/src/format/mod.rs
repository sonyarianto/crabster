use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FormatType {
    Ogg,
    Mp3,
    Aac,
    Opus,
    Flac,
    Webm,
    Theora,
    Speex,
    Midi,
    Kate,
    Text,
    Generic,
    Unknown,
}

impl FormatType {
    pub fn from_content_type(content_type: &str) -> Self {
        let ct = content_type.to_lowercase();
        match ct.as_str() {
            "application/ogg" | "audio/ogg" => Self::Ogg,
            "audio/mpeg" | "audio/mp3" => Self::Mp3,
            "audio/aac" | "audio/aacp" => Self::Aac,
            "audio/opus" => Self::Opus,
            "audio/flac" | "application/flac" => Self::Flac,
            "video/webm" | "audio/webm" => Self::Webm,
            "video/theora" => Self::Theora,
            "audio/speex" => Self::Speex,
            "audio/midi" => Self::Midi,
            "application/kate" => Self::Kate,
            "text/plain" | "text/html" => Self::Text,
            _ => {
                if ct.contains("ogg") {
                    Self::Ogg
                } else if ct.contains("mpeg") || ct.contains("mp3") {
                    Self::Mp3
                } else if ct.contains("aac") {
                    Self::Aac
                } else if ct.contains("opus") {
                    Self::Opus
                } else if ct.contains("flac") {
                    Self::Flac
                } else {
                    Self::Unknown
                }
            }
        }
    }

    pub fn mime_type(&self) -> &'static str {
        match self {
            Self::Ogg => "application/ogg",
            Self::Mp3 => "audio/mpeg",
            Self::Aac => "audio/aac",
            Self::Opus => "audio/opus",
            Self::Flac => "audio/flac",
            Self::Webm => "audio/webm",
            Self::Theora => "video/theora",
            Self::Speex => "audio/speex",
            Self::Midi => "audio/midi",
            Self::Kate => "application/kate",
            Self::Text => "text/plain",
            Self::Generic => "application/octet-stream",
            Self::Unknown => "application/octet-stream",
        }
    }

    pub fn supports_icy_metadata(&self) -> bool {
        matches!(self, Self::Mp3 | Self::Aac | Self::Generic)
    }
}

pub struct FormatPlugin {
    pub format_type: FormatType,
    pub content_type: String,
    pub icy_metadata_interval: Option<u16>,
}

impl FormatPlugin {
    pub fn new(format_type: FormatType, icy_interval: Option<u16>) -> Self {
        Self {
            content_type: format_type.mime_type().to_string(),
            format_type,
            icy_metadata_interval: icy_interval,
        }
    }
}

pub struct FormatRegistry {
    plugins: HashMap<FormatType, Arc<FormatPlugin>>,
}

impl FormatRegistry {
    pub fn new() -> Self {
        let mut plugins = HashMap::new();
        for ft in &[
            FormatType::Ogg,
            FormatType::Mp3,
            FormatType::Aac,
            FormatType::Opus,
            FormatType::Flac,
            FormatType::Webm,
            FormatType::Generic,
        ] {
            let icy_interval = ft.supports_icy_metadata().then_some(8192u16);
            plugins.insert(*ft, Arc::new(FormatPlugin::new(*ft, icy_interval)));
        }
        Self { plugins }
    }

    pub fn get(&self, format_type: FormatType) -> Option<Arc<FormatPlugin>> {
        self.plugins.get(&format_type).cloned()
    }

    pub fn get_by_content_type(&self, content_type: &str) -> Option<Arc<FormatPlugin>> {
        let ft = FormatType::from_content_type(content_type);
        self.get(ft)
    }
}
