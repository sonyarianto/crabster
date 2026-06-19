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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_content_types() {
        assert_eq!(FormatType::from_content_type("application/ogg"), FormatType::Ogg);
        assert_eq!(FormatType::from_content_type("audio/ogg"), FormatType::Ogg);
        assert_eq!(FormatType::from_content_type("audio/mpeg"), FormatType::Mp3);
        assert_eq!(FormatType::from_content_type("audio/mp3"), FormatType::Mp3);
        assert_eq!(FormatType::from_content_type("audio/aac"), FormatType::Aac);
        assert_eq!(FormatType::from_content_type("audio/aacp"), FormatType::Aac);
        assert_eq!(FormatType::from_content_type("audio/opus"), FormatType::Opus);
        assert_eq!(FormatType::from_content_type("audio/flac"), FormatType::Flac);
        assert_eq!(FormatType::from_content_type("application/flac"), FormatType::Flac);
        assert_eq!(FormatType::from_content_type("video/webm"), FormatType::Webm);
        assert_eq!(FormatType::from_content_type("audio/webm"), FormatType::Webm);
        assert_eq!(FormatType::from_content_type("video/theora"), FormatType::Theora);
        assert_eq!(FormatType::from_content_type("audio/speex"), FormatType::Speex);
        assert_eq!(FormatType::from_content_type("audio/midi"), FormatType::Midi);
        assert_eq!(FormatType::from_content_type("application/kate"), FormatType::Kate);
        assert_eq!(FormatType::from_content_type("text/plain"), FormatType::Text);
        assert_eq!(FormatType::from_content_type("text/html"), FormatType::Text);
    }

    #[test]
    fn content_type_case_insensitive() {
        assert_eq!(FormatType::from_content_type("Audio/MPEG"), FormatType::Mp3);
        assert_eq!(FormatType::from_content_type("APPLICATION/OGG"), FormatType::Ogg);
    }

    #[test]
    fn content_type_with_parameters() {
        assert_eq!(
            FormatType::from_content_type("audio/ogg; codecs=opus"),
            FormatType::Ogg
        );
        assert_eq!(
            FormatType::from_content_type("audio/mpeg; charset=utf-8"),
            FormatType::Mp3
        );
    }

    #[test]
    fn content_type_substring_fallback() {
        assert_eq!(FormatType::from_content_type("audio/x-vorbis+ogg"), FormatType::Ogg);
        assert_eq!(FormatType::from_content_type("audio/x-mpeg-3"), FormatType::Mp3);
        assert_eq!(FormatType::from_content_type("audio/x-aac"), FormatType::Aac);
        assert_eq!(FormatType::from_content_type("audio/x-opus"), FormatType::Opus);
        assert_eq!(FormatType::from_content_type("audio/x-flac"), FormatType::Flac);
    }

    #[test]
    fn content_type_unknown() {
        assert_eq!(FormatType::from_content_type("video/mp4"), FormatType::Unknown);
        assert_eq!(FormatType::from_content_type("image/jpeg"), FormatType::Unknown);
        assert_eq!(FormatType::from_content_type(""), FormatType::Unknown);
    }

    #[test]
    fn mime_type_roundtrip() {
        for ft in &[FormatType::Ogg, FormatType::Mp3, FormatType::Aac, FormatType::Opus] {
            let ct = ft.mime_type();
            assert_eq!(FormatType::from_content_type(ct), *ft);
        }
    }

    #[test]
    fn supports_icy_metadata() {
        assert!(FormatType::Mp3.supports_icy_metadata());
        assert!(FormatType::Aac.supports_icy_metadata());
        assert!(FormatType::Generic.supports_icy_metadata());
        assert!(!FormatType::Ogg.supports_icy_metadata());
        assert!(!FormatType::Flac.supports_icy_metadata());
        assert!(!FormatType::Opus.supports_icy_metadata());
    }
}
