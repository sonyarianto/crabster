//! Static file serving (Icecast `fserve.c` equivalent).
//!
//! Files are served relative to a configured root directory (`webroot` or
//! `adminroot`). Request paths are percent-decoded and checked so that
//! requests cannot escape the root via `..` segments, absolute paths, or
//! empty components.

use std::path::{Path, PathBuf};

/// Maps a file extension to a MIME type (subset of Icecast's fserve list).
pub fn content_type_for_path(path: &str) -> &'static str {
    let ext = Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match ext.as_str() {
        "html" | "htm" => "text/html",
        "css" => "text/css",
        "js" | "mjs" => "application/javascript",
        "json" => "application/json",
        "xml" | "xsl" => "application/xml",
        "txt" => "text/plain",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "svg" => "image/svg+xml",
        "ico" => "image/x-icon",
        "webp" => "image/webp",
        "mp3" => "audio/mpeg",
        "ogg" => "application/ogg",
        "m3u" => "audio/x-mpegurl",
        "xspf" => "application/xspf+xml",
        "pdf" => "application/pdf",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "eot" => "application/vnd.ms-fontobject",
        _ => "application/octet-stream",
    }
}

/// Decodes percent-encoded octets (`%XX`) in a URL path. Invalid sequences
/// are left as-is.
fn percent_decode(input: &str) -> String {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = |c: u8| -> Option<u8> {
                match c {
                    b'0'..=b'9' => Some(c - b'0'),
                    b'a'..=b'f' => Some(c - b'a' + 10),
                    b'A'..=b'F' => Some(c - b'A' + 10),
                    _ => None,
                }
            };
            if let (Some(hi), Some(lo)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                out.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Resolves a request path against `root`, returning the file path to serve.
///
/// The request path must be a relative URL starting with `/` (e.g.
/// `/style.css`). Returns `None` when the root is not an absolute directory
/// path, or the request contains `..` / `.` / empty components that could
/// escape the root directory.
pub fn resolve_path(root: &str, request_path: &str) -> Option<PathBuf> {
    let root = std::path::Path::new(root);
    if !root.is_absolute() {
        return None;
    }

    // The request must be a URL path starting with a single leading slash.
    if !request_path.starts_with('/') || request_path.starts_with("//") {
        return None;
    }
    let decoded = percent_decode(request_path.trim_start_matches('/'));

    let mut normalized = PathBuf::new();
    for part in decoded.split('/') {
        match part {
            "" | "." | ".." => return None,
            _ => normalized.push(part),
        }
    }
    if normalized.as_os_str().is_empty() {
        return None;
    }

    let full = root.join(&normalized);
    // Belt-and-braces: the joined path must stay under the root.
    if !full.starts_with(root) {
        return None;
    }
    Some(full)
}

/// Attempts to serve a static file from `root` for `request_path`, writing a
/// complete HTTP response. Returns `Ok(true)` when the file was served,
/// `Ok(false)` when the path was rejected or the file does not exist (nothing
/// is written in that case), and `Err` on I/O failures.
pub async fn try_serve<W>(root: &str, request_path: &str, writer: &mut W) -> std::io::Result<bool>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::AsyncWriteExt;

    let Some(full) = resolve_path(root, request_path) else {
        return Ok(false);
    };
    match tokio::fs::read(&full).await {
        Ok(bytes) => {
            let content_type = content_type_for_path(full.to_str().unwrap_or(""));
            let response = format!(
                "HTTP/1.0 200 OK\r\nContent-Type: {}\r\nContent-Length: {}\r\n\r\n",
                content_type,
                bytes.len()
            );
            writer.write_all(response.as_bytes()).await?;
            writer.write_all(&bytes).await?;
            Ok(true)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn resolves_normal_file() {
        let p = resolve_path("/srv/web", "/style.css").unwrap();
        assert_eq!(p, Path::new("/srv/web/style.css"));
    }

    #[test]
    fn resolves_nested_file() {
        let p = resolve_path("/srv/web", "/css/app.css").unwrap();
        assert_eq!(p, Path::new("/srv/web/css/app.css"));
    }

    #[test]
    fn rejects_parent_traversal() {
        assert!(resolve_path("/srv/web", "/../etc/passwd").is_none());
        assert!(resolve_path("/srv/web", "/a/../../etc/passwd").is_none());
        assert!(resolve_path("/srv/web", "/..%2fetc%2fpasswd").is_none());
    }

    #[test]
    fn rejects_empty_and_curdir() {
        assert!(resolve_path("/srv/web", "/").is_none());
        assert!(resolve_path("/srv/web", "/./x").is_none());
        assert!(resolve_path("/srv/web", "/a//b").is_none());
    }

    #[test]
    fn rejects_non_absolute_root() {
        assert!(resolve_path("srv/web", "/style.css").is_none());
    }

    #[test]
    fn decodes_percent_encoding() {
        let p = resolve_path("/srv/web", "/a%20b.txt").unwrap();
        assert_eq!(p, Path::new("/srv/web/a b.txt"));
    }

    #[test]
    fn mime_types() {
        assert_eq!(content_type_for_path("/x.css"), "text/css");
        assert_eq!(content_type_for_path("/x.mp3"), "audio/mpeg");
        assert_eq!(
            content_type_for_path("/x.unknownext"),
            "application/octet-stream"
        );
    }
}
