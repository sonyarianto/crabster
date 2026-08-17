//! Renders the live server state as the Icecast-compatible `<icestats>` XML
//! document (the raw XML that XSLT stylesheets transform, mirroring Icecast's
//! `/admin/stats.xml`).

use serde_json::json;

use crate::source::Source;
use crate::SharedState;

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Builds the `<icestats>` XML document from the current server state.
pub async fn stats_xml(state: &SharedState) -> String {
    let sources = state.sources.all_sources();
    let global = state.stats.global();
    let hostname = state.config.read().await.hostname.clone();

    let mut out = String::from("<?xml version=\"1.0\"?>\n<icestats>\n");
    out.push_str(&format!(
        "  <admin>crabster</admin>\n  <host>{}</host>\n  <location>Earth</location>\n",
        xml_escape(&hostname)
    ));
    out.push_str(&format!(
        "  <server_id>Crabster/{}</server_id>\n",
        env!("CARGO_PKG_VERSION")
    ));
    out.push_str(&format!(
        "  <client_connections>{}</client_connections>\n",
        global
            .total_connections
            .load(std::sync::atomic::Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "  <clients>{}</clients>\n",
        global
            .current_listeners
            .load(std::sync::atomic::Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "  <listener_connections>{}</listener_connections>\n",
        global
            .current_listeners
            .load(std::sync::atomic::Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "  <listeners>{}</listeners>\n",
        global
            .current_listeners
            .load(std::sync::atomic::Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "  <source_client_connections>{}</source_client_connections>\n",
        global
            .total_source_connections
            .load(std::sync::atomic::Ordering::Relaxed)
    ));
    out.push_str(&format!(
        "  <source_total_connections>{}</source_total_connections>\n",
        global
            .total_source_connections
            .load(std::sync::atomic::Ordering::Relaxed)
    ));
    out.push_str(&format!("  <sources>{}</sources>\n", sources.len()));

    for source in &sources {
        out.push_str(&source_xml(source));
    }

    out.push_str("</icestats>\n");
    out
}

/// Builds the JSON status document for `/status-json.xsl` (Icecast-style).
pub async fn stats_json(state: &SharedState) -> String {
    let sources = state.sources.all_sources();
    let global = state.stats.global();
    let hostname = state.config.read().await.hostname.clone();

    let mut source_list = Vec::new();
    for source in &sources {
        let (title, artist, server_name, server_description, genre, url, bitrate) = {
            let meta = source.info.metadata.read();
            (
                meta.title.clone().unwrap_or_default(),
                meta.artist.clone().unwrap_or_default(),
                meta.icy_name.clone().unwrap_or_default(),
                meta.description.clone().unwrap_or_default(),
                meta.icy_genre.clone().unwrap_or_default(),
                meta.icy_url.clone().unwrap_or_default(),
                meta.icy_br.unwrap_or(0),
            )
        };
        let (listeners, peak, bytes_read, bytes_sent) = {
            let stats = source.info.stats.read();
            (
                stats.current_listeners,
                stats.peak_listeners,
                stats.total_bytes_received,
                stats.total_bytes_sent,
            )
        };
        source_list.push(json!({
            "mount": source.info.mount,
            "title": title,
            "artist": artist,
            "server_name": server_name,
            "server_description": server_description,
            "server_type": source.info.format.mime_type(),
            "genre": genre,
            "server_url": url,
            "bitrate": bitrate,
            "listeners": listeners,
            "listener_peak": peak,
            "max_listeners": source.info.max_listeners,
            "public": source.info.public,
            "total_bytes_read": bytes_read,
            "total_bytes_sent": bytes_sent,
        }));
    }

    serde_json::to_string_pretty(&json!({
        "icestats": {
            "admin": "crabster",
            "host": hostname,
            "location": "Earth",
            "server_id": format!("Crabster/{}", env!("CARGO_PKG_VERSION")),
            "listeners": global.current_listeners.load(std::sync::atomic::Ordering::Relaxed),
            "sources": sources.len(),
            "source": source_list,
        }
    }))
    .unwrap_or_else(|_| "{}".into())
}

/// Renders one `<source mount="...">` element (mirrors the fields Icecast
/// exposes per source).
fn source_xml(source: &Source) -> String {
    let info = &source.info;
    let (title, artist, server_name, server_description, genre, url, bitrate) = {
        let meta = info.metadata.read();
        (
            meta.title.clone().unwrap_or_default(),
            meta.artist.clone().unwrap_or_default(),
            meta.icy_name.clone().unwrap_or_default(),
            meta.description.clone().unwrap_or_default(),
            meta.icy_genre.clone().unwrap_or_default(),
            meta.icy_url.clone().unwrap_or_default(),
            meta.icy_br.unwrap_or(0),
        )
    };
    let (listeners, peak, bytes_read, bytes_sent) = {
        let stats = info.stats.read();
        (
            stats.current_listeners,
            stats.peak_listeners,
            stats.total_bytes_received,
            stats.total_bytes_sent,
        )
    };
    let connected = source.connected.load(std::sync::atomic::Ordering::Relaxed);

    let mut out = String::new();
    out.push_str(&format!(
        "  <source mount=\"{}\">\n",
        xml_escape(&info.mount)
    ));
    out.push_str(&format!("    <title>{}</title>\n", xml_escape(&title)));
    out.push_str(&format!("    <artist>{}</artist>\n", xml_escape(&artist)));
    out.push_str(&format!(
        "    <server_name>{}</server_name>\n",
        xml_escape(&server_name)
    ));
    out.push_str(&format!(
        "    <server_description>{}</server_description>\n",
        xml_escape(&server_description)
    ));
    out.push_str(&format!(
        "    <server_type>{}</server_type>\n",
        xml_escape(info.format.mime_type())
    ));
    out.push_str(&format!("    <genre>{}</genre>\n", xml_escape(&genre)));
    out.push_str(&format!(
        "    <server_url>{}</server_url>\n",
        xml_escape(&url)
    ));
    out.push_str(&format!("    <bitrate>{}</bitrate>\n", bitrate));
    out.push_str(&format!("    <listeners>{}</listeners>\n", listeners));
    out.push_str(&format!("    <listener_peak>{}</listener_peak>\n", peak));
    out.push_str(&format!("    <max_listeners>{}</max_listeners>\n", {
        info.max_listeners
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unlimited".into())
    }));
    out.push_str(&format!(
        "    <public>{}</public>\n",
        if info.public { 1 } else { 0 }
    ));
    out.push_str(&format!(
        "    <source_ip>{}</source_ip>\n",
        xml_escape(&info.client_ip)
    ));
    out.push_str(&format!(
        "    <user_agent>{}</user_agent>\n",
        xml_escape(&info.user_agent)
    ));
    out.push_str(&format!(
        "    <connected>{}</connected>\n",
        if connected { 1 } else { 0 }
    ));
    out.push_str(&format!(
        "    <total_bytes_read>{}</total_bytes_read>\n",
        bytes_read
    ));
    out.push_str(&format!(
        "    <total_bytes_sent>{}</total_bytes_sent>\n",
        bytes_sent
    ));
    out.push_str("  </source>\n");
    out
}
