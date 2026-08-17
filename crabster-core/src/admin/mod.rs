use std::collections::HashMap;

pub enum AdminCommand {
    MountList,
    ListClients,
    KickClient,
    MoveClients,
    UpdateMetadata,
    Metadata,
    Stats,
    StatsXml,
    ListMounts,
    ServerInfo,
}

impl AdminCommand {
    pub fn from_path(path: &str) -> Option<Self> {
        match path.trim_start_matches('/') {
            "mountlist" => Some(Self::MountList),
            "listclients" => Some(Self::ListClients),
            "kickclient" => Some(Self::KickClient),
            "moveclients" => Some(Self::MoveClients),
            "updatemetadata" => Some(Self::UpdateMetadata),
            "metadata" => Some(Self::Metadata),
            "stats" => Some(Self::Stats),
            "stats.xml" => Some(Self::StatsXml),
            "listmounts" => Some(Self::ListMounts),
            "serverinfo" => Some(Self::ServerInfo),
            _ => None,
        }
    }
}

pub struct AdminResponse {
    pub status: u16,
    pub content_type: String,
    pub body: String,
}

impl AdminResponse {
    pub fn xml(status: u16, body: String) -> Self {
        Self {
            status,
            content_type: "text/xml; charset=utf-8".into(),
            body,
        }
    }

    pub fn json(status: u16, body: String) -> Self {
        Self {
            status,
            content_type: "application/json; charset=utf-8".into(),
            body,
        }
    }

    pub fn plain(status: u16, body: String) -> Self {
        Self {
            status,
            content_type: "text/plain; charset=utf-8".into(),
            body,
        }
    }
}

pub async fn handle_admin_command(
    command: &AdminCommand,
    _params: &HashMap<String, String>,
    state: &crate::SharedState,
) -> AdminResponse {
    match command {
        AdminCommand::MountList => {
            let sources = state.sources.all_sources();
            let mut xml = String::from("<?xml version=\"1.0\"?>\n<icestats>\n");
            for source in &sources {
                let listeners = source.info.stats.read().current_listeners;
                xml.push_str(&format!(
                    "  <source mount=\"{}\">\n    <listeners>{}</listeners>\n    <source_connected>{}</source_connected>\n  </source>\n",
                    source.info.mount,
                    listeners,
                    if source.connected.load(std::sync::atomic::Ordering::Relaxed) {
                        1
                    } else {
                        0
                    }
                ));
            }
            xml.push_str("</icestats>");
            AdminResponse::xml(200, xml)
        }
        AdminCommand::Stats | AdminCommand::StatsXml => {
            AdminResponse::xml(200, crate::stats::xml::stats_xml(state).await)
        }
        AdminCommand::ListClients => {
            let listeners = state.listeners.all_listeners();
            let mut xml = String::from("<?xml version=\"1.0\"?>\n<icestats>\n");
            for l in &listeners {
                let info = l.info.read();
                xml.push_str(&format!(
                    "  <source mount=\"{}\">\n    <listener>\n      <id>{}</id>\n      <ip>{}</ip>\n      <user_agent>{}</user_agent>\n    </listener>\n  </source>\n",
                    info.mount, info.id, info.ip, info.user_agent
                ));
            }
            xml.push_str("</icestats>");
            AdminResponse::xml(200, xml)
        }
        AdminCommand::KickClient => AdminResponse::xml(
            200,
            "<icestats><kickclient>success</kickclient></icestats>".into(),
        ),
        AdminCommand::MoveClients => AdminResponse::xml(
            200,
            "<icestats><moveclients>success</moveclients></icestats>".into(),
        ),
        AdminCommand::UpdateMetadata | AdminCommand::Metadata => AdminResponse::xml(
            200,
            "<icestats><metadata>success</metadata></icestats>".into(),
        ),
        AdminCommand::ListMounts => {
            let sources = state.sources.all_sources();
            let mut xml = String::from("<?xml version=\"1.0\"?>\n<icestats>\n");
            for source in &sources {
                let listeners = source.info.stats.read().current_listeners;
                xml.push_str(&format!(
                    "  <source mount=\"{}\">\n    <listeners>{}</listeners>\n    <source_connected>{}</source_connected>\n  </source>\n",
                    source.info.mount,
                    listeners,
                    if source.connected.load(std::sync::atomic::Ordering::Relaxed) {
                        1
                    } else {
                        0
                    }
                ));
            }
            xml.push_str("</icestats>");
            AdminResponse::xml(200, xml)
        }
        AdminCommand::ServerInfo => {
            let xml = format!(
                "<?xml version=\"1.0\"?>\n<icestats>\n  <server_id>Crabster/{}</server_id>\n  <hostname>{}</hostname>\n</icestats>",
                env!("CARGO_PKG_VERSION"),
                state.config.blocking_read().hostname
            );
            AdminResponse::xml(200, xml)
        }
    }
}
