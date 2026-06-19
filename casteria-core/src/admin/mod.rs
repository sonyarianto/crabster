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

pub fn handle_admin_command(
    command: &AdminCommand,
    _params: &HashMap<String, String>,
    _state: &crate::SharedState,
) -> AdminResponse {
    match command {
        AdminCommand::MountList => {
            let xml = r#"<?xml version="1.0"?>
<icestats>
  <source mount="/stream">
    <listeners>0</listeners>
    <listener_connections>0</listener_connections>
    <source_connected>0</source_connected>
  </source>
</icestats>"#;
            AdminResponse::xml(200, xml.to_string())
        }
        AdminCommand::Stats | AdminCommand::StatsXml => {
            let xml = r#"<?xml version="1.0"?>
<icestats>
  <admin>casteria</admin>
  <host>localhost</host>
  <location>Earth</location>
  <server_id>Casteria/0.1.0</server_id>
  <server_start>0</server_start>
  <source_total>0</source_total>
  <sources>0</sources>
  <listeners>0</listeners>
  <listener_connections>0</listener_connections>
</icestats>"#;
            AdminResponse::xml(200, xml.to_string())
        }
        AdminCommand::ListClients => {
            let xml = r#"<?xml version="1.0"?>
<icestats>
  <source mount="/stream">
    <listener>
      <id>0</id>
      <ip>127.0.0.1</ip>
      <user_agent>casteria</user_agent>
      <connected>0</connected>
    </listener>
  </source>
</icestats>"#;
            AdminResponse::xml(200, xml.to_string())
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
            let xml = r#"<?xml version="1.0"?>
<icestats>
  <source mount="/stream">
    <listeners>0</listeners>
    <source_connected>0</source_connected>
  </source>
</icestats>"#;
            AdminResponse::xml(200, xml.to_string())
        }
        AdminCommand::ServerInfo => {
            let xml = r#"<?xml version="1.0"?>
<icestats>
  <server_id>Casteria/0.1.0</server_id>
  <hostname>localhost</hostname>
</icestats>"#;
            AdminResponse::xml(200, xml.to_string())
        }
    }
}
