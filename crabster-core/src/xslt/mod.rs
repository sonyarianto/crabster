//! Minimal XSLT 1.0 engine covering the subset used by status pages
//! (Icecast `xslt.c` equivalent).
//!
//! Supported instructions:
//! - `xsl:template match="/"` (root template)
//! - `xsl:value-of select="..."`
//! - `xsl:for-each select="..."`
//! - `xsl:if test="..."`
//! - `xsl:choose` / `xsl:when test="..."` / `xsl:otherwise`
//! - `xsl:text` (verbatim output)
//! - Attribute value templates `{expr}` in literal result elements
//! - Literal result elements and text passed through
//!
//! Supported XPath subset: `/`, child steps (`a/b/c`), `@attr`, `text()`,
//! and existence tests (`element`, `@attr`). This covers the constructs used
//! by the classic Icecast `status.xsl`.

use std::collections::HashMap;

// ── Tiny XML model ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct XmlNode {
    pub name: String,
    pub attrs: HashMap<String, String>,
    pub children: Vec<XmlChild>,
}

#[derive(Debug, Clone)]
pub enum XmlChild {
    Element(XmlNode),
    Text(String),
}

impl XmlNode {
    pub fn children(&self) -> impl Iterator<Item = &XmlNode> {
        self.children.iter().filter_map(|c| match c {
            XmlChild::Element(e) => Some(e),
            XmlChild::Text(_) => None,
        })
    }

    pub fn child(&self, name: &str) -> Option<&XmlNode> {
        self.children().find(|c| c.name == name)
    }

    pub fn text(&self) -> String {
        let mut out = String::new();
        self.collect_text(&mut out);
        out
    }

    fn collect_text(&self, out: &mut String) {
        for c in &self.children {
            match c {
                XmlChild::Text(t) => out.push_str(t),
                XmlChild::Element(e) => e.collect_text(out),
            }
        }
    }
}

// ── XML parser ─────────────────────────────────────────────────────────────

/// Parses an XML document into an element tree. Only one root element is
/// expected; a leading `<?xml ...?>` declaration is skipped.
pub fn parse_xml(input: &str) -> Result<XmlNode, String> {
    let mut pos = 0;
    skip_ws_and_decl(input, &mut pos);
    parse_element(input, &mut pos)
}

fn skip_ws_and_decl(input: &str, pos: &mut usize) {
    loop {
        while input[*pos..]
            .chars()
            .next()
            .map(|c| c.is_whitespace())
            .unwrap_or(false)
        {
            *pos += 1;
        }
        if input[*pos..].starts_with("<?") {
            if let Some(end) = input[*pos..].find("?>") {
                *pos += end + 2;
                continue;
            }
        }
        break;
    }
}

fn parse_element(input: &str, pos: &mut usize) -> Result<XmlNode, String> {
    if !input[*pos..].starts_with('<') {
        return Err(format!("expected element at offset {}", *pos));
    }
    *pos += 1;
    let name = parse_name(input, pos)?;

    let mut attrs = HashMap::new();
    loop {
        skip_ws(input, pos);
        if input[*pos..].starts_with("/>") {
            *pos += 2;
            return Ok(XmlNode {
                name,
                attrs,
                children: Vec::new(),
            });
        }
        if input[*pos..].starts_with('>') {
            *pos += 1;
            break;
        }
        let attr_name = parse_name(input, pos)?;
        skip_ws(input, pos);
        if !input[*pos..].starts_with('=') {
            return Err(format!("expected '=' after attribute {attr_name}"));
        }
        *pos += 1;
        skip_ws(input, pos);
        let value = parse_attr_value(input, pos)?;
        attrs.insert(attr_name, value);
    }

    let mut children = Vec::new();
    loop {
        if *pos >= input.len() {
            return Err(format!("unexpected end of input inside <{name}>"));
        }
        if input[*pos..].starts_with("</") {
            *pos += 2;
            let close = parse_name(input, pos)?;
            skip_ws(input, pos);
            if !input[*pos..].starts_with('>') {
                return Err(format!("expected '>' closing </{close}>"));
            }
            *pos += 1;
            if close != name {
                return Err(format!("mismatched closing tag </{close}> for <{name}>"));
            }
            return Ok(XmlNode {
                name,
                attrs,
                children,
            });
        }
        if input[*pos..].starts_with("<!--") {
            if let Some(end) = input[*pos..].find("-->") {
                *pos += end + 3;
                continue;
            }
            return Err("unterminated comment".into());
        }
        if input[*pos..].starts_with("<![CDATA[") {
            if let Some(end) = input[*pos..].find("]]>") {
                let start = *pos + 9;
                children.push(XmlChild::Text(input[start..*pos + end].to_string()));
                *pos += end + 3;
                continue;
            }
            return Err("unterminated CDATA".into());
        }
        if input[*pos..].starts_with("<?") {
            if let Some(end) = input[*pos..].find("?>") {
                *pos += end + 2;
                continue;
            }
            return Err("unterminated processing instruction".into());
        }
        if input[*pos..].starts_with('<') {
            children.push(XmlChild::Element(parse_element(input, pos)?));
        } else {
            let text = parse_text(input, pos);
            if !text.is_empty() {
                children.push(XmlChild::Text(text));
            }
        }
    }
}

fn parse_name(input: &str, pos: &mut usize) -> Result<String, String> {
    let rest = &input[*pos..];
    let end = rest
        .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == '-' || c == '.' || c == ':'))
        .unwrap_or(rest.len());
    if end == 0 {
        return Err(format!("expected name at offset {}", *pos));
    }
    let name = rest[..end].to_string();
    *pos += end;
    Ok(name)
}

fn parse_attr_value(input: &str, pos: &mut usize) -> Result<String, String> {
    let quote = input[*pos..].chars().next().unwrap();
    if quote != '"' && quote != '\'' {
        return Err("expected quoted attribute value".into());
    }
    *pos += 1;
    let start = *pos;
    while *pos < input.len() {
        let c = input[*pos..].chars().next().unwrap();
        if c == quote {
            let raw = &input[start..*pos];
            *pos += 1;
            return Ok(unescape(raw));
        }
        *pos += c.len_utf8();
    }
    Err("unterminated attribute value".into())
}

fn parse_text(input: &str, pos: &mut usize) -> String {
    let start = *pos;
    while *pos < input.len() {
        if input[*pos..].starts_with('<') {
            break;
        }
        let c = input[*pos..].chars().next().unwrap();
        *pos += c.len_utf8();
    }
    unescape(&input[start..*pos])
}

fn unescape(raw: &str) -> String {
    raw.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

fn skip_ws(input: &str, pos: &mut usize) {
    while input[*pos..]
        .chars()
        .next()
        .map(|c| c.is_whitespace())
        .unwrap_or(false)
    {
        *pos += 1;
    }
}

// ── XPath subset ───────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum Step {
    Root,
    Child(String),
    Attr(String),
    Text,
}

fn parse_path(path: &str) -> Result<Vec<Step>, String> {
    let mut steps = Vec::new();
    let trimmed = path.trim();
    if trimmed == "/" {
        return Ok(vec![Step::Root]);
    }
    let mut rest = trimmed;
    if let Some(r) = rest.strip_prefix('/') {
        steps.push(Step::Root);
        rest = r;
    }
    for part in rest.split('/') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        if let Some(attr) = part.strip_prefix('@') {
            steps.push(Step::Attr(attr.to_string()));
        } else if part == "text()" {
            steps.push(Step::Text);
        } else {
            steps.push(Step::Child(part.to_string()));
        }
    }
    Ok(steps)
}

/// Selects nodes from a context node-set following a simple path.
fn select<'a>(context: &[&'a XmlNode], path: &str) -> Vec<&'a XmlNode> {
    let Ok(steps) = parse_path(path) else {
        return Vec::new();
    };
    let mut current: Vec<&XmlNode> = context.to_vec();
    for step in steps {
        match step {
            Step::Root => {
                // Everything shares the same root; nothing to do here since
                // the caller passes the root node already.
            }
            Step::Child(name) => {
                current = current
                    .iter()
                    .flat_map(|n| n.children())
                    .filter(|c| c.name == name)
                    .collect();
            }
            Step::Attr(_) | Step::Text => {
                // Attribute/text selections are terminal and handled by
                // `string_value` / `exists`.
                break;
            }
        }
    }
    current
}

fn string_value(node: &XmlNode, path: &str) -> String {
    let Ok(steps) = parse_path(path) else {
        return String::new();
    };
    // Resolve all but the final step against the node.
    if steps.is_empty() {
        return node.text();
    }
    if steps.len() == 1 {
        return match &steps[0] {
            Step::Root => node.text(),
            Step::Text => node.text(),
            Step::Attr(a) => node.attrs.get(a).cloned().unwrap_or_default(),
            Step::Child(name) => node.child(name).map(|c| c.text()).unwrap_or_default(),
        };
    }
    let mut current = vec![node];
    let last = steps.last().unwrap();
    for step in &steps[..steps.len() - 1] {
        match step {
            Step::Child(name) => {
                current = current
                    .iter()
                    .flat_map(|n| n.children())
                    .filter(|c| c.name == *name)
                    .collect();
            }
            _ => return String::new(),
        }
    }
    match last {
        Step::Attr(a) => current
            .first()
            .map(|n| n.attrs.get(a).cloned().unwrap_or_default())
            .unwrap_or_default(),
        Step::Child(name) => current
            .first()
            .and_then(|n| n.child(name))
            .map(|c| c.text())
            .unwrap_or_default(),
        _ => current.first().map(|n| n.text()).unwrap_or_default(),
    }
}

fn exists(node: &XmlNode, test: &str) -> bool {
    let test = test.trim();
    // Handle simple existence tests like `listeners`, `@mount`, `source`.
    if let Some(attr) = test.strip_prefix('@') {
        return node.attrs.contains_key(attr);
    }
    let Ok(steps) = parse_path(test) else {
        return false;
    };
    let mut current = vec![node];
    for step in steps {
        match step {
            Step::Child(name) => {
                current = current
                    .iter()
                    .flat_map(|n| n.children())
                    .filter(|c| c.name == name)
                    .collect();
            }
            Step::Attr(a) => {
                return current.iter().any(|n| n.attrs.contains_key(&a));
            }
            Step::Text | Step::Root => {}
        }
        if current.is_empty() {
            return false;
        }
    }
    !current.is_empty()
}

// ── Transformer ────────────────────────────────────────────────────────────

fn local_name(name: &str) -> &str {
    name.rsplit(':').next().unwrap_or(name)
}

/// Transforms an XML document with an XSLT stylesheet, returning the result.
pub fn transform(xml: &str, xsl: &str) -> Result<String, String> {
    let doc = parse_xml(xml)?;
    let sheet = parse_xml(xsl)?;
    if local_name(&sheet.name) != "stylesheet" {
        return Err("root element must be xsl:stylesheet".into());
    }

    let template = sheet
        .children()
        .find(|c| local_name(&c.name) == "template")
        .ok_or_else(|| "no xsl:template found".to_string())?;

    let mut out = String::new();
    render_node(template, &doc, &mut out)?;
    Ok(out)
}

fn render_node(node: &XmlNode, context: &XmlNode, out: &mut String) -> Result<(), String> {
    for child in &node.children {
        match child {
            // Whitespace-only text nodes in the stylesheet are stripped,
            // as required by XSLT 1.0 (use <xsl:text> for literal whitespace).
            XmlChild::Text(t) if t.trim().is_empty() => {}
            XmlChild::Text(t) => out.push_str(t),
            XmlChild::Element(e) => render_element(e, context, out)?,
        }
    }
    Ok(())
}

fn render_element(elem: &XmlNode, context: &XmlNode, out: &mut String) -> Result<(), String> {
    let name = local_name(&elem.name);
    match name {
        "template" => render_node(elem, context, out),
        "value-of" => {
            if let Some(select) = elem.attrs.get("select") {
                out.push_str(&string_value(context, select));
            }
            Ok(())
        }
        "for-each" => {
            let path = elem.attrs.get("select").cloned().unwrap_or_default();
            let nodes = select(std::slice::from_ref(&context), &path);
            for n in nodes {
                render_node(elem, n, out)?;
            }
            Ok(())
        }
        "if" => {
            let test = elem.attrs.get("test").cloned().unwrap_or_default();
            if exists(context, &test) {
                render_node(elem, context, out)?;
            }
            Ok(())
        }
        "choose" => {
            for child in elem.children() {
                match local_name(&child.name) {
                    "when" => {
                        let test = child.attrs.get("test").cloned().unwrap_or_default();
                        if exists(context, &test) {
                            render_node(child, context, out)?;
                            return Ok(());
                        }
                    }
                    "otherwise" => {
                        render_node(child, context, out)?;
                        return Ok(());
                    }
                    _ => {}
                }
            }
            Ok(())
        }
        "text" => {
            if elem
                .attrs
                .get("disable-output-escaping")
                .map(|v| v == "yes")
                .unwrap_or(false)
            {
                out.push_str(&elem.text());
            } else {
                out.push_str(&escape_text(&elem.text()));
            }
            Ok(())
        }
        _ => {
            // Literal result element: emit the tag with attributes (AVTs
            // evaluated), then children.
            out.push('<');
            out.push_str(&elem.name);
            for (k, v) in &elem.attrs {
                out.push(' ');
                out.push_str(k);
                out.push_str("=\"");
                out.push_str(&evaluate_avts(v, context));
                out.push('"');
            }
            out.push('>');
            render_node(elem, context, out)?;
            out.push_str("</");
            out.push_str(&elem.name);
            out.push('>');
            Ok(())
        }
    }
}

/// Replaces `{expr}` occurrences in an attribute value with the evaluated
/// string (attribute value template).
fn evaluate_avts(value: &str, context: &XmlNode) -> String {
    let mut out = String::new();
    let mut rest = value;
    while let Some(start) = rest.find('{') {
        if let Some(end) = rest[start..].find('}') {
            out.push_str(&rest[..start]);
            let expr = &rest[start + 1..start + end];
            // `{{` escapes a literal brace.
            if expr.is_empty() {
                out.push('{');
            } else {
                out.push_str(&string_value(context, expr));
            }
            rest = &rest[start + end + 1..];
        } else {
            out.push_str(rest);
            return out;
        }
    }
    out.push_str(rest);
    out
}

fn escape_text(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;

    const STATS_XML: &str = r#"<?xml version="1.0"?>
<icestats>
  <admin>crabster</admin>
  <host>localhost</host>
  <listeners>5</listeners>
  <sources>1</sources>
  <source mount="/live">
    <server_name>Test Station</server_name>
    <listeners>4</listeners>
    <server_type>audio/mpeg</server_type>
    <bitrate>128</bitrate>
  </source>
</icestats>"#;

    #[test]
    fn parses_simple_xml() {
        let doc = parse_xml("<a x=\"1\"><b>hi</b></a>").unwrap();
        assert_eq!(doc.name, "a");
        assert_eq!(doc.attrs.get("x").unwrap(), "1");
        assert_eq!(doc.child("b").unwrap().text(), "hi");
    }

    #[test]
    fn parses_self_closing_and_cdata() {
        let doc = parse_xml("<a><b/><![CDATA[<raw>]]></a>").unwrap();
        assert!(doc.child("b").is_some());
        assert!(doc.text().contains("<raw>"));
    }

    #[test]
    fn value_of_and_for_each() {
        let xsl = r#"<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:template match="/"><h1><xsl:value-of select="host"/></h1>
<xsl:for-each select="source"><p><xsl:value-of select="@mount"/> - <xsl:value-of select="server_name"/></p></xsl:for-each>
</xsl:template></xsl:stylesheet>"#;
        let out = transform(STATS_XML, xsl).unwrap();
        assert!(out.contains("<h1>localhost</h1>"));
        assert!(
            out.contains("<p>/live - Test Station</p>"),
            "actual output: {:?}",
            out
        );
    }

    #[test]
    fn if_and_choose() {
        let xsl = r#"<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:template match="/"><xsl:choose>
<xsl:when test="source"><b>has sources</b></xsl:when>
<xsl:otherwise>none</xsl:otherwise>
</xsl:choose><xsl:if test="admin">admin=<xsl:value-of select="admin"/></xsl:if>
</xsl:template></xsl:stylesheet>"#;
        let out = transform(STATS_XML, xsl).unwrap();
        assert!(out.contains("<b>has sources</b>"));
        assert!(out.contains("admin=crabster"));
    }

    #[test]
    fn attribute_value_templates() {
        let xsl = r#"<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:template match="/"><a href="/listen/{source/@mount}">listen</a></xsl:template></xsl:stylesheet>"#;
        let out = transform(STATS_XML, xsl).unwrap();
        assert!(out.contains("href=\"/listen//live\""));
    }

    #[test]
    fn literal_elements_pass_through() {
        let xsl = r#"<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
<xsl:template match="/"><html><body>Hello</body></html></xsl:template></xsl:stylesheet>"#;
        let out = transform(STATS_XML, xsl).unwrap();
        assert!(out.contains("<html><body>Hello</body></html>"));
    }
}
