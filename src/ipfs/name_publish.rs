use serde::Serialize;
use ipfs_api_backend_hyper::request::ApiRequest;

#[derive(Serialize)]
pub struct NamePublish<'a, 'b, 'c, 'd> {
    #[serde(rename = "arg")]
    pub path: &'a str,

    pub resolve: bool,

    pub offline: bool,

    pub lifetime: Option<&'b str>,

    pub ttl: Option<&'c str>,

    pub key: Option<&'d str>,
}

impl<'a, 'b, 'c, 'd> ApiRequest for NamePublish<'a, 'b, 'c, 'd> {
    const PATH: &'static str = "/name/publish";
}

#[derive(Serialize)]
pub struct NameResolve<'a> {
    #[serde(rename = "arg")]
    pub name: Option<&'a str>,

    pub recursive: bool,

    pub nocache: bool,
}

impl<'a> ApiRequest for NameResolve<'a> {
    const PATH: &'static str = "/name/resolve";
}
