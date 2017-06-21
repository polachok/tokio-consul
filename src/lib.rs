#![allow(non_snake_case)]

extern crate futures;
extern crate hyper;
extern crate tokio_core;
extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;

use std::sync::Arc;

use hyper::Client as HyperClient;
use hyper::client::HttpConnector;
use hyper::{Uri, Request, Method};
use futures::{Future, Stream};
use tokio_core::reactor::Handle;

use serde::Serialize;

#[derive(Debug)]
pub enum Error {
    Http(hyper::Error),
    Consul(String)
}

impl From<hyper::Error> for Error {
    fn from(e: hyper::Error) -> Self {
        Error::Http(e)
    }
}

/// Node represents a node
#[derive(Serialize, Deserialize, Debug)]
pub struct Node {
    pub Node: String,
    pub Address: String,
}

/// Service represents a service
#[derive(Serialize, Deserialize, Debug)]
pub struct Service {
    pub ID: String,
    pub Service: String,
    pub Tags: Option<Vec<String>>,
    pub Port: u32,
}

/// HealthService is used for the health service
#[derive(Serialize, Deserialize)]
pub struct HealthService{
    pub Node: Node,
    pub Service: Service,
}

/// Service represents a service
#[derive(Serialize, Deserialize, Debug)]
pub struct RegisterService {
    pub ID: String,
    pub Name: String,
    pub Tags: Vec<String>,
    pub Port: u16,
    pub Address: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Check: Option<Check>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Check {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl: Option<String>,
    pub interval: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub script: Option<String>,
    pub DeregisterCriticalServiceAfter: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TtlHealthCheck {
    pub ServiceID: String,
    pub ID: String,
    pub Name: String,
    pub Notes: String,
    pub TTL: String
}

/// Client for the consul API
pub struct Client {
    client: Arc<HyperClient<HttpConnector>>,
    base_uri: Uri,
}

/// Agent endpoint
pub struct Agent<'a> {
    client: &'a Client,
}

/// Key-value store endpoint
pub struct KV<'a> {
    client: &'a Client,
}

impl Client {
    pub fn new(handle: &Handle, url: &str) -> Result<Self, hyper::error::UriError> {
        let uri = url.parse()?;
        Ok(Client { client: Arc::new(HyperClient::new(handle)), base_uri: uri })
    }

    pub fn agent(&self) -> Agent {
        Agent { client: self }
    }

    pub fn kv(&self) -> KV {
        KV { client: self }
    }

    fn request(&self, method: Method, path: &str, type_: hyper::header::ContentType, body: Vec<u8>) -> hyper::client::FutureResponse {
        use hyper::header::ContentLength;

        let uri_str = format!("{}{}", self.base_uri, path);
        let uri = uri_str.parse().unwrap();

        let mut req = Request::new(method, uri);
        req.headers_mut().set(type_);
        req.headers_mut().set(ContentLength(body.len() as u64));
        req.set_body(body);

        let client = self.client.clone();
        client.request(req)
    }

    fn request_json<T: Serialize>(&self, method: Method, path: &str, body: T) -> hyper::client::FutureResponse {
        use hyper::header::{ContentLength,ContentType};

        let json = serde_json::to_string(&body).unwrap();
        let uri_str = format!("{}{}", self.base_uri, path);
        let uri = uri_str.parse().unwrap();
        let mut req = Request::new(method, uri);
        req.headers_mut().set(ContentType::json());
        req.headers_mut().set(ContentLength(json.as_bytes().len() as u64));
        //println!("SENDING {}", json);
        req.set_body(json);

        let client = self.client.clone();
        client.request(req)
    }
}

impl<'a> Agent<'a> {
    pub fn register(&self, service: RegisterService) -> Box<Future<Item = (), Error = Error>> {
        Box::new(self.client.request_json(Method::Put, "/v1/agent/service/register", service)
        .and_then(|resp| {
            let status = resp.status();
            resp.body().concat2().map(move |body| (status, body))
        })
        .map_err(|e| e.into())
        .and_then(|(status, body)| {
            if status.is_success() {
                return Ok(());
            }
            Err(Error::Consul(String::from_utf8_lossy(&body).to_string()))
        }))
    }
}

impl<'a> KV<'a> {
    pub fn put(&self, path: &str, data: Vec<u8>) -> Box<Future<Item = bool, Error = Error>> {
        use hyper::header::{ContentType};
        let mut uri: String = "/v1/kv/".into();
        uri.push_str(path);
        Box::new(self.client.request(Method::Put, &uri, ContentType::octet_stream(), data)
        .and_then(|resp| {
            let status = resp.status();
            resp.body().concat2().map(move |body| (status, body))
        })
        .map_err(|e| e.into())
        .and_then(|(status, body)| {
            if status.is_success() {
                use std::ops::Deref;
                if body.deref() == b"true\n" {
                    return Ok(true);
                }
                if body.deref() == b"false\n" {
                    return Ok(false);
                }
            }
            Err(Error::Consul(String::from_utf8_lossy(&body).to_string()))
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{Client, RegisterService, Check};
    use tokio_core::reactor::Core;
    #[test]
    fn it_works() {
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle(), "http://127.0.0.1:8500").unwrap();
        let sr = RegisterService {
            ID: "hello".to_string(),
            Name: "test".to_string(),
            Tags: vec![],
            Port: 9999,
            Address: "127.0.0.1".to_string(),
            Check: Some(Check {
                http: Some("http://127.0.0.1:9999/health".into()),
                interval: "1s".into(),
                script: None,
                ttl: None,
                DeregisterCriticalServiceAfter: "24h".into(),
            })
        };
        let res = core.run(client.agent().register(sr)).unwrap();
        println!("{:?}", res);
    }

    #[test]
    fn key_value() {
        let mut core = Core::new().unwrap();
        let client = Client::new(&core.handle(), "http://127.0.0.1:8500").unwrap();
        let res = core.run(client.kv().put("hello/world", vec![1,2,3,4])).unwrap();
        println!("{:?}", res);
    }
}
