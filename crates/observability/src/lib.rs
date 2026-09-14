//! This is the core of the observability engine.
//! It works as the center-piece for whatever observability tools the user has
//! Developement should always keep it decoupled from vendors such as Prometheus, OTel, etc.
//! All primitives should be kept simple, and clear. The focus of the crate is:
//! - Event timestamping
//! - Event classification
//! - Easy logging

pub mod config;

pub mod exporters;

pub mod engine;

pub fn serve_metrics() {}
