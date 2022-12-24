use std::num::NonZeroU64;

use crate::{
    config::{MuxConfig, StreamIdType},
    mux::{Mux, TokioConn},
};

/// Build a `Mux`.
/// Note that `client` and `server` simply correspond to the two ends of the
/// connection. Both of them can initiate or accept streams.
#[derive(Clone, Debug)]
pub struct Builder {
    config: MuxConfig,
}

impl Builder {
    #[must_use]
    pub fn new_client() -> Self {
        Self {
            config: MuxConfig {
                stream_id_type: StreamIdType::Odd,
                keep_alive_interval: None,
                idle_timeout: None,
            },
        }
    }
    #[must_use]
    pub fn new_server() -> Self {
        Self {
            config: MuxConfig {
                stream_id_type: StreamIdType::Even,
                keep_alive_interval: None,
                idle_timeout: None,
            },
        }
    }
    pub fn with_keep_alive_interval(&mut self, interval_secs: NonZeroU64) -> &mut Self {
        self.config.keep_alive_interval = Some(interval_secs);
        self
    }

    pub fn with_idle_timeout(&mut self, timeout_secs: NonZeroU64) -> &mut Self {
        self.config.idle_timeout = Some(timeout_secs);
        self
    }

    pub async fn build<T: TokioConn>(&mut self, connection: T) -> Mux<T> {
        Mux::from_connection(connection, self.config).await
    }
}
