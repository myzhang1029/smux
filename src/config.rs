use std::{fmt::Display, num::NonZeroU64};

#[derive(Clone, Copy, Debug, Hash, Ord, PartialOrd, PartialEq, Eq)]
pub enum StreamIdType {
    Even = 0,
    Odd = 1,
}

impl Display for StreamIdType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamIdType::Even => write!(f, "server"),
            StreamIdType::Odd => write!(f, "client"),
        }
    }
}

/// Mux configuration.
#[derive(Clone, Copy, Debug, Hash, Ord, PartialOrd, PartialEq, Eq)]
pub struct MuxConfig {
    pub stream_id_type: StreamIdType,
    pub keep_alive_interval: Option<NonZeroU64>,
    pub idle_timeout: Option<NonZeroU64>,
}
