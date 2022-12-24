//! A lightweight asynchronous [smux](https://github.com/xtaci/smux) (Simple MUltipleXing) library for smol, async-std and any async runtime compatible to `futures`.

pub mod builder;
pub mod config;
pub mod error;
mod frame;
mod mux;

pub use builder::Builder as MuxBuilder;
pub use config::{MuxConfig, StreamIdType};
pub use error::{Error, Result};
pub use mux::{Mux, MuxStream};

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU64, time::Duration};

    use rand::RngCore;
    use tokio::{
        io::{duplex, AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
    };

    use crate::{
        builder::Builder as MuxBuilder, frame::MAX_PAYLOAD_SIZE, mux::TokioConn, MuxStream,
    };

    async fn get_tcp_pair() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let h = tokio::spawn(async move {
            let (a, _) = listener.accept().await.unwrap();
            a
        });

        let b = TcpStream::connect(addr).await.unwrap();
        let a = h.await.unwrap();
        a.set_nodelay(true).unwrap();
        b.set_nodelay(true).unwrap();
        (a, b)
    }

    async fn test_stream<T: TokioConn>(mut a: MuxStream<T>, mut b: MuxStream<T>) {
        const LEN: usize = MAX_PAYLOAD_SIZE + 0x200;
        let mut data1 = vec![0; LEN];
        let mut data2 = vec![0; LEN];
        rand::thread_rng().fill_bytes(&mut data1);
        rand::thread_rng().fill_bytes(&mut data2);

        let mut buf = vec![0; LEN];

        a.write_all(&data1).await.unwrap();
        a.flush().await.unwrap();
        b.write_all(&data2).await.unwrap();
        b.flush().await.unwrap();

        a.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, data2);
        b.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, data1);

        a.write_all(&data1).await.unwrap();
        a.flush().await.unwrap();
        b.read_exact(&mut buf[..LEN / 2]).await.unwrap();
        b.read_exact(&mut buf[LEN / 2..]).await.unwrap();
        assert_eq!(buf, data1);

        a.write_all(&data1[..LEN / 2]).await.unwrap();
        a.flush().await.unwrap();
        b.read_exact(&mut buf[..LEN / 2]).await.unwrap();
        assert_eq!(buf[..LEN / 2], data1[..LEN / 2]);

        a.shutdown().await.unwrap();
        b.shutdown().await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_mux() {
        let (a, b) = duplex(0x8000);
        let mut mux_a = MuxBuilder::new_client().build(a).await;
        let mut mux_b = MuxBuilder::new_server().build(b).await;

        let stream1 = mux_a.connect().unwrap();
        let stream2 = mux_b.accept().await.unwrap();
        test_stream(stream1, stream2).await;

        let stream1 = mux_b.connect().unwrap();
        let stream2 = mux_a.accept().await.unwrap();
        test_stream(stream1, stream2).await;

        assert_eq!(mux_a.get_num_channels(), 0);
        assert_eq!(mux_b.get_num_channels(), 0);

        const STREAM_NUM: usize = 0x1000;
        let mut tasks = tokio::task::JoinSet::new();
        for _ in 0..STREAM_NUM {
            let stream_a = mux_a.connect().unwrap();
            let stream_b = mux_b.accept().await.unwrap();
            tasks.spawn(async move {
                test_stream(stream_a, stream_b).await;
            });
        }

        while let Some(task) = tasks.join_next().await {
            task.unwrap();
        }

        assert_eq!(mux_a.get_num_channels(), 0);
        assert_eq!(mux_b.get_num_channels(), 0);
    }

    #[tokio::test]
    async fn test_shutdown() {
        let (a, b) = get_tcp_pair().await;
        let mux_a = MuxBuilder::new_client().build(a).await;
        let mut mux_b = MuxBuilder::new_server().build(b).await;

        let mut stream1 = mux_a.connect().unwrap();
        let mut stream2 = mux_b.accept().await.unwrap();

        let data_2_1 = [1, 2, 3, 4];
        stream2.write_all(&data_2_1).await.unwrap();
        stream2.shutdown().await.unwrap();

        tokio::time::sleep(Duration::from_secs(1)).await;

        // stream2 down, write into stream1 should fail
        let result = stream1.write_all(&[0, 1, 2, 3]).await;
        assert!(result.is_err());
        let result = stream1.flush().await;
        assert!(result.is_err());
        // reading the outstanding data still work
        let mut buf = vec![0; 4];
        let result = stream1.read_exact(&mut buf).await;
        assert!(result.is_ok());
        assert_eq!(buf, data_2_1);
        // data drained, reading from stream2 should fail
        let result = stream1.read(&mut buf).await;
        assert!(result.is_err());

        let mut stream = mux_b.connect().unwrap();
        mux_a.close().await.unwrap();

        stream.read(&mut buf).await.unwrap_err();
        stream.flush().await.unwrap_err();
        stream.shutdown().await.unwrap_err();
    }

    #[tokio::test]
    async fn test_timeout() {
        let (a, b) = duplex(1024);
        let mux_a = MuxBuilder::new_client()
            .with_idle_timeout(NonZeroU64::new(3).unwrap())
            .build(a)
            .await;
        let mut mux_b = MuxBuilder::new_server().build(b).await;

        let mut stream1 = mux_a.connect().unwrap();
        let mut stream2 = mux_b.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert!(!stream1.is_closed());
        assert!(!stream2.is_closed());

        tokio::time::sleep(Duration::from_secs(5)).await;

        assert!(stream1.is_closed());
        assert!(stream2.is_closed());
    }
}
