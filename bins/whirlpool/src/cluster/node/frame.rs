//! frames are what are sent to and from nodes.

use base64::Engine;
use log::trace;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::io;
use std::io::ErrorKind;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// The struct that is sent to and from nodes
#[derive(Debug)]
pub struct Frame<T: DeserializeOwned + Serialize> {
    body: T,
}

impl<T: DeserializeOwned + Serialize> Frame<T> {
    /// Create a new frame
    pub fn new(body: T) -> Self {
        Self { body }
    }

    /// Unwraps the frame into a body
    pub fn unwrap(self) -> T {
        self.body
    }
}

/// An async frame writer
#[derive(Debug)]
pub struct AsyncFrameWriter<W: AsyncWrite + Unpin> {
    writer: W,
}

const MAGIC_NUMBER: u64 = 0x575f504f4f4c;
const LEN_SIZE: usize = std::mem::size_of::<u64>();
const RESERVED_SIZE: usize = std::mem::size_of::<u32>();
const MAGIC_NUMBER_SIZE: usize = std::mem::size_of::<u64>();
const BASE_FRAME_SIZE: usize = LEN_SIZE + RESERVED_SIZE + MAGIC_NUMBER_SIZE;

impl<W: AsyncWrite + Unpin> AsyncFrameWriter<W> {
    /// Creates a new frame writer
    pub async fn new(writer: W) -> Self {
        Self { writer }
    }

    pub async fn flush(&mut self) -> io::Result<()> {
        self.writer.flush().await
    }

    pub async fn write_frame<T: Serialize + DeserializeOwned>(
        &mut self,
        frame: Frame<T>,
    ) -> io::Result<usize> {
        let mut buffered = vec![];
        serde_json::to_writer_pretty(&mut buffered, &frame.body)?;

        let encoded = base64::engine::general_purpose::STANDARD_NO_PAD
            .encode(buffered)
            .into_bytes();

        let data_length = encoded.len();
        let frame_length = BASE_FRAME_SIZE + data_length;

        self.writer.write_u64(frame_length as u64).await?;
        self.writer.write_all(&[0; RESERVED_SIZE]).await?;
        self.writer.write_u64(MAGIC_NUMBER).await?;
        self.writer.write_all(&encoded).await?;

        Ok(frame_length)
    }
}

/// An async frame reader
#[derive(Debug)]
pub struct AsyncFrameReader<R: AsyncRead + Unpin> {
    reader: R,
}

impl<R: AsyncRead + Unpin> AsyncFrameReader<R> {
    pub async fn new(reader: R) -> Self {
        Self { reader }
    }

    pub async fn read_frame<T: Serialize + DeserializeOwned>(&mut self) -> io::Result<Frame<T>> {
        let frame_length = self.reader.read_u64().await?;
        let mut reserved = [0; RESERVED_SIZE];
        self.reader.read_exact(&mut reserved).await?;
        let magic = self.reader.read_u64().await?;
        if magic != MAGIC_NUMBER {
            return Err(io::Error::new(
                ErrorKind::InvalidData,
                "magic number is wrong",
            ));
        }

        let mut encoded_data = vec![0_u8; (frame_length as usize - BASE_FRAME_SIZE)];
        self.reader.read_exact(&mut encoded_data).await?;
        let decoded = base64::engine::general_purpose::STANDARD_NO_PAD
            .decode(encoded_data)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;

        let body = serde_json::from_slice::<T>(&decoded)?;
        Ok(Frame::new(body))
    }
}

#[cfg(test)]
mod tests {
    use crate::cluster::node::frame::{AsyncFrameReader, AsyncFrameWriter, Frame};
    use clap::builder::Str;
    use tokio::io::duplex;

    #[tokio::test]
    async fn write_read_frame() {
        let value = "Hello, World".to_string();
        let (writer, reader) = duplex(4096);

        let mut writer = AsyncFrameWriter::new(writer).await;
        let mut reader = AsyncFrameReader::new(reader).await;

        writer.write_frame(Frame::new(value.clone())).await.unwrap();

        let read_frame = reader.read_frame::<String>().await.unwrap();
        assert_eq!(read_frame.unwrap(), value);
    }
}
