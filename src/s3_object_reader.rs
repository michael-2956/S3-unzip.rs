use log::trace;
use std::io::Read;
use aws_sdk_s3::Client;
use tokio::runtime::Handle;

const OBJECT_READER_MAX_BUF_SIZE: usize = 1024 * 1024 * 16;

pub struct S3ObjectReader<'a> {
    client: &'a Client,
    bucket_name: &'a String,
    object_name: &'a String,
    current_object_byte_offset: usize,
    buf: Box<[u8]>,
    buf_byte_offset: usize,
    buf_bytes_available: usize,
}

impl<'a> S3ObjectReader<'a> {
    pub fn new(client: &'a Client, bucket_name: &'a String, object_name: &'a String) -> Self {
        Self {
            client,
            bucket_name,
            object_name,
            current_object_byte_offset: 0,
            buf: vec![0; OBJECT_READER_MAX_BUF_SIZE].into_boxed_slice(),
            buf_byte_offset: 0,
            buf_bytes_available: 0,
        }
    }

    fn refresh_buf(&mut self) -> std::io::Result<usize> {
        trace!("Enter-refresh");
        let handle = Handle::current();
        let buffer = {
            trace!("Refresh-get-handle-success");
            let _ = handle.enter();
            trace!("Refresh-enter-handle-success");
            let client = self.client;
            let bucket_name = self.bucket_name.clone();
            let object_name = self.object_name.clone();
            let current_object_byte_offset = self.current_object_byte_offset;
            futures::executor::block_on(async move {
                match match client
                    .get_object()
                    .bucket(bucket_name)
                    .key(object_name)
                    .range(format!(
                        "bytes={}-{}",
                        current_object_byte_offset,
                        current_object_byte_offset + OBJECT_READER_MAX_BUF_SIZE - 1
                    ))
                    .send()
                    .await
                {
                    Ok(buffer) => {
                        trace!("Refresh-request-succcess");
                        buffer
                    },
                    Err(err) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::ConnectionRefused,
                            format!("client.get_object failed with: {}", err),
                        ))
                    }
                }
                .body
                .collect()
                .await
                {
                    Ok(bytes) => {
                        trace!("Refresh-collect-body-succcess");
                        Ok(bytes)
                    },
                    Err(err) => {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::ConnectionAborted,
                            format!("client.get_object failed with: {}", err),
                        ))
                    }
                }
            })
        }?;

        trace!("Refresh-exit-async-succcess");

        let buffer_bytes = buffer.into_bytes();
        let data = match buffer_bytes.chunks(OBJECT_READER_MAX_BUF_SIZE).next() {
            Some(data) => data,
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("client.get_object failed with: chunk is None"),
                ))
            }
        };

        trace!("Refresh-get-bytes-succcess");

        let read_bytes_num = data.len().min(OBJECT_READER_MAX_BUF_SIZE);

        self.buf[..read_bytes_num].clone_from_slice(&data[..read_bytes_num]);

        self.current_object_byte_offset += read_bytes_num;
        self.buf_bytes_available = read_bytes_num;
        self.buf_byte_offset = 0;

        trace!("Refresh-bytes-clone-succcess");

        Ok(read_bytes_num)
    }
}

impl<'a> Read for S3ObjectReader<'a> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        trace!("Enter read...");
        let mut bytes_needed = buf.len();
        let mut argbuf_bytes_read = 0;

        while bytes_needed > 0 {
            let bytes_toread = bytes_needed.min(self.buf_bytes_available);
            if bytes_toread > 0 {
                trace!("Available bytes! {}", bytes_toread);
                buf[argbuf_bytes_read..(argbuf_bytes_read + bytes_toread)].clone_from_slice(
                    &self.buf[self.buf_byte_offset..(self.buf_byte_offset + bytes_toread)],
                );
                self.buf_bytes_available -= bytes_toread;
                self.buf_byte_offset += bytes_toread;
                argbuf_bytes_read += bytes_toread;
                bytes_needed -= bytes_toread;
            }
            if bytes_needed > 0 {
                trace!("Bytes needed... {}", bytes_needed);
                // buffer has exhausted. Update it:
                let read_bytes_num = self.refresh_buf()?;
                if read_bytes_num == 0 {
                    break;
                }
            }
        }

        Ok(argbuf_bytes_read)
    }
}