use std::io::Read;

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{Client, Region, types::ByteStream};
use log::trace;
use tokio::runtime::Handle;

fn new_invalid_input_error(err_text: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, err_text)
}

fn get_args() -> Result<(String, String, String), std::io::Error> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() <= 3 {
        let err_text = match args.len() {
            0 => format!("Usage: program_name [region_name] [bucket_name] [zip_name]"),
            1 | 2 | 3 => format!("Usage: {} [region_name] [bucket_name] [zip_name]", args[0]),
            _ => "Error: wrong number of args".to_string(),
        };
        return Err(new_invalid_input_error(err_text));
    }
    Ok((args[1].clone(), args[2].clone(), args[3].clone()))
}

async fn get_client(region_name: String) -> Client {
    let region_provider = RegionProviderChain::first_try(Region::new(region_name))
        .or_default_provider()
        .or_else("us-east-1");
    let config = aws_config::from_env().region(region_provider).load().await;
    Client::new(&config)
}

async fn check_bucket_in_list(
    client: &Client,
    s3_bucket_name: String,
) -> Result<(), std::io::Error> {
    let resp = match client.list_buckets().send().await {
        Ok(buckets) => buckets,
        Err(err) => {
            return Err(new_invalid_input_error(format!(
                "Could not list_buckets on client: {}",
                err
            )))
        }
    };
    let buckets = resp.buckets().unwrap_or_default();
    if buckets
        .iter()
        .all(|bucket| bucket.name().unwrap_or_default() != s3_bucket_name)
    {
        return Err(new_invalid_input_error(format!(
            "Bucket {s3_bucket_name} is not available!"
        )));
    }

    Ok(())
}

async fn check_object_exists(
    client: &Client,
    s3_bucket_name: String,
    s3_object_name: String,
) -> Result<(), std::io::Error> {
    let resp = match client.list_objects_v2().bucket(s3_bucket_name).send().await {
        Ok(objects) => objects,
        Err(err) => {
            return Err(new_invalid_input_error(format!(
                "Could not list_objects_v2 on client: {}",
                err
            )))
        }
    };

    let objects = resp.contents().unwrap_or_default();
    if objects
        .iter()
        .all(|object| object.key().unwrap_or_default() != s3_object_name)
    {
        return Err(new_invalid_input_error(format!(
            "Object {s3_object_name} is not available!"
        )));
    }

    Ok(())
}

// Upload a file to a bucket.
// snippet-start:[s3.rust.s3-helloworld]
async fn upload_object(
    client: &Client,
    bucket_name: &str,
    object_name: &str,
    boxed_body: Box<[u8]>
) -> std::io::Result<()> {
    let body = ByteStream::from(boxed_body.into_vec());

    if let Err(err) = client
        .put_object()
        .bucket(bucket_name)
        .key(object_name)
        .body(body)
        .send()
        .await 
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            format!("error: upload_object: {:?}", err),
        ))
    };

    Ok(())
}

const OBJECT_READER_MAX_BUF_SIZE: usize = 1024 * 1024 * 16;

struct S3ObjectReader<'a> {
    client: &'a Client,
    bucket_name: &'a String,
    object_name: &'a String,
    current_object_byte_offset: usize,
    buf: Box<[u8]>,
    buf_byte_offset: usize,
    buf_bytes_available: usize,
}

impl<'a> S3ObjectReader<'a> {
    fn new(client: &'a Client, bucket_name: &'a String, object_name: &'a String) -> Self {
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

async fn unzip_and_upload(client: &Client, bucket_name: &String, zip_name: &String) -> std::io::Result<()> {
    let mut object_reader = S3ObjectReader::new(client, bucket_name, zip_name);

    let mut buf: Box<[u8]>;

    loop {
        match zip::read::read_zipfile_from_stream(&mut object_reader) {
            Ok(Some(mut file)) => {
                println!(
                    "{}: {} bytes ({} bytes packed)",
                    file.name(),
                    file.size(),
                    file.compressed_size()
                );
                
                buf = (vec![0u8; file.size() as usize]).into_boxed_slice();
                let mut offset = 0;

                while offset != file.size() as usize {
                    match file.read(&mut buf[offset..]) {
                        Ok(n) => {
                            if n == 0 {
                                return Err(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    format!("error: unzip: the uncompressed file size was not met: file.size(): {}", file.size()),
                                ))
                            }
                            offset += n;
                            trace!("unzip: read {} / {}", offset, file.size());
                        },
                        Err(e) => return Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("error: unzip: could not read the file: {:?}", e),
                        ))
                    };
                }

                let object_name = file.enclosed_name()
                    .expect(&*format!("error: unzip: path is {}", file.name()))
                    .to_str()
                    .expect(&*format!("error: unzip: couldn't convert path to string: {}", file.name()));

                upload_object(client, bucket_name, object_name, buf).await?;
            }
            Ok(None) => break,
            Err(e) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("error: unzip: {:?}", e),
                ))
            }
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), std::io::Error> {
    let (s3_region_name, s3_bucket_name, s3_zip_name) = get_args()?;

    let client = get_client(s3_region_name).await;

    check_bucket_in_list(&client, s3_bucket_name.clone()).await?;

    check_object_exists(&client, s3_bucket_name.clone(), s3_zip_name.clone()).await?;

    unzip_and_upload(&client, &s3_bucket_name, &s3_zip_name).await?;

    Ok(())
}
