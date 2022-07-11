mod s3_helpers;
mod s3_object_reader;

use s3_object_reader::S3ObjectReader;
use s3_helpers::{get_client, check_bucket_in_list, upload_object};

use std::io::Read;

use log::trace;
use aws_sdk_s3::Client;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct ProgramArgs {
    /// Activate verbose mode
    #[structopt(short, long)]
    verbose: bool,

    /// Specify region name
    #[structopt(short, long, default_value="us-east-1")]
    region_name: String,

    /// Specify unpacked objects' key prefix (folder)
    #[structopt(short, long, default_value="/")]
    prefix_name: String,

    /// Bucket name
    #[structopt()]
    bucket_name: String,

    /// archive key name
    #[structopt()]
    zip_name: String,
}

async fn unzip_and_upload(client: &Client, program_args: &ProgramArgs) -> std::io::Result<()> {
    let ProgramArgs {
        bucket_name, zip_name,
        verbose, region_name: _, prefix_name
    } = program_args;
    let mut object_reader = S3ObjectReader::new(client, bucket_name, zip_name);

    let mut buf: Box<[u8]>;

    loop {
        match zip::read::read_zipfile_from_stream(&mut object_reader) {
            Ok(Some(mut file)) => {
                if *verbose {
                    println!(
                        "{}: {} bytes ({} bytes packed)",
                        file.name(),
                        file.size(),
                        file.compressed_size()
                    );
                }
                
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

                let object_name = &*format!(
                    "{}{}", prefix_name, file.enclosed_name()
                    .expect(&*format!("error: unzip: path is {}", file.name()))
                    .to_str()
                    .expect(&*format!("error: unzip: couldn't convert path to string: {}", file.name()))
                );

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
    let program_args = ProgramArgs::from_args();

    let client = get_client(program_args.region_name.clone()).await;

    check_bucket_in_list(&client, program_args.bucket_name.clone()).await?;

    unzip_and_upload(&client, &program_args).await?;

    Ok(())
}
