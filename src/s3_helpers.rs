use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::{Client, Region, types::ByteStream};

pub fn new_invalid_input_error(err_text: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, err_text)
}

pub async fn get_client(region_name: String) -> Client {
    let region_provider = RegionProviderChain::first_try(Region::new(region_name))
        .or_default_provider()
        .or_else("us-east-1");
    let config = aws_config::from_env().region(region_provider).load().await;
    Client::new(&config)
}

pub async fn upload_object(
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

pub async fn check_bucket_in_list(
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

pub async fn check_object_exists(
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