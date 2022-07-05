use std::env;

use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::os::unix::prelude::IntoRawFd;
use std::process::{Command, Stdio};

use nix::unistd::pipe;
use std::os::unix::io::FromRawFd;

fn get_unzip_process(s3_zip_url: String, unzip_opt: &str) -> std::process::Child {
    let unzip_l_fd = pipe().expect("failed to create pipe in get_unzip_process");

    let (unzip_l_pipe_out, unzip_l_pipe_in) = unsafe {
        (
            Stdio::from_raw_fd(unzip_l_fd.0),
            Stdio::from_raw_fd(unzip_l_fd.1),
        )
    };

    // aws s3 cp s3://sagemaker-studio-qt0kal0xm2/vox1_test_wav.zip -
    Command::new("aws")
        .args(["s3", "cp", &*s3_zip_url.clone(), "-"])
        .stdout(unzip_l_pipe_in)
        .spawn()
        .expect(&*format!("Failed to execute: aws s3 cp {s3_zip_url} -"));

    // busybox unzip -l -
    Command::new("busybox")
        .args(["unzip", unzip_opt, "-"])
        .stdin(unzip_l_pipe_out)
        .stdout(Stdio::piped())
        .spawn()
        .expect(&*format!("Failed to execute: unzip {unzip_opt} -"))
}

#[allow(dead_code)]
fn get_unzip_process_dummy(zip_name: String, unzip_opt: &str) -> std::process::Child {
    Command::new("unzip")
        .args([unzip_opt, &*zip_name])
        .stdout(Stdio::piped())
        .spawn()
        .expect(&*format!("Failed to execute: unzip {unzip_opt} zip.zip"))
}

fn get_aws_create_object_process(s3_object_url: String) -> std::process::Child {
    Command::new("aws")
        .args(["s3", "cp", "-", &*s3_object_url.clone()])
        .stdin(Stdio::piped())
        .spawn()
        .expect(&*format!("Failed to execute: aws s3 cp - {s3_object_url}"))
}

#[allow(dead_code)]
fn get_aws_create_object_process_dummy(filename: String) -> std::process::Child {
    let fd = File::create(&*filename)
        .expect("failed to create file in get_aws_create_object_process_dummy")
        .into_raw_fd();
    let file_out = unsafe { Stdio::from_raw_fd(fd) };
    Command::new("cat")
        .stdin(Stdio::piped())
        .stdout(file_out)
        .spawn()
        .expect(&*format!("Failed to execute: cat"))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() <= 2 {
        let err_text = match args.len() {
            0 => format!("Usage: program_name [bucket_name] [zip_name]"),
            1 | 2 => format!("Usage: {} [bucket_name] [zip_name]", args[0]),
            _ => "Error: wrong number of args".to_string(),
        };
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            err_text,
        )));
    }
    let s3_bucket_name = args[1].clone();
    let s3_zip_name = args[2].clone();
    let s3_zip_url = format!("s3://{s3_bucket_name}/{s3_zip_name}");
    println!("Url: {s3_zip_url}");

    let mut unzip_l_process = get_unzip_process(s3_zip_url.clone(), "-l");
    let mut unzip_p_process = get_unzip_process(s3_zip_url.clone(), "-p");

    {
        let unzip_l_stdout = BufReader::new(
            unzip_l_process
                .stdout
                .take()
                .expect("Failed to get unzip -l stdout"),
        );
        let mut unzip_p_process_bytes = unzip_p_process
            .stdout
            .take()
            .expect("Failed to get unzip -p stdout")
            .bytes();

        for line in unzip_l_stdout
            .lines()
            .map(|line_res| match line_res {
                Ok(line) => line,
                Err(_) => panic!(),
            })
            .skip(3)
            .take_while(|line| !line.contains("--------                     -------"))
        {
            let (filename, size) = (
                format!(
                    "wav/{}",
                    line.split("   wav/")
                        .skip(1)
                        .next()
                        .expect("failed to split by wav/")
                        .trim()
                ),
                line.split("  05-29")
                    .next()
                    .expect("failed to split by 05-29")
                    .trim()
                    .parse::<usize>()
                    .expect("failed to parse usize"),
            );
            println!("File: {filename} {size}       \r");
            if !filename.ends_with('/') && size != 0 {
                let mut create_object_process =
                    get_aws_create_object_process(format!("s3://{s3_bucket_name}/{filename}"));
                {
                    let mut create_object_process_stdin = create_object_process
                        .stdin
                        .take()
                        .expect("Failed to get stdin of object");
                    let buf: Vec<u8> = unzip_p_process_bytes
                        .by_ref()
                        .take(size)
                        .map(|b_res| b_res.expect("Failed to unpack u8"))
                        .collect();
                    if let Err(err) = create_object_process_stdin.write(&buf) {
                        panic!("Error writing to create_object_process_stdin: {}", err);
                    }
                }
                create_object_process
                    .wait()
                    .expect("failed to wait for create_object_process");
            }
        }
    }

    unzip_l_process.wait().expect("failed to wait for unzip -l");
    unzip_p_process.wait().expect("failed to wait for unzip -p");

    Ok(())
}
