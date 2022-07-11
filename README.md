# S3-unzip

A simple Rust library to unzip an S3 archive into S3's root directory.
Works without downloading the archive or putting it into RAM, so will unzip a huge dataset
archive even if run on `t2.micro`(restrictions apply).

### Capabilities
- Works without downloading the archive or putting it into RAM, so will unzip a huge dataset
archive even if run on `t2.micro`

### Restrictions
- The only condition for it to work currently is that your machine has more RAM then the largest archive's file.
- If you want to add streaming the files directly to S3 as they get unzipped you can make a PR, we will be very thankful for that!
- May not work with archives generated with macos archiver. Guaranteed to work with archives produced from `zip`.

### Usage
`s3-unzip -v -r [region_name] -p [prefix_name] <bucket_name> <zip_name>`
- `-v` turns on verbose mode. Prints the files to the stdout as they unpack
- pass `-r [region_name]` to specify region name. `us-east-1` by default.
- pass `-p [prefix_name]` to prefix all unpacked S3 object keys with a value. This evvectively puts the unpacked files into a folder is you specify something like `-p path/to/folder/`
- specify `<bucket_name>` and `<zip_name>` to find the archive.

### Credentials
For the program to find your credentials, `aws configure` them with the `aws-cli` client on your machine.
Note that those must have access to your bucket or global access.

### Cross-compilation
If you want to cross-compile this to your EC2 instance from a `macOS` host, you might try these steps (linker installation procedure may differ in your case):
1. Add the new target using rustup: `rustup target add x86_64-unknown-linux-gnu`
2. Install the cross-platform linker using `brew`: `brew tap SergioBenitez/osxct && brew install x86_64-unknown-linux-gnu`
3. Compile using a custom linker: ```CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=x86_64-linux-gnu-gcc cargo build --target=x86_64-unknown-linux-gnu --release```.
4. Your binary is now in `target/x86_64-unknown-linux-gnu/release/s3-unzip`. You might want to copy it to the server using `scp` and use `screen -d -m <command>` to execute it in the background (see `screen` docs for details).

### Lambda usage
This package is not lambda-compatible since lambda limits the execution time to 15 minutes. If you want to make it compatible, though, feel free to make a PR.