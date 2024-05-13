use std::fs::OpenOptions;
use std::io::BufRead;
use std::io::BufReader;

#[cfg(feature = "aws-s3")]
use std::io::Cursor;
use std::io::ErrorKind;

use async_trait::async_trait;


#[cfg(feature = "aws-s3")]
use aws_sdk_s3::primitives::ByteStream;

#[cfg(feature = "aws-s3")]
use aws_config::SdkConfig;

#[derive(Debug)]
pub struct FileProviderError {
    details: String,
}

impl FileProviderError {
    pub fn new(msg: &str) -> FileProviderError {
        FileProviderError{details: msg.to_string()}
    }
}

impl std::fmt::Display for FileProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f,"{}",self.details)
    }
}

impl std::error::Error for FileProviderError {
    fn description(&self) -> &str {
        &self.details
    }
}

impl From<std::io::Error> for FileProviderError {
    fn from(err: std::io::Error) -> FileProviderError {
        return FileProviderError::new(&err.to_string());
    }
}

#[cfg(feature = "aws-s3")]
impl<T> From<aws_sdk_s3::error::SdkError<T>> for FileProviderError {
    fn from(err: aws_sdk_s3::error::SdkError<T>) -> Self {
        return FileProviderError::new(&err.to_string());
    }
}

type Error = FileProviderError;

#[derive(Debug)]
pub struct FileEntry {
    pub name: String,
    pub size: u64,
}

#[async_trait]
pub trait FileProvider : Send + Sync {    
    async fn write(&self, path: &str, content: &str) -> Result<(), Error>;
    async fn write_file(&self, path: String, contents: Vec<u8>) -> Result<(), Error>;
    async fn read_file_buffer(&self, path: &str) -> Result<Box<dyn BufRead>, Error>;
    async fn delete_file(&self, path: &str) -> Result<(), Error>;

    async fn read_dir(&self, path: &str) -> Result<Vec<FileEntry>, Error>;
    async fn list_dir(&self, path: &str) -> Result<Vec<String>, Error>;
    async fn create_dir(&self, path: &str) -> Result<(), Error>;

    async fn move_file(&self, file_path: &str, ending_path: &str, delete: bool) -> Result<(), Error>;

    fn get_base_path(&self) -> &str;
}
#[derive(Debug)]
pub struct LocalFileProvider {
    pub base: String,
}

#[derive(Debug)]
#[cfg(feature = "aws-s3")]
pub struct AwsS3FileProvider {
    pub bucket: String,
    client: aws_sdk_s3::Client,
}

#[cfg(feature = "aws-s3")]
impl AwsS3FileProvider {
    pub async fn new(bucket: String, config: &SdkConfig) -> AwsS3FileProvider {
        return AwsS3FileProvider {
            bucket: bucket,
            client: aws_sdk_s3::Client::new(config)
        };
    }
}

#[async_trait]
impl FileProvider for LocalFileProvider {

    fn get_base_path(&self) -> &str {
        return &self.base;
    }

    async fn read_file_buffer(&self, path: &str) -> Result<Box<dyn BufRead>, Error> {
        let path = [&self.base, path].join("");

        let file_desc = match OpenOptions::new().read(true).open(&path) {
            Ok(file) => file,
            Err(e) => return Err(Error::new(&format!("File opening error: {}\nPath: {}", e.to_string(), path))),
        };

        let buffer = Box::new(BufReader::new(file_desc));

        Ok(buffer)
    }

    async fn move_file(&self, file_path: &str, ending_path: &str, delete: bool) -> Result<(), Error> {
        //use std::fs::rename;
        
        std::fs::copy([&self.base, file_path].join(""), ending_path)?;

        if delete {
            std::fs::remove_file([&self.base, file_path].join(""))?;
        }

        return Ok(());
    }

    async fn read_dir(&self, path: &str) -> Result<Vec<FileEntry>, Error> {

        let mut entries = Vec::new();
        for entry in std::fs::read_dir([&self.base, path].join(""))? {
            let entry = entry?;

            let file_entry = FileEntry {
                name: entry.file_name().to_string_lossy().to_string(),
                size: entry.metadata().unwrap().len()
            };

            entries.push(file_entry);
        }

        return Ok(entries);
    }

    async fn write_file(&self, path: String, contents: Vec<u8>) -> Result<(), Error> {
        use std::fs::write;

        write([self.base.clone(), path].join(""), contents)?;
        Ok(())
    }

    async fn write(&self, path: &str, contents: &str) -> Result<(), Error> {
        use std::fs::write;

        write([&self.base, path].join(""), contents)?;
        Ok(())
    }

    async fn delete_file(&self, path: &str) -> Result<(), Error> {
        use std::fs::remove_file;

        remove_file([&self.base, path].join(""))?;
        Ok(())
    }

    async fn list_dir(&self, path: &str) -> Result<Vec<String>, Error> {
        let res = self.read_dir(&[&self.base, path].join("")).await?;
        
        Ok(res.into_iter().map(|item| {
            [&self.base, path, "/", &item.name].join("")
        }).collect::<Vec<String>>())
    }

    async fn create_dir(&self, path: &str)  -> Result<(), Error> {
        let location = [&self.base, "/", path].join("");
        println!("\nCreating dir at {}\n", location);
        match std::fs::create_dir(location) {
            Ok(_) => {}
            Err(e) => {
                match e.kind() {
                    ErrorKind::AlreadyExists => {},
                    _ => {
                        return Err(e.into())
                    }
                }
            }
        };

        Ok(())
    }
}


#[async_trait]
#[cfg(feature = "aws-s3")]
impl FileProvider for AwsS3FileProvider {
    async fn read_file_buffer(&self, path: &str) -> Result<Box<dyn BufRead>, Error> {
        let resp = match self.client.get_object().bucket(self.bucket.clone()).key(path).send().await {
            Ok(out) => out,
            Err(e) => {
                eprintln!("Failure reading the file buffer: {:?}, Bucket: {}, Key: {}", e, self.bucket, path);
                return Err(Error::new(e.to_string().as_ref()));
            }
        };

        match resp.body.collect().await {
            Ok(out) => {
                let data = out.to_vec();
                let buf = BufReader::new(Cursor::new(data));

                return Ok(Box::new(buf));
            },
            Err(e) => {
                eprintln!("Failure decoding the file buffer: {:?}", e);
                return Err(Error::new(e.to_string().as_ref()));
            }
        };
    }

    async fn move_file(&self, file_name: &str, ending_path: &str, delete: bool) -> Result<(), Error> {

        let split_file_location: Vec<&str> = ending_path.split("/").collect::<Vec<&str>>();
        let ending_file_location = split_file_location.first().expect("You didn't supply a / with your request. It should be {bucket}/{file_name}");
        
        println!("Moving file from {:?} to {:?}", [&self.bucket, "/", file_name].join(""), ending_path);
        
        self.client
        .copy_object()
        .copy_source([&self.bucket, "/", file_name].join(""))
        .bucket(*ending_file_location)
        .key(file_name)
        .send()
        .await?;

        if delete {
            self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(file_name)
            .send()
            .await?;
        }

        Ok(())
    }

    /* Path is unused here */
    async fn read_dir(&self, prefix: &str) -> Result<Vec<FileEntry>, Error> {
        let resp = self.client
        .list_objects_v2()
        .bucket(self.bucket.clone())
        .prefix(prefix)
        .send()
        .await?;

        let mut entries = Vec::new();
        for entry in resp.contents.unwrap() {

            let file_entry = FileEntry {
                name: entry.key.unwrap(),
                size: entry.size.unwrap() as u64,
            };

            entries.push(file_entry);
        }


        Ok(entries)
    }

    async fn list_dir(&self, _: &str) -> Result<Vec<String>, Error> {
        let res = self.read_dir("").await?;
        
        Ok(res.into_iter().map(|item| {
            [&self.bucket, "/", &item.name].join("")
        }).collect::<Vec<String>>())
    }

    async fn write_file(&self, path: String, contents: Vec<u8>) -> Result<(), Error> {
        match self.client.put_object()
        .bucket(self.bucket.clone())
        .key(&path)
        .body(ByteStream::from(contents))
        .send()
        .await {
            Ok(_) => {
                let valu = format!("{} uploaded to {}", path, self.bucket);
                println!("{}", valu)
            },
            Err(e) => {
                let valu = format!("\n\n{} uploaded failed {:?}\n\n", path, e);
                println!("{}", valu)
            }
        };

        Ok(())
    }

    async fn write(&self, path: &str, content: &str) -> Result<(), Error> {
        unimplemented!()
    }

    async fn delete_file(&self, path: &str) -> Result<(), Error> {
        unimplemented!()
    }

    async fn create_dir(&self, path: &str) -> Result<(), Error> {
        unimplemented!()
    }

    fn get_base_path(&self) -> &str {
        &self.bucket
    }

}
