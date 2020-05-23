pub mod gmail;
pub mod gauth;
mod multipart;

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
