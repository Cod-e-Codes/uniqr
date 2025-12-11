use std::fmt;
use std::io;

/// Error types for uniqr
#[derive(Debug)]
pub enum Error {
    /// I/O error
    Io(io::Error),
    /// Invalid argument
    InvalidArgument(String),
    /// UTF-8 conversion error
    Utf8Error(std::string::FromUtf8Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(e) => write!(f, "I/O error: {}", e),
            Error::InvalidArgument(msg) => write!(f, "Invalid argument: {}", msg),
            Error::Utf8Error(e) => write!(f, "UTF-8 error: {}", e),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Utf8Error(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(err)
    }
}

impl From<std::string::FromUtf8Error> for Error {
    fn from(err: std::string::FromUtf8Error) -> Self {
        Error::Utf8Error(err)
    }
}

/// Result type alias for uniqr
pub type Result<T> = std::result::Result<T, Error>;
