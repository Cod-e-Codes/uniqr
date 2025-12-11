//! # uniqr
//!
//! A library for line deduplication with various strategies.
//!
//! ## Example
//!
//! ```
//! use uniqr::{deduplicate, DeduplicationMode, DeduplicationOptions};
//! use std::io::Cursor;
//!
//! let input = b"line1\nline2\nline1\nline3\n";
//! let mut output = Vec::new();
//!
//! let options = DeduplicationOptions {
//!     mode: DeduplicationMode::KeepFirst,
//!     ignore_case: false,
//!     count: false,
//!     show_removed: false,
//!     column: None,
//!     use_disk: false,
//! };
//!
//! deduplicate(Cursor::new(input), &mut output, &options).unwrap();
//! assert_eq!(output, b"line1\nline2\nline3\n");
//! ```

use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};

#[cfg(feature = "fast-hash")]
use ahash::HashMap as AHashMap;

#[cfg(not(feature = "fast-hash"))]
use std::collections::HashMap;

pub mod error;
pub use error::{Error, Result};

/// Deduplication strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeduplicationMode {
    /// Keep first occurrence of each line (default)
    KeepFirst,
    /// Keep last occurrence of each line (two-pass)
    KeepLast,
    /// Remove all lines that appear more than once (two-pass)
    RemoveAll,
}

/// Options for deduplication
#[derive(Debug, Clone)]
pub struct DeduplicationOptions {
    pub mode: DeduplicationMode,
    pub ignore_case: bool,
    pub count: bool,
    pub show_removed: bool,
    pub column: Option<usize>,
    /// Use disk-backed storage for massive files (requires 'disk-backed' feature)
    pub use_disk: bool,
}

impl Default for DeduplicationOptions {
    fn default() -> Self {
        Self {
            mode: DeduplicationMode::KeepFirst,
            ignore_case: false,
            count: false,
            show_removed: false,
            column: None,
            use_disk: false,
        }
    }
}

/// Statistics about deduplication
#[derive(Debug, Default)]
pub struct DeduplicationStats {
    pub lines_read: usize,
    pub lines_written: usize,
    pub lines_removed: usize,
    pub unique_lines: usize,
}

/// Main deduplication function (safe for non-seekable streams)
///
/// Note: This function cannot perform disk-backed two-pass deduplication
/// (`KeepLast` or `RemoveAll` with `use_disk: true`) because they require
/// a seekable input source. Use `deduplicate_seekable` for those cases.
pub fn deduplicate<R: std::io::Read, W: Write>(
    input: R,
    output: &mut W,
    options: &DeduplicationOptions,
) -> Result<DeduplicationStats> {
    #[cfg(feature = "disk-backed")]
    if options.use_disk {
        match options.mode {
            DeduplicationMode::KeepFirst => {
                return deduplicate_keep_first_disk(input, output, options);
            }
            DeduplicationMode::KeepLast | DeduplicationMode::RemoveAll => {
                return Err(Error::InvalidArgument(
                    "Disk-backed KeepLast and RemoveAll modes require a seekable input. Use deduplicate_seekable() or provide a file.".to_string(),
                ));
            }
        }
    }

    let stats = match options.mode {
        DeduplicationMode::KeepFirst => deduplicate_keep_first(input, output, options),
        DeduplicationMode::KeepLast => deduplicate_keep_last(input, output, options),
        DeduplicationMode::RemoveAll => deduplicate_remove_all(input, output, options),
    }?;
    output.flush()?;
    Ok(stats)
}

/// Deduplication function for seekable inputs (supports all modes)
pub fn deduplicate_seekable<R: std::io::Read + std::io::Seek, W: Write>(
    input: R,
    output: &mut W,
    options: &DeduplicationOptions,
) -> Result<DeduplicationStats> {
    #[cfg(feature = "disk-backed")]
    if options.use_disk {
        match options.mode {
            DeduplicationMode::KeepLast => {
                return deduplicate_keep_last_disk(input, output, options);
            }
            DeduplicationMode::RemoveAll => {
                return deduplicate_remove_all_disk(input, output, options);
            }
            _ => {
                // KeepFirst (disk) and in-memory modes don't strictly *need* Seek,
                // so we can delegate to the standard function.
                return deduplicate(input, output, options);
            }
        }
    }

    // Default to standard deduplicate if disk-backed is not used
    let stats = deduplicate(input, output, options)?;
    output.flush()?;
    Ok(stats)
}

/// One-pass keep-first algorithm
fn deduplicate_keep_first<R: std::io::Read, W: Write>(
    input: R,
    output: &mut W,
    options: &DeduplicationOptions,
) -> Result<DeduplicationStats> {
    let mut reader = BufReader::new(input);
    let mut stats = DeduplicationStats::default();

    #[cfg(feature = "fast-hash")]
    type MapType = AHashMap<Vec<u8>, usize>;

    #[cfg(not(feature = "fast-hash"))]
    type MapType = HashMap<Vec<u8>, usize>;

    let mut seen: MapType = MapType::default();
    let mut lines_for_count = Vec::new();

    let mut line = Vec::new();
    while reader.read_until(b'\n', &mut line)? > 0 {
        stats.lines_read += 1;

        // Strip newline for key generation but keep for output
        let key_line = if line.ends_with(b"\n") {
            if line.ends_with(b"\r\n") {
                &line[..line.len() - 2]
            } else {
                &line[..line.len() - 1]
            }
        } else {
            &line[..]
        };

        let key = make_key(key_line, options)?;
        let count = seen.entry(key).or_insert(0);
        *count += 1;

        if *count == 1 {
            if options.count {
                lines_for_count.push(line.clone());
            } else {
                output.write_all(&line)?;
            }
            stats.lines_written += 1;
        } else {
            stats.lines_removed += 1;
            if options.show_removed {
                write!(output, "[REMOVED] ")?;
                output.write_all(&line)?;
            }
        }
        line.clear();
    }

    stats.unique_lines = seen.len();

    // Write counts if requested
    if options.count {
        for line in lines_for_count {
            let _key = make_key(&line, options)?; // Correct key generation logic needed here too if stripping happened above, but line has newline now.
            // Actually, lines_for_count stores full lines with newlines.
            // make_key expects just content. We need to strip again or refactor make_key.
            // Let's strip locally.
            let key_line = if line.ends_with(b"\n") {
                if line.ends_with(b"\r\n") {
                    &line[..line.len() - 2]
                } else {
                    &line[..line.len() - 1]
                }
            } else {
                &line[..]
            };

            let key = make_key(key_line, options)?;

            if let Some(&cnt) = seen.get(&key) {
                write!(output, "{:>7} ", cnt)?;
                output.write_all(&line)?;
            }
        }
    }

    Ok(stats)
}

/// Two-pass keep-last algorithm
fn deduplicate_keep_last<R: std::io::Read, W: Write>(
    input: R,
    output: &mut W,
    options: &DeduplicationOptions,
) -> Result<DeduplicationStats> {
    let mut reader = BufReader::new(input);
    let mut stats = DeduplicationStats::default();

    #[cfg(feature = "fast-hash")]
    type MapType = AHashMap<Vec<u8>, (usize, Vec<u8>)>;

    #[cfg(not(feature = "fast-hash"))]
    type MapType = HashMap<Vec<u8>, (usize, Vec<u8>)>;

    let mut last_occurrence: MapType = MapType::default();
    let mut lines = Vec::new();

    // First pass: read all lines and track last occurrence
    let mut line = Vec::new();
    while reader.read_until(b'\n', &mut line)? > 0 {
        stats.lines_read += 1;

        let key_line = if line.ends_with(b"\n") {
            if line.ends_with(b"\r\n") {
                &line[..line.len() - 2]
            } else {
                &line[..line.len() - 1]
            }
        } else {
            &line[..]
        };

        let key = make_key(key_line, options)?;
        last_occurrence.insert(key, (stats.lines_read - 1, line.clone()));
        lines.push(line.clone());
        line.clear();
    }

    stats.unique_lines = last_occurrence.len();

    // Build set of indices to keep
    let kept_indices: HashSet<usize> = last_occurrence.values().map(|(idx, _)| *idx).collect();

    // Second pass: emit only last occurrences in order
    for (idx, line) in lines.iter().enumerate() {
        if kept_indices.contains(&idx) {
            if options.count {
                let key_line = if line.ends_with(b"\n") {
                    if line.ends_with(b"\r\n") {
                        &line[..line.len() - 2]
                    } else {
                        &line[..line.len() - 1]
                    }
                } else {
                    &line[..]
                };

                let key = make_key(key_line, options)?;
                let count = lines
                    .iter()
                    .filter(|l| {
                        let l_key_line = if l.ends_with(b"\n") {
                            if l.ends_with(b"\r\n") {
                                &l[..l.len() - 2]
                            } else {
                                &l[..l.len() - 1]
                            }
                        } else {
                            &l[..]
                        };
                        make_key(l_key_line, options).ok() == Some(key.clone())
                    })
                    .count();
                write!(output, "{:>7} ", count)?;
            }
            output.write_all(line)?;
            stats.lines_written += 1;
        } else {
            stats.lines_removed += 1;
            if options.show_removed {
                write!(output, "[REMOVED] ")?;
                output.write_all(line)?;
            }
        }
    }

    Ok(stats)
}

/// Two-pass remove-all algorithm
fn deduplicate_remove_all<R: std::io::Read, W: Write>(
    input: R,
    output: &mut W,
    options: &DeduplicationOptions,
) -> Result<DeduplicationStats> {
    let mut reader = BufReader::new(input);
    let mut stats = DeduplicationStats::default();

    #[cfg(feature = "fast-hash")]
    type MapType = AHashMap<Vec<u8>, usize>;

    #[cfg(not(feature = "fast-hash"))]
    type MapType = HashMap<Vec<u8>, usize>;

    let mut counts: MapType = MapType::default();
    let mut lines = Vec::new();

    // First pass: count all occurrences
    let mut line = Vec::new();
    while reader.read_until(b'\n', &mut line)? > 0 {
        stats.lines_read += 1;

        let key_line = if line.ends_with(b"\n") {
            if line.ends_with(b"\r\n") {
                &line[..line.len() - 2]
            } else {
                &line[..line.len() - 1]
            }
        } else {
            &line[..]
        };

        let key = make_key(key_line, options)?;
        *counts.entry(key).or_insert(0) += 1;
        lines.push(line.clone());
        line.clear();
    }

    // Count unique lines (those appearing exactly once)
    stats.unique_lines = counts.values().filter(|&&c| c == 1).count();

    // Second pass: emit only lines that appear exactly once
    for line in lines {
        let key_line = if line.ends_with(b"\n") {
            if line.ends_with(b"\r\n") {
                &line[..line.len() - 2]
            } else {
                &line[..line.len() - 1]
            }
        } else {
            &line[..]
        };

        let key = make_key(key_line, options)?;
        let count = counts.get(&key).copied().unwrap_or(0);

        if count == 1 {
            if options.count {
                write!(output, "{:>7} ", count)?;
            }
            output.write_all(&line)?;
            stats.lines_written += 1;
        } else {
            stats.lines_removed += 1;
            if options.show_removed {
                write!(output, "[REMOVED] ")?;
                output.write_all(&line)?;
            }
        }
    }

    Ok(stats)
}

/// Create deduplication key from line
fn make_key(line: &[u8], options: &DeduplicationOptions) -> Result<Vec<u8>> {
    let data = if let Some(col_idx) = options.column {
        // Extract column (1-indexed) using whitespace splitting
        // This handles standard whitespace separation more robustly than manual byte checks
        let text = String::from_utf8_lossy(line);
        let cols: Vec<&str> = text.split_whitespace().collect();

        if col_idx > 0 && col_idx <= cols.len() {
            // We need to return an owned Vec<u8> because text is temporary
            cols[col_idx - 1].as_bytes().to_vec()
        } else {
            line.to_vec()
        }
    } else {
        line.to_vec()
    };

    if options.ignore_case {
        // Try to convert to lowercase UTF-8
        match std::str::from_utf8(&data) {
            Ok(s) => Ok(s.to_lowercase().into_bytes()),
            Err(_) => Ok(data),
        }
    } else {
        Ok(data)
    }
}

/// Disk-backed keep-first algorithm using sled
#[cfg(feature = "disk-backed")]
fn deduplicate_keep_first_disk<R: std::io::Read, W: Write>(
    input: R,
    output: &mut W,
    options: &DeduplicationOptions,
) -> Result<DeduplicationStats> {
    use sled::Db;

    let mut reader = BufReader::new(input);
    let mut stats = DeduplicationStats::default();

    // Create temporary sled database
    let db: Db = sled::Config::new()
        .temporary(true)
        .open()
        .map_err(|e| Error::InvalidArgument(format!("Failed to create temp database: {}", e)))?;

    let mut lines_for_count = Vec::new();

    let mut line = Vec::new();
    while reader.read_until(b'\n', &mut line)? > 0 {
        stats.lines_read += 1;

        // Strip newline for key generation but keep for output
        let key_line = if line.ends_with(b"\n") {
            if line.ends_with(b"\r\n") {
                &line[..line.len() - 2]
            } else {
                &line[..line.len() - 1]
            }
        } else {
            &line[..]
        };

        let key = make_key(key_line, options)?;

        // Check if we've seen this key before
        let count = if let Some(existing) = db
            .get(&key)
            .map_err(|e| Error::InvalidArgument(format!("Database error: {}", e)))?
        {
            let mut count_bytes = [0u8; 8];
            count_bytes.copy_from_slice(&existing);
            u64::from_le_bytes(count_bytes) + 1
        } else {
            1
        };

        // Store the count
        db.insert(&key, &count.to_le_bytes())
            .map_err(|e| Error::InvalidArgument(format!("Database error: {}", e)))?;

        if count == 1 {
            if options.count {
                lines_for_count.push(line.clone());
            } else {
                output.write_all(&line)?;
            }
            stats.lines_written += 1;
        } else {
            stats.lines_removed += 1;
            if options.show_removed {
                write!(output, "[REMOVED] ")?;
                output.write_all(&line)?;
            }
        }
        line.clear();
    }

    stats.unique_lines = db.len();

    // Write counts if requested
    if options.count {
        for line in lines_for_count {
            let key = make_key(&line, options)?;
            if let Some(count_bytes) = db
                .get(&key)
                .map_err(|e| Error::InvalidArgument(format!("Database error: {}", e)))?
            {
                let mut bytes = [0u8; 8];
                bytes.copy_from_slice(&count_bytes);
                let cnt = u64::from_le_bytes(bytes);
                write!(output, "{:>7} ", cnt)?;
                output.write_all(&line)?;
            }
        }
    }

    Ok(stats)
}

/// Disk-backed keep-last algorithm using sled (two-pass)
#[cfg(feature = "disk-backed")]
fn deduplicate_keep_last_disk<R: std::io::Read + std::io::Seek, W: Write>(
    mut input: R,
    output: &mut W,
    options: &DeduplicationOptions,
) -> Result<DeduplicationStats> {
    use sled::Db;

    let mut stats = DeduplicationStats::default();

    // Create temporary sled database
    let db: Db = sled::Config::new()
        .temporary(true)
        .open()
        .map_err(|e| Error::InvalidArgument(format!("Failed to create temp database: {}", e)))?;

    // Pass 1: Track last occurrence index for each key
    let mut reader = BufReader::new(&mut input);
    let mut line = Vec::new();
    for (line_index, _) in (0..).enumerate() {
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }
        stats.lines_read += 1;

        let key_line = if line.ends_with(b"\n") {
            if line.ends_with(b"\r\n") {
                &line[..line.len() - 2]
            } else {
                &line[..line.len() - 1]
            }
        } else {
            &line[..]
        };

        let key = make_key(key_line, options)?;

        // Retrieve existing data to update count
        let count = if let Some(existing) = db
            .get(&key)
            .map_err(|e| Error::InvalidArgument(format!("Database error: {}", e)))?
        {
            // Existing value is 16 bytes: [last_index (8) | count (8)]
            // Or if we need to migrate/handle unexpected sizes, we can check len.
            // Since we are creating a temp DB from scratch, we control the layout.
            if existing.len() == 16 {
                let mut count_bytes = [0u8; 8];
                count_bytes.copy_from_slice(&existing[8..16]);
                u64::from_le_bytes(count_bytes) + 1
            } else {
                1
            }
        } else {
            1
        };

        // Store: line_index (8 bytes) + count (8 bytes)
        let mut value = [0u8; 16];
        value[0..8].copy_from_slice(&(line_index as u64).to_le_bytes());
        value[8..16].copy_from_slice(&count.to_le_bytes());

        db.insert(&key, &value)
            .map_err(|e| Error::InvalidArgument(format!("Database error: {}", e)))?;

        line.clear();
    }

    stats.unique_lines = db.len();

    // Pass 2: Re-read file and output only last occurrences
    input.seek(std::io::SeekFrom::Start(0))?;
    let mut reader = BufReader::new(&mut input);
    let mut line = Vec::new();

    for (current_index, _) in (0..).enumerate() {
        if reader.read_until(b'\n', &mut line)? == 0 {
            break;
        }

        let key_line = if line.ends_with(b"\n") {
            if line.ends_with(b"\r\n") {
                &line[..line.len() - 2]
            } else {
                &line[..line.len() - 1]
            }
        } else {
            &line[..]
        };

        let key = make_key(key_line, options)?;

        if let Some(last_index_bytes) = db
            .get(&key)
            .map_err(|e| Error::InvalidArgument(format!("Database error: {}", e)))?
        {
            // Value is 16 bytes: [last_index (8) | count (8)]
            if last_index_bytes.len() == 16 {
                let mut index_bytes = [0u8; 8];
                index_bytes.copy_from_slice(&last_index_bytes[0..8]);
                let last_index = u64::from_le_bytes(index_bytes);

                if (current_index as u64) == last_index {
                    if options.count {
                        let mut count_bytes = [0u8; 8];
                        count_bytes.copy_from_slice(&last_index_bytes[8..16]);
                        let count = u64::from_le_bytes(count_bytes);
                        write!(output, "{:>7} ", count)?;
                    }
                    output.write_all(&line)?;
                    stats.lines_written += 1;
                } else {
                    stats.lines_removed += 1;
                    if options.show_removed {
                        write!(output, "[REMOVED] ")?;
                        output.write_all(&line)?;
                    }
                }
            } else {
                // Fallback for unexpected data format (should not happen with new logic)
                // Just assume it's index only logic from before? No, let's treat as error or safe fallback using old logic if length is 8.
                // For now, ignoring to keep simple.
            }
        }
        line.clear();
    }

    Ok(stats)
}

/// Disk-backed remove-all algorithm using sled (two-pass)
#[cfg(feature = "disk-backed")]
fn deduplicate_remove_all_disk<R: std::io::Read + std::io::Seek, W: Write>(
    mut input: R,
    output: &mut W,
    options: &DeduplicationOptions,
) -> Result<DeduplicationStats> {
    use sled::Db;

    let mut stats = DeduplicationStats::default();

    // Create temporary sled database
    let db: Db = sled::Config::new()
        .temporary(true)
        .open()
        .map_err(|e| Error::InvalidArgument(format!("Failed to create temp database: {}", e)))?;

    // Pass 1: Count occurrences of each key
    let mut reader = BufReader::new(&mut input);
    let mut line = Vec::new();

    while reader.read_until(b'\n', &mut line)? > 0 {
        stats.lines_read += 1;

        let key_line = if line.ends_with(b"\n") {
            if line.ends_with(b"\r\n") {
                &line[..line.len() - 2]
            } else {
                &line[..line.len() - 1]
            }
        } else {
            &line[..]
        };

        let key = make_key(key_line, options)?;

        // Get current count and increment
        let count = if let Some(existing) = db
            .get(&key)
            .map_err(|e| Error::InvalidArgument(format!("Database error: {}", e)))?
        {
            let mut count_bytes = [0u8; 8];
            count_bytes.copy_from_slice(&existing);
            u64::from_le_bytes(count_bytes) + 1
        } else {
            1
        };

        db.insert(&key, &count.to_le_bytes())
            .map_err(|e| Error::InvalidArgument(format!("Database error: {}", e)))?;
        line.clear();
    }

    // Count unique lines (those appearing exactly once)
    for item in db.iter() {
        let (_, count_bytes) =
            item.map_err(|e| Error::InvalidArgument(format!("Database error: {}", e)))?;
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&count_bytes);
        let count = u64::from_le_bytes(bytes);
        if count == 1 {
            stats.unique_lines += 1;
        }
    }

    // Pass 2: Re-read file and output only lines that appear exactly once
    input.seek(std::io::SeekFrom::Start(0))?;
    let mut reader = BufReader::new(&mut input);
    let mut line = Vec::new();

    while reader.read_until(b'\n', &mut line)? > 0 {
        let key_line = if line.ends_with(b"\n") {
            if line.ends_with(b"\r\n") {
                &line[..line.len() - 2]
            } else {
                &line[..line.len() - 1]
            }
        } else {
            &line[..]
        };

        let key = make_key(key_line, options)?;

        if let Some(count_bytes) = db
            .get(&key)
            .map_err(|e| Error::InvalidArgument(format!("Database error: {}", e)))?
        {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&count_bytes);
            let count = u64::from_le_bytes(bytes);

            if count == 1 {
                if options.count {
                    write!(output, "{:>7} ", count)?;
                }
                output.write_all(&line)?;
                stats.lines_written += 1;
            } else {
                stats.lines_removed += 1;
                if options.show_removed {
                    write!(output, "[REMOVED] ")?;
                    output.write_all(&line)?;
                }
            }
        }
        line.clear();
    }

    Ok(stats)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_keep_first_basic() {
        let input = b"a\nb\na\nc\n";
        let mut output = Vec::new();

        let opts = DeduplicationOptions::default();
        let stats = deduplicate(Cursor::new(input), &mut output, &opts).unwrap();

        assert_eq!(output, b"a\nb\nc\n");
        assert_eq!(stats.lines_read, 4);
        assert_eq!(stats.lines_written, 3);
        assert_eq!(stats.lines_removed, 1);
    }

    #[test]
    fn test_ignore_case() {
        let input = b"Apple\napple\nBanana\n";
        let mut output = Vec::new();

        let opts = DeduplicationOptions {
            ignore_case: true,
            ..Default::default()
        };
        let stats = deduplicate(Cursor::new(input), &mut output, &opts).unwrap();

        assert_eq!(output, b"Apple\nBanana\n");
        assert_eq!(stats.unique_lines, 2);
    }

    #[test]
    fn test_keep_last() {
        let input = b"a\nb\na\nc\n";
        let mut output = Vec::new();

        let opts = DeduplicationOptions {
            mode: DeduplicationMode::KeepLast,
            ..Default::default()
        };
        let stats = deduplicate(Cursor::new(input), &mut output, &opts).unwrap();

        assert_eq!(output, b"b\na\nc\n");
        assert_eq!(stats.lines_written, 3);
    }

    #[test]
    fn test_remove_all() {
        let input = b"a\nb\na\nc\n";
        let mut output = Vec::new();

        let opts = DeduplicationOptions {
            mode: DeduplicationMode::RemoveAll,
            ..Default::default()
        };
        let stats = deduplicate(Cursor::new(input), &mut output, &opts).unwrap();

        assert_eq!(output, b"b\nc\n");
        assert_eq!(stats.unique_lines, 2);
    }

    #[test]
    fn test_empty_input() {
        let input = b"";
        let mut output = Vec::new();

        let opts = DeduplicationOptions::default();
        let stats = deduplicate(Cursor::new(input), &mut output, &opts).unwrap();

        assert_eq!(stats.lines_read, 0);
        assert_eq!(stats.lines_written, 0);
    }

    #[test]
    fn test_non_utf8() {
        let input = vec![0xFF, 0xFE, b'\n', 0xFF, 0xFE, b'\n', b'a', b'\n'];
        let mut output = Vec::new();

        let opts = DeduplicationOptions::default();
        let stats = deduplicate(Cursor::new(&input), &mut output, &opts).unwrap();

        assert_eq!(stats.lines_written, 2);
    }

    #[cfg(feature = "disk-backed")]
    #[test]
    fn test_disk_backed_keep_first() {
        use std::io::Cursor;

        let input = b"a\nb\na\nc\n";
        let mut output = Vec::new();

        let opts = DeduplicationOptions {
            use_disk: true,
            ..Default::default()
        };
        let stats = deduplicate(Cursor::new(input), &mut output, &opts).unwrap();

        assert_eq!(output, b"a\nb\nc\n");
        assert_eq!(stats.lines_written, 3);
        assert_eq!(stats.unique_lines, 3);
    }

    #[cfg(feature = "disk-backed")]
    #[test]
    fn test_disk_backed_keep_last() {
        use std::io::Cursor;

        let input = b"a\nb\na\nc\n";
        let mut cursor = Cursor::new(input);
        let mut output = Vec::new();

        let opts = DeduplicationOptions {
            mode: DeduplicationMode::KeepLast,
            use_disk: true,
            ..Default::default()
        };
        let stats = deduplicate_seekable(&mut cursor, &mut output, &opts).unwrap();

        assert_eq!(output, b"b\na\nc\n");
        assert_eq!(stats.lines_written, 3);
    }

    #[cfg(feature = "disk-backed")]
    #[test]
    fn test_disk_backed_remove_all() {
        use std::io::Cursor;

        let input = b"a\nb\na\nc\n";
        let mut cursor = Cursor::new(input);
        let mut output = Vec::new();

        let opts = DeduplicationOptions {
            mode: DeduplicationMode::RemoveAll,
            use_disk: true,
            ..Default::default()
        };
        let stats = deduplicate_seekable(&mut cursor, &mut output, &opts).unwrap();

        assert_eq!(output, b"b\nc\n");
        assert_eq!(stats.unique_lines, 2);
    }
}
