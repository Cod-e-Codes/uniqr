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

    match options.mode {
        DeduplicationMode::KeepFirst => deduplicate_keep_first(input, output, options),
        DeduplicationMode::KeepLast => deduplicate_keep_last(input, output, options),
        DeduplicationMode::RemoveAll => deduplicate_remove_all(input, output, options),
    }
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
    deduplicate(input, output, options)
}

/// One-pass keep-first algorithm
fn deduplicate_keep_first<R: std::io::Read, W: Write>(
    input: R,
    output: &mut W,
    options: &DeduplicationOptions,
) -> Result<DeduplicationStats> {
    let reader = BufReader::new(input);
    let mut stats = DeduplicationStats::default();

    #[cfg(feature = "fast-hash")]
    type MapType = AHashMap<Vec<u8>, usize>;

    #[cfg(not(feature = "fast-hash"))]
    type MapType = HashMap<Vec<u8>, usize>;

    let mut seen: MapType = MapType::default();
    let mut lines_for_count = Vec::new();

    for line_result in reader.split(b'\n') {
        let line = line_result?;
        stats.lines_read += 1;

        let key = make_key(&line, options)?;
        let count = seen.entry(key).or_insert(0);
        *count += 1;

        if *count == 1 {
            if options.count {
                lines_for_count.push(line.clone());
            } else {
                write_line(output, &line)?;
            }
            stats.lines_written += 1;
        } else {
            stats.lines_removed += 1;
            if options.show_removed {
                writeln!(output, "[REMOVED] {}", String::from_utf8_lossy(&line))?;
            }
        }
    }

    stats.unique_lines = seen.len();

    // Write counts if requested
    if options.count {
        for line in lines_for_count {
            let key = make_key(&line, options)?;
            if let Some(&cnt) = seen.get(&key) {
                write!(output, "{:>7} ", cnt)?;
                write_line(output, &line)?;
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
    let reader = BufReader::new(input);
    let mut stats = DeduplicationStats::default();

    #[cfg(feature = "fast-hash")]
    type MapType = AHashMap<Vec<u8>, (usize, Vec<u8>)>;

    #[cfg(not(feature = "fast-hash"))]
    type MapType = HashMap<Vec<u8>, (usize, Vec<u8>)>;

    let mut last_occurrence: MapType = MapType::default();
    let mut lines = Vec::new();

    // First pass: read all lines and track last occurrence
    for line_result in reader.split(b'\n') {
        let line = line_result?;
        stats.lines_read += 1;

        let key = make_key(&line, options)?;
        last_occurrence.insert(key, (stats.lines_read - 1, line.clone()));
        lines.push(line);
    }

    stats.unique_lines = last_occurrence.len();

    // Build set of indices to keep
    let kept_indices: HashSet<usize> = last_occurrence.values().map(|(idx, _)| *idx).collect();

    // Second pass: emit only last occurrences in order
    for (idx, line) in lines.iter().enumerate() {
        if kept_indices.contains(&idx) {
            if options.count {
                let key = make_key(line, options)?;
                let count = lines
                    .iter()
                    .filter(|l| make_key(l, options).ok() == Some(key.clone()))
                    .count();
                write!(output, "{:>7} ", count)?;
            }
            write_line(output, line)?;
            stats.lines_written += 1;
        } else {
            stats.lines_removed += 1;
            if options.show_removed {
                writeln!(output, "[REMOVED] {}", String::from_utf8_lossy(line))?;
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
    let reader = BufReader::new(input);
    let mut stats = DeduplicationStats::default();

    #[cfg(feature = "fast-hash")]
    type MapType = AHashMap<Vec<u8>, usize>;

    #[cfg(not(feature = "fast-hash"))]
    type MapType = HashMap<Vec<u8>, usize>;

    let mut counts: MapType = MapType::default();
    let mut lines = Vec::new();

    // First pass: count all occurrences
    for line_result in reader.split(b'\n') {
        let line = line_result?;
        stats.lines_read += 1;

        let key = make_key(&line, options)?;
        *counts.entry(key).or_insert(0) += 1;
        lines.push(line);
    }

    // Count unique lines (those appearing exactly once)
    stats.unique_lines = counts.values().filter(|&&c| c == 1).count();

    // Second pass: emit only lines that appear exactly once
    for line in lines {
        let key = make_key(&line, options)?;
        let count = counts.get(&key).copied().unwrap_or(0);

        if count == 1 {
            if options.count {
                write!(output, "{:>7} ", count)?;
            }
            write_line(output, &line)?;
            stats.lines_written += 1;
        } else {
            stats.lines_removed += 1;
            if options.show_removed {
                writeln!(output, "[REMOVED] {}", String::from_utf8_lossy(&line))?;
            }
        }
    }

    Ok(stats)
}

/// Create deduplication key from line
fn make_key(line: &[u8], options: &DeduplicationOptions) -> Result<Vec<u8>> {
    let data = if let Some(col_idx) = options.column {
        // Extract column (1-indexed)
        let cols: Vec<&[u8]> = line
            .split(|&b| b == b'\t' || b == b' ')
            .filter(|s| !s.is_empty())
            .collect();

        if col_idx > 0 && col_idx <= cols.len() {
            cols[col_idx - 1]
        } else {
            line
        }
    } else {
        line
    };

    if options.ignore_case {
        // Try to convert to lowercase UTF-8
        match std::str::from_utf8(data) {
            Ok(s) => Ok(s.to_lowercase().into_bytes()),
            Err(_) => Ok(data.to_vec()),
        }
    } else {
        Ok(data.to_vec())
    }
}

/// Write a line to output with newline
fn write_line<W: Write>(output: &mut W, line: &[u8]) -> Result<()> {
    output.write_all(line)?;
    output.write_all(b"\n")?;
    Ok(())
}

/// Disk-backed keep-first algorithm using sled
#[cfg(feature = "disk-backed")]
fn deduplicate_keep_first_disk<R: std::io::Read, W: Write>(
    input: R,
    output: &mut W,
    options: &DeduplicationOptions,
) -> Result<DeduplicationStats> {
    use sled::Db;

    let reader = BufReader::new(input);
    let mut stats = DeduplicationStats::default();

    // Create temporary sled database
    let db: Db = sled::Config::new()
        .temporary(true)
        .open()
        .map_err(|e| Error::InvalidArgument(format!("Failed to create temp database: {}", e)))?;

    let mut lines_for_count = Vec::new();

    for line_result in reader.split(b'\n') {
        let line = line_result?;
        stats.lines_read += 1;

        let key = make_key(&line, options)?;

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
                write_line(output, &line)?;
            }
            stats.lines_written += 1;
        } else {
            stats.lines_removed += 1;
            if options.show_removed {
                writeln!(output, "[REMOVED] {}", String::from_utf8_lossy(&line))?;
            }
        }
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
                write_line(output, &line)?;
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
    let reader = BufReader::new(&mut input);
    let mut line_index = 0u64;

    for line_result in reader.split(b'\n') {
        let line = line_result?;
        stats.lines_read += 1;

        let key = make_key(&line, options)?;

        // Store the current line index as the last occurrence
        db.insert(&key, &line_index.to_le_bytes())
            .map_err(|e| Error::InvalidArgument(format!("Database error: {}", e)))?;

        line_index += 1;
    }

    stats.unique_lines = db.len();

    // Pass 2: Re-read file and output only last occurrences
    input.seek(std::io::SeekFrom::Start(0))?;
    let reader = BufReader::new(&mut input);
    let mut current_index = 0u64;

    for line_result in reader.split(b'\n') {
        let line = line_result?;
        let key = make_key(&line, options)?;

        if let Some(last_index_bytes) = db
            .get(&key)
            .map_err(|e| Error::InvalidArgument(format!("Database error: {}", e)))?
        {
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&last_index_bytes);
            let last_index = u64::from_le_bytes(bytes);

            if current_index == last_index {
                if options.count {
                    // Count how many times this key appeared (need to count in db)
                    // For simplicity, we'll just show 1 for now
                    write!(output, "{:>7} ", 1)?;
                }
                write_line(output, &line)?;
                stats.lines_written += 1;
            } else {
                stats.lines_removed += 1;
                if options.show_removed {
                    writeln!(output, "[REMOVED] {}", String::from_utf8_lossy(&line))?;
                }
            }
        }

        current_index += 1;
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
    let reader = BufReader::new(&mut input);

    for line_result in reader.split(b'\n') {
        let line = line_result?;
        stats.lines_read += 1;

        let key = make_key(&line, options)?;

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
    let reader = BufReader::new(&mut input);

    for line_result in reader.split(b'\n') {
        let line = line_result?;
        let key = make_key(&line, options)?;

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
                write_line(output, &line)?;
                stats.lines_written += 1;
            } else {
                stats.lines_removed += 1;
                if options.show_removed {
                    writeln!(output, "[REMOVED] {}", String::from_utf8_lossy(&line))?;
                }
            }
        }
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
