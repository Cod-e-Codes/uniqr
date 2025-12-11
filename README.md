# uniqr

A fast line deduplication tool that preserves order, written in Rust.

## Features

- **Order-Preserving**: Unlike `sort | uniq`, `uniqr` maintains the original order of lines
- **Multiple Modes**:
  - `KeepFirst`: Keep the first occurrence of each line (default)
  - `KeepLast`: Keep the last occurrence of each line
  - `RemoveAll`: Remove all lines that appear more than once
- **Case-Insensitive Matching**: Optional case-insensitive deduplication
- **Column-Based Deduplication**: Deduplicate based on specific columns
- **Statistics**: View deduplication statistics
- **Fast Hashing**: Optional `ahash` support for improved performance
- **Disk-Backed Storage**: Handle massive files that don't fit in RAM using `sled` embedded database

## Installation

```bash
cargo install --path .
```

## Usage

### Basic Usage

```bash
# Deduplicate from stdin
echo -e "line1\nline2\nline1\nline3" | uniqr

# Deduplicate a file
uniqr input.txt

# Write to output file
uniqr input.txt -o output.txt
```

### Advanced Options

```bash
# Keep last occurrence instead of first
uniqr --keep-last input.txt

# Remove all duplicate lines (keep only unique)
uniqr --remove-all input.txt

# Case-insensitive deduplication
uniqr --ignore-case input.txt

# Show occurrence counts
uniqr --count input.txt

# Show removed lines
uniqr --show-removed input.txt

# View statistics
uniqr --stats input.txt

# Deduplicate by column (1-indexed)
uniqr --column 1 data.tsv

# Dry run (don't write output)
uniqr --dry-run --stats input.txt

# Use disk-backed storage for massive files (requires 'disk-backed' feature)
uniqr --use-disk huge_file.txt
```

## Library Usage

```rust
use uniqr::{deduplicate, DeduplicationMode, DeduplicationOptions};
use std::io::Cursor;

let input = b"line1\nline2\nline1\nline3\n";
let mut output = Vec::new();

let options = DeduplicationOptions {
    mode: DeduplicationMode::KeepFirst,
    ignore_case: false,
    count: false,
    show_removed: false,
    column: None,
    use_disk: false,  // Set to true for disk-backed storage
};

deduplicate(Cursor::new(input), &mut output, &options).unwrap();
assert_eq!(output, b"line1\nline2\nline3\n");
```

> **Note**: For disk-backed `KeepLast` and `RemoveAll` modes, use `deduplicate_seekable` instead of `deduplicate` as these modes require a seekable input source.

## Why uniqr?

The standard Unix `uniq` command only removes **adjacent** duplicates. To remove all duplicates, you must use `sort | uniq`, which destroys the original order of lines.

`uniqr` solves this by using a HashMap-based approach to track seen lines globally while preserving the original input order. The time complexity is **O(N)** for reading the input, where $N$ is the number of lines. The `KeepLast` and `RemoveAll` modes are **two-pass**, resulting in $O(2N)$ time complexity, which is still linear time $O(N)$.

### Use Cases

- Cleaning log files while maintaining chronological order
- Deduplicating `.bash_history` or command lists
- Processing data streams where order matters
- Finding unique entries in unsorted data

## Performance

- **One-pass algorithms**: `KeepFirst` mode processes the file in a single pass
- **Fast hashing**: Uses `ahash` by default for improved performance
- **Memory Usage**: In the default (non-disk-backed) mode, all deduplication algorithms have **linear memory complexity $O(U)$**, where $U$ is the number of **unique line keys**, not the total number of lines. The two-pass modes (`KeepLast`, `RemoveAll`) also buffer the file contents in memory, limiting them to files smaller than available RAM.
- **Disk-backed mode**: Uses `sled` embedded database to handle files larger than available RAM

### Scaling to Large Files

For files that don't fit in RAM, use the `--use-disk` flag (requires building with `disk-backed` feature):

```bash
# Build with disk-backed support
cargo build --release --features disk-backed

# Example: Using KeepFirst on a massive file (one-pass, works with stdin or file)
./target/release/uniqr --use-disk huge_file.log

# Process with KeepLast (two-pass, requires file input)
./target/release/uniqr --use-disk --keep-last huge_file.log

# Process with RemoveAll (two-pass, requires file input)
./target/release/uniqr --use-disk --remove-all huge_file.log
```

**Disk-backed mode** trades speed for memory efficiency, storing seen keys in a temporary `sled` database instead of RAM:

- **KeepFirst**: One-pass algorithm, works with stdin or files
- **KeepLast**: Two-pass algorithm, requires file input (needs seeking)
- **RemoveAll**: Two-pass algorithm, requires file input (needs seeking)

> **Note**: `--keep-last` and `--remove-all` with `--use-disk` require a file as input (not stdin) because they need to read the file twice.

## Building

```bash
# Debug build
cargo build

# Release build (optimized)
cargo build --release

# Run tests
cargo test

# Build with disk-backed storage support
cargo build --release --features disk-backed

# Build with all features
cargo build --release --all-features

# Run with custom features
cargo build --no-default-features
```

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.
