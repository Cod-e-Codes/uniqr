use clap::Parser;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use uniqr::{DeduplicationMode, DeduplicationOptions, Error, deduplicate};

/// Deduplication mode arguments (mutually exclusive)
#[derive(clap::Args, Debug, Default, Clone, Copy)]
#[group(required = false, multiple = false)]
struct ModeArgs {
    /// Keep last occurrence instead of first (two-pass)
    #[arg(long)]
    keep_last: bool,

    /// Remove all lines that appear more than once (two-pass)
    #[arg(long)]
    remove_all: bool,
}

/// A fast line deduplication tool that preserves order
#[derive(Parser, Debug)]
#[command(name = "uniqr")]
#[command(version = "0.1.0")]
#[command(about = "Remove duplicate lines while preserving order", long_about = None)]
struct Cli {
    /// Input file (uses stdin if not provided)
    #[arg(value_name = "FILE")]
    input: Option<PathBuf>,

    /// Output file (uses stdout if not provided)
    #[arg(short, long, value_name = "FILE")]
    output: Option<PathBuf>,

    /// Prefix lines with occurrence count
    #[arg(short, long)]
    count: bool,

    /// Ignore case when comparing lines
    #[arg(short = 'i', long)]
    ignore_case: bool,

    /// Deduplication mode
    #[command(flatten)]
    mode: ModeArgs,

    /// Show removed duplicate lines with [REMOVED] prefix
    #[arg(long)]
    show_removed: bool,

    /// Show deduplication statistics
    #[arg(long)]
    stats: bool,

    /// Preview changes without writing output
    #[arg(long)]
    dry_run: bool,

    /// Deduplicate by specific column (1-indexed, whitespace-separated)
    #[arg(long, value_name = "N")]
    column: Option<usize>,

    /// Use disk-backed storage for massive files (requires 'disk-backed' feature)
    #[cfg(feature = "disk-backed")]
    #[arg(long)]
    use_disk: bool,
}

fn main() {
    if let Err(e) = run() {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

fn run() -> Result<(), Error> {
    let cli = Cli::parse();

    // Determine deduplication mode (clap ensures only one is set)
    let mode = if cli.mode.keep_last {
        DeduplicationMode::KeepLast
    } else if cli.mode.remove_all {
        DeduplicationMode::RemoveAll
    } else {
        DeduplicationMode::KeepFirst
    };

    let options = DeduplicationOptions {
        mode,
        ignore_case: cli.ignore_case,
        count: cli.count,
        show_removed: cli.show_removed,
        column: cli.column,
        #[cfg(feature = "disk-backed")]
        use_disk: cli.use_disk,
        #[cfg(not(feature = "disk-backed"))]
        use_disk: false,
    };

    // Validate disk-backed modes that require seeking
    #[cfg(feature = "disk-backed")]
    if options.use_disk
        && (mode == DeduplicationMode::KeepLast || mode == DeduplicationMode::RemoveAll)
        && cli.input.is_none()
    {
        return Err(Error::InvalidArgument(
            "Disk-backed --keep-last and --remove-all require a file input (not stdin)".to_string(),
        ));
    }

    // Open input and perform deduplication with appropriate trait bounds
    let stats = if let Some(path) = cli.input {
        // File input is seekable
        let file = File::open(&path).map_err(|e| {
            Error::Io(io::Error::new(
                e.kind(),
                format!("Failed to open input file '{}': {}", path.display(), e),
            ))
        })?;

        // Prepare output
        if cli.dry_run {
            let mut null_output = io::sink();
            uniqr::deduplicate_seekable(file, &mut null_output, &options)?
        } else if let Some(output_path) = cli.output {
            // Atomic file write setup
            let temp_path = output_path.with_extension("tmp");
            let temp_file = File::create(&temp_path).map_err(|e| {
                Error::Io(io::Error::new(
                    e.kind(),
                    format!(
                        "Failed to create temp file '{}': {}",
                        temp_path.display(),
                        e
                    ),
                ))
            })?;
            let mut writer = BufWriter::new(temp_file);

            let stats = uniqr::deduplicate_seekable(file, &mut writer, &options)?;

            writer.flush()?;
            drop(writer);
            std::fs::rename(&temp_path, &output_path).map_err(|e| {
                Error::Io(io::Error::new(
                    e.kind(),
                    format!(
                        "Failed to rename '{}' to '{}': {}",
                        temp_path.display(),
                        output_path.display(),
                        e
                    ),
                ))
            })?;
            stats
        } else {
            // Write to stdout
            let stdout = io::stdout();
            let mut writer = BufWriter::new(stdout.lock());
            let stats = uniqr::deduplicate_seekable(file, &mut writer, &options)?;
            writer.flush()?;
            stats
        }
    } else {
        // Stdin input (not seekable via standard Stdin handle)
        let stdin = io::stdin();
        let input = stdin.lock(); // StdinLock implements Read

        // Prepare output
        if cli.dry_run {
            let mut null_output = io::sink();
            deduplicate(input, &mut null_output, &options)?
        } else if let Some(output_path) = cli.output {
            // Atomic file write setup for Stdin input
            let temp_path = output_path.with_extension("tmp");
            let temp_file = File::create(&temp_path).map_err(|e| {
                Error::Io(io::Error::new(
                    e.kind(),
                    format!(
                        "Failed to create temp file '{}': {}",
                        temp_path.display(),
                        e
                    ),
                ))
            })?;
            let mut writer = BufWriter::new(temp_file);

            let stats = deduplicate(input, &mut writer, &options)?;

            writer.flush()?;
            drop(writer);
            std::fs::rename(&temp_path, &output_path).map_err(|e| {
                Error::Io(io::Error::new(
                    e.kind(),
                    format!(
                        "Failed to rename '{}' to '{}': {}",
                        temp_path.display(),
                        output_path.display(),
                        e
                    ),
                ))
            })?;
            stats
        } else {
            // Write to stdout
            let stdout = io::stdout();
            let mut writer = BufWriter::new(stdout.lock());
            let stats = deduplicate(input, &mut writer, &options)?;
            writer.flush()?;
            stats
        }
    };

    // Print statistics if requested
    if cli.stats {
        eprintln!("Statistics:");
        eprintln!("  Lines read:    {}", stats.lines_read);
        eprintln!("  Lines written: {}", stats.lines_written);
        eprintln!("  Lines removed: {}", stats.lines_removed);
        eprintln!("  Unique lines:  {}", stats.unique_lines);
    }

    Ok(())
}
