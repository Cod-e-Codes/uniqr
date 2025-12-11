use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::NamedTempFile;

#[test]
fn test_basic_dedup_stdin() {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("uniqr"));
    cmd.write_stdin("line1\nline2\nline1\nline3\n")
        .assert()
        .success()
        .stdout("line1\nline2\nline3\n");
}

#[test]
fn test_keep_last_mode() {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("uniqr"));
    cmd.arg("--keep-last")
        .write_stdin("a\nb\na\nc\n")
        .assert()
        .success()
        .stdout("b\na\nc\n");
}

#[test]
fn test_remove_all_mode() {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("uniqr"));
    cmd.arg("--remove-all")
        .write_stdin("a\nb\na\nc\n")
        .assert()
        .success()
        .stdout("b\nc\n");
}

#[test]
fn test_ignore_case() {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("uniqr"));
    cmd.arg("--ignore-case")
        .write_stdin("Apple\napple\nBanana\n")
        .assert()
        .success()
        .stdout("Apple\nBanana\n");
}

#[test]
fn test_count_flag() {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("uniqr"));
    cmd.arg("--count")
        .write_stdin("a\nb\na\na\n")
        .assert()
        .success()
        .stdout(predicate::str::contains("3"))
        .stdout(predicate::str::contains("1"));
}

#[test]
fn test_stats_flag() {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("uniqr"));
    cmd.arg("--stats")
        .write_stdin("a\nb\na\n")
        .assert()
        .success()
        .stderr(predicate::str::contains("Lines read:    3"))
        .stderr(predicate::str::contains("Lines written: 2"));
}

#[test]
fn test_file_input() {
    let file = NamedTempFile::new().unwrap();
    fs::write(file.path(), "x\ny\nx\n").unwrap();

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("uniqr"));
    cmd.arg(file.path()).assert().success().stdout("x\ny\n");
}

#[test]
fn test_file_output() {
    let output_file = NamedTempFile::new().unwrap();

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("uniqr"));
    cmd.arg("--output")
        .arg(output_file.path())
        .write_stdin("1\n2\n1\n")
        .assert()
        .success();

    let contents = fs::read_to_string(output_file.path()).unwrap();
    assert_eq!(contents, "1\n2\n");
}

#[test]
fn test_dry_run() {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("uniqr"));
    cmd.arg("--dry-run")
        .arg("--stats")
        .write_stdin("a\nb\na\n")
        .assert()
        .success()
        .stdout("")
        .stderr(predicate::str::contains("Lines written: 2"));
}

#[test]
fn test_conflicting_modes() {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("uniqr"));
    cmd.arg("--keep-last")
        .arg("--remove-all")
        .write_stdin("a\n")
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be used with"));
}

#[test]
fn test_empty_input() {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("uniqr"));
    cmd.write_stdin("").assert().success().stdout("");
}

#[test]
fn test_show_removed() {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("uniqr"));
    cmd.arg("--show-removed")
        .write_stdin("a\nb\na\n")
        .assert()
        .success()
        .stdout(predicate::str::contains("[REMOVED] a"));
}

#[test]
fn test_single_line() {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("uniqr"));
    cmd.write_stdin("only\n")
        .assert()
        .success()
        .stdout("only\n");
}

#[test]
fn test_all_duplicates() {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("uniqr"));
    cmd.write_stdin("x\nx\nx\n")
        .assert()
        .success()
        .stdout("x\n");
}

#[test]
fn test_column_mode() {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("uniqr"));
    cmd.arg("--column")
        .arg("1")
        .write_stdin("1\tapple\n2\tbanana\n1\torange\n")
        .assert()
        .success()
        .stdout("1\tapple\n2\tbanana\n");
}

#[test]
fn test_keep_last_count() {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("uniqr"));
    cmd.arg("--keep-last")
        .arg("--count")
        .write_stdin("a\nb\na\n")
        .assert()
        .success()
        .stdout(predicate::str::contains("2 a")) // 'a' appears twice, kept last. count should be 2.
        .stdout(predicate::str::contains("1 b"));
}

#[test]
fn test_empty_line_preservation() {
    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("uniqr"));
    cmd.write_stdin("a\n\nb\n\na\n")
        .assert()
        .success()
        // Deduplicated output:
        // 1. "a" (seen)
        // 2. "" (seen)
        // 3. "b" (seen)
        // 4. "" (duplicate of 2, removed)
        // 5. "a" (duplicate of 1, removed)
        .stdout("a\n\nb\n");
}

#[test]
fn test_disk_backed_keep_last_count() {
    let input_file = NamedTempFile::new().unwrap();
    fs::write(input_file.path(), "a\nb\na\n").unwrap();

    let mut cmd = Command::new(assert_cmd::cargo::cargo_bin!("uniqr"));
    cmd.arg("--keep-last")
        .arg("--count")
        .arg("--use-disk")
        .arg(input_file.path())
        .assert()
        .success()
        .stdout(predicate::str::contains("2 a"))
        .stdout(predicate::str::contains("1 b"));
}
