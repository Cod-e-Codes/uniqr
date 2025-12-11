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
