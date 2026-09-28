//! Guards the hexagonal dependency rule: dependencies point inward only.
//!
//! domain ← ports ← app ← adapters ← main/config

use std::fs;
use std::path::{Path, PathBuf};

/// Infrastructure the core must never touch; adapters wrap it.
const INFRA: &[&str] = &[
    "crate::adapters",
    "crate::config",
    "aws_config::",
    "aws_sdk_",
    "axum::",
    "tokio_postgres::",
    "postgres_",
    "metrics_exporter_prometheus::",
    "config::",
];

#[test]
fn the_core_does_not_depend_on_outer_layers() {
    let rules: &[(&str, &[&str])] = &[
        ("src/domain", &["crate::ports", "crate::app", "tokio::"]),
        ("src/ports.rs", &["crate::app"]),
        ("src/app", &[]),
    ];

    let mut violations = Vec::new();
    for (path, forbidden) in rules {
        for file in rust_files(&Path::new(env!("CARGO_MANIFEST_DIR")).join(path)) {
            let source = fs::read_to_string(&file).unwrap();
            for (number, line) in source.lines().enumerate() {
                let code = line.trim_start();
                if code.starts_with("//") {
                    continue;
                }
                if let Some(bad) = INFRA
                    .iter()
                    .chain(forbidden.iter())
                    .find(|bad| code.contains(*bad))
                {
                    violations.push(format!("{}:{}: uses `{bad}`", file.display(), number + 1));
                }
            }
        }
    }
    assert!(
        violations.is_empty(),
        "dependency rule broken:\n{}",
        violations.join("\n")
    );
}

fn rust_files(path: &Path) -> Vec<PathBuf> {
    if path.is_file() {
        return vec![path.to_path_buf()];
    }
    fs::read_dir(path)
        .unwrap()
        .flat_map(|entry| rust_files(&entry.unwrap().path()))
        .collect()
}
