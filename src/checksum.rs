use sha2::{Sha256, Digest};
use std::path::Path;

//compute SHA256 hex digest of a file's contents
pub fn hash_file(path: &Path) -> anyhow::Result<String> {
    let content = std::fs::read(path)
        .map_err(|e| anyhow::anyhow!("cannot read {}: {}", path.display(), e))?;
    Ok(hash_bytes(&content))
}

//compute SHA256 hex digest of raw bytes
fn hash_bytes(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_hash_file_deterministic() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"hello world").unwrap();

        let h1 = hash_file(f.path()).unwrap();
        let h2 = hash_file(f.path()).unwrap();
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64); //SHA256 hex = 64 chars
    }

    #[test]
    fn test_hash_file_changes_on_content_change() {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(b"version 1").unwrap();
        let h1 = hash_file(f.path()).unwrap();

        std::fs::write(f.path(), b"version 2").unwrap();
        let h2 = hash_file(f.path()).unwrap();

        assert_ne!(h1, h2);
    }

    #[test]
    fn test_hash_file_nonexistent() {
        let result = hash_file(Path::new("/tmp/nonexistent_pulsar_test_file.md"));
        assert!(result.is_err());
    }
}
