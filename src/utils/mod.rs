/// Extract filename from a URL's path segment.
pub fn filename_from_url(url: &str) -> String {
    url.split('/')
        .last()
        .filter(|s| !s.is_empty())
        .unwrap_or("downloaded_file")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_filename() {
        assert_eq!(filename_from_url("https://example.com/file.zip"), "file.zip");
        assert_eq!(filename_from_url("https://example.com/"), "downloaded_file");
    }
}