use indicatif::{ProgressBar, ProgressStyle};

pub fn build_progress_bar(total_size: Option<u64>) -> ProgressBar {
    match total_size {
        Some(size) => {
            let bar = ProgressBar::new(size);
            bar.set_style(
                ProgressStyle::default_bar()
                    .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
                    .unwrap()
                    .progress_chars("#>-"),
            );
            bar
        }
        None => {
            let bar = ProgressBar::new_spinner();
            bar.set_style(
                ProgressStyle::default_spinner()
                    .template("{spinner:.green} [{elapsed_precise}] {bytes} downloaded ({bytes_per_sec})")
                    .unwrap(),
            );
            bar
        }
    }
}