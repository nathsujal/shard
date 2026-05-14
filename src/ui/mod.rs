use std::time::{Duration, Instant};
 
use anyhow::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Gauge, Paragraph, Row, Table, TableState},
    Frame, Terminal,
};
 
use crate::ipc::client::send_request;
use crate::ipc::message::{JobSummary, Request, Response};
 
// ── App state ─────────────────────────────────────────────────────────────────
 
/// Everything the TUI needs to render one frame.
struct App {
    /// Current list of jobs from the daemon.
    jobs: Vec<JobSummary>,
 
    /// Which row is selected in the table (ratatui needs this for highlighting).
    table_state: TableState,
 
    /// When we last polled the daemon.
    last_poll: Instant,
 
    /// How often to poll (1 second).
    poll_interval: Duration,
 
    /// Error message to show at the bottom (e.g. "daemon not running").
    error: Option<String>,
 
    /// Whether user pressed q/Ctrl+C to quit.
    should_quit: bool,
}
 
impl App {
    fn new() -> Self {
        let mut table_state = TableState::default();
        table_state.select(Some(0)); // start with first row selected
 
        Self {
            jobs: Vec::new(),
            table_state,
            last_poll: Instant::now() - Duration::from_secs(2), // poll immediately on start
            poll_interval: Duration::from_secs(1),
            error: None,
            should_quit: false,
        }
    }
 
    /// Move selection down one row.
    fn next_row(&mut self) {
        if self.jobs.is_empty() {
            return;
        }
        let i = match self.table_state.selected() {
            Some(i) => (i + 1) % self.jobs.len(),
            None => 0,
        };
        self.table_state.select(Some(i));
    }
 
    /// Move selection up one row.
    fn prev_row(&mut self) {
        if self.jobs.is_empty() {
            return;
        }
        let i = match self.table_state.selected() {
            Some(0) | None => self.jobs.len() - 1,
            Some(i) => i - 1,
        };
        self.table_state.select(Some(i));
    }
 
    /// Returns the currently selected job, if any.
    fn selected_job(&self) -> Option<&JobSummary> {
        self.table_state.selected().and_then(|i| self.jobs.get(i))
    }
 
    /// Poll daemon for fresh job list. Non-blocking — uses existing IPC client.
    async fn poll_daemon(&mut self) {
        match send_request(&Request::Status { all: false }).await {
            Ok(Response::JobList { jobs }) => {
                self.jobs = jobs;
                self.error = None;
                // Keep selection in bounds after list changes.
                if let Some(sel) = self.table_state.selected() {
                    if sel >= self.jobs.len() && !self.jobs.is_empty() {
                        self.table_state.select(Some(self.jobs.len() - 1));
                    }
                }
            }
            Ok(_) => {} // unexpected response type, ignore
            Err(e) => {
                self.error = Some(format!("Daemon unreachable: {e}"));
                self.jobs.clear();
            }
        }
        self.last_poll = Instant::now();
    }
 
    /// Send pause signal for selected job.
    async fn pause_selected(&self) {
        if let Some(job) = self.selected_job() {
            let _ = send_request(&Request::Pause { id: job.id }).await;
        }
    }
 
    /// Send cancel signal for selected job.
    async fn cancel_selected(&self) {
        if let Some(job) = self.selected_job() {
            let _ = send_request(&Request::Cancel { id: job.id }).await;
        }
    }
}
 
// ── Entry point ───────────────────────────────────────────────────────────────
 
/// Launch the TUI. Blocks until user quits.
pub async fn run_tui() -> Result<()> {
    // 1. Set terminal to raw mode — keystrokes go to us, not the shell.
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
 
    // 2. Enter alternate screen — like vim does; original terminal restored on exit.
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
 
    // 3. Build ratatui Terminal backed by crossterm.
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;
 
    let mut app = App::new();
    let result = run_loop(&mut terminal, &mut app).await;
 
    // ── Cleanup — ALWAYS restore terminal, even on error ─────────────────────
    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;
 
    result
}
 
// ── Main event loop ───────────────────────────────────────────────────────────
 
async fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    app: &mut App,
) -> Result<()> {
    loop {
        // Poll daemon if interval has elapsed.
        if app.last_poll.elapsed() >= app.poll_interval {
            app.poll_daemon().await;
        }
 
        // Draw the current frame.
        // `terminal.draw()` calls our closure with a `Frame`, clears the screen,
        // then flushes the new content — all in one atomic operation.
        terminal.draw(|frame| draw(frame, app))?;
 
        // Check for keyboard input without blocking (timeout = 250ms).
        // This keeps the UI responsive while still polling the daemon.
        if event::poll(Duration::from_millis(250))? {
            if let Event::Key(key) = event::read()? {
                match (key.code, key.modifiers) {
                    // Quit
                    (KeyCode::Char('q'), _) | (KeyCode::Char('c'), KeyModifiers::CONTROL) => {
                        app.should_quit = true;
                    }
 
                    // Navigation
                    (KeyCode::Down, _) | (KeyCode::Char('j'), _) => app.next_row(),
                    (KeyCode::Up, _) | (KeyCode::Char('k'), _) => app.prev_row(),
 
                    // Actions on selected job
                    (KeyCode::Char('p'), _) => {
                        app.pause_selected().await;
                        app.poll_daemon().await; // refresh immediately
                    }
                    (KeyCode::Char('x'), _) => {
                        app.cancel_selected().await;
                        app.poll_daemon().await;
                    }
 
                    // Force refresh
                    (KeyCode::Char('r'), _) => {
                        app.poll_daemon().await;
                    }
 
                    _ => {}
                }
            }
        }
 
        if app.should_quit {
            break;
        }
    }
 
    Ok(())
}
 
// ── Drawing ───────────────────────────────────────────────────────────────────
//
// `draw()` is called every frame (~4x/sec due to 250ms poll timeout).
// It splits the screen into zones using Layout, then renders widgets.
 
fn draw(frame: &mut Frame, app: &mut App) {
    let area = frame.size();
 
    // Split screen into 3 horizontal bands:
    //   [0] title bar  — 3 lines tall (fixed)
    //   [1] job table  — fills remaining space
    //   [2] status bar — 3 lines tall (fixed)
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // title
            Constraint::Min(0),    // table (takes all remaining)
            Constraint::Length(3), // status/help bar
        ])
        .split(area);
 
    draw_title(frame, chunks[0]);
    draw_table(frame, app, chunks[1]);
    draw_statusbar(frame, app, chunks[2]);
}
 
// ── Title bar ─────────────────────────────────────────────────────────────────
 
fn draw_title(frame: &mut Frame, area: Rect) {
    let title = Paragraph::new(Line::from(vec![
        Span::styled(
            " ⬡ shard ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            "download manager",
            Style::default().fg(Color::DarkGray),
        ),
    ]))
    .block(Block::default().borders(Borders::ALL));
 
    frame.render_widget(title, area);
}
 
// ── Job table ─────────────────────────────────────────────────────────────────
 
fn draw_table(frame: &mut Frame, app: &mut App, area: Rect) {
    // Column headers.
    let header = Row::new(vec![
        Cell::from("ID").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("URL").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("STATUS").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("PROGRESS").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("SIZE").style(Style::default().add_modifier(Modifier::BOLD)),
    ])
    .style(Style::default().fg(Color::Yellow))
    .height(1);
 
    // One row per job.
    let rows: Vec<Row> = app
        .jobs
        .iter()
        .map(|job| {
            // Truncate URL to fit column.
            let url_display = if job.url.len() > 45 {
                format!("{}…", &job.url[..44])
            } else {
                job.url.clone()
            };
 
            // Color-code status.
            let status_style = match job.status.as_str() {
                "Active" => Style::default().fg(Color::Green),
                "Pending" => Style::default().fg(Color::Yellow),
                "Paused" => Style::default().fg(Color::Magenta),
                "Done" => Style::default().fg(Color::Cyan),
                s if s.starts_with("Failed") => Style::default().fg(Color::Red),
                _ => Style::default(),
            };
 
            // Progress as "62.3%" string.
            let progress_str = format!("{:.1}%", job.progress_pct);
 
            // Human-readable size.
            let size_str = format_bytes(job.total_size);
 
            Row::new(vec![
                Cell::from(job.id.to_string()),
                Cell::from(url_display),
                Cell::from(job.status.clone()).style(status_style),
                Cell::from(progress_str),
                Cell::from(size_str),
            ])
            .height(1)
        })
        .collect();
 
    let empty_msg = if app.error.is_some() {
        "No connection to daemon"
    } else {
        "Queue is empty — add downloads with: shard add <url>"
    };
 
    // Build the Table widget.
    // Widths are proportional: ID tiny, URL wide, rest fixed.
    let table = Table::new(
        rows,
        [
            Constraint::Length(5),      // ID
            Constraint::Min(20),        // URL (expands)
            Constraint::Length(12),     // STATUS
            Constraint::Length(10),     // PROGRESS
            Constraint::Length(10),     // SIZE
        ],
    )
    .header(header)
    .block(
        Block::default()
            .borders(Borders::ALL)
            .title(" Downloads ")
            .title_style(Style::default().fg(Color::Cyan)),
    )
    .highlight_style(
        // Selected row gets a blue background.
        Style::default()
            .bg(Color::Blue)
            .add_modifier(Modifier::BOLD),
    )
    .highlight_symbol("▶ ");
 
    if app.jobs.is_empty() {
        // Show placeholder text when queue is empty.
        let placeholder = Paragraph::new(format!("\n  {empty_msg}"))
            .style(Style::default().fg(Color::DarkGray))
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title(" Downloads ")
                    .title_style(Style::default().fg(Color::Cyan)),
            );
        frame.render_widget(placeholder, area);
    } else {
        // `render_stateful_widget` — like render_widget but also takes &mut TableState
        // so ratatui knows which row to highlight.
        frame.render_stateful_widget(table, area, &mut app.table_state);
    }
}
 
// ── Status bar ────────────────────────────────────────────────────────────────
 
fn draw_statusbar(frame: &mut Frame, app: &App, area: Rect) {
    let content = if let Some(ref err) = app.error {
        // Show error in red if daemon unreachable.
        Line::from(vec![Span::styled(
            format!(" ✗ {err}"),
            Style::default().fg(Color::Red),
        )])
    } else {
        // Normal keybind help.
        Line::from(vec![
            Span::styled(" [↑↓/jk]", Style::default().fg(Color::Cyan)),
            Span::raw(" navigate  "),
            Span::styled("[p]", Style::default().fg(Color::Cyan)),
            Span::raw(" pause  "),
            Span::styled("[x]", Style::default().fg(Color::Cyan)),
            Span::raw(" cancel  "),
            Span::styled("[r]", Style::default().fg(Color::Cyan)),
            Span::raw(" refresh  "),
            Span::styled("[q]", Style::default().fg(Color::Cyan)),
            Span::raw(" quit"),
        ])
    };
 
    let bar = Paragraph::new(content)
        .block(Block::default().borders(Borders::ALL));
 
    frame.render_widget(bar, area);
}
 
// ── Helpers ───────────────────────────────────────────────────────────────────
 
/// Format raw bytes into human-readable string: 1.4 MB, 892 KB, etc.
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
 
    if bytes == 0 {
        "—".to_string()
    } else if bytes >= GB {
        format!("{:.1} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.1} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.1} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}