use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect, Alignment},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Row, Table, TableState},
    Frame, Terminal,
};
use tokio::sync::RwLock;

use crate::ipc::client::{send_request, subscribe_and_stream};
use crate::ipc::message::{JobSummary, Request};
use crate::utils::filename_from_url;

// Opencode-inspired theme colors
const _C_BG: Color = Color::Rgb(13, 17, 23);        // #0d1117 - deep dark
const C_TEXT: Color = Color::Rgb(230, 237, 243);  // #e6edf3 - bright white
const C_MUTED: Color = Color::Rgb(139, 148, 158); // #8b949e - gray
const C_ACCENT: Color = Color::Rgb(6, 182, 212);   // #06b6d4 - cyan
const C_SUCCESS: Color = Color::Rgb(34, 197, 94);  // #22c55e - green
const C_ERROR: Color = Color::Rgb(248, 113, 113);  // #f87171 - light red
const C_WARNING: Color = Color::Rgb(234, 179, 8);  // #eab308 - yellow
const C_PAUSED: Color = Color::Rgb(168, 85, 247);  // #a855f7 - purple

const SPINNER: &[&str] = &[
    "⠋","⠙","⠹","⠸",
    "⠼","⠴","⠦","⠧",
    "⠇","⠏"
];

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Tab {
    All,
    Active,
    Pending,
    Paused,
    Done,
    Failed,
}

impl Tab {
    fn filter(&self, status: &str) -> bool {
        match self {
            Tab::All => true,
            Tab::Active => status == "Active",
            Tab::Pending => status == "Pending",
            Tab::Paused => status == "Paused",
            Tab::Done => status == "Done",
            Tab::Failed => status.starts_with("Failed"),
        }
    }

    fn label(&self) -> &'static str {
        match self {
            Tab::All => "All",
            Tab::Active => "Active",
            Tab::Pending => "Pending",
            Tab::Paused => "Paused",
            Tab::Done => "Done",
            Tab::Failed => "Failed",
        }
    }
}

struct App {
    jobs: Vec<JobSummary>,
    filtered_jobs: Vec<JobSummary>,
    table_state: TableState,
    selected_job_id: Option<u64>,
    last_poll: Instant,
    last_update: Instant,
    _poll_interval: Duration,
    animation_tick: usize,
    error: Option<String>,
    should_quit: bool,
    current_tab: Tab,
    command_mode: bool,
    command_buffer: String,
    command_history: Vec<String>,
    history_pos: Option<usize>,
    history_draft: String,
    shared_jobs: Arc<RwLock<Vec<JobSummary>>>,
    connected: Arc<AtomicBool>,
}

impl App {
    fn new(shared_jobs: Arc<RwLock<Vec<JobSummary>>>, connected: Arc<AtomicBool>) -> Self {
        let mut table_state = TableState::default();
        table_state.select(Some(0));

        Self {
            jobs: Vec::new(),
            filtered_jobs: Vec::new(),
            table_state,
            selected_job_id: None,
            last_poll: Instant::now() - Duration::from_secs(2),
            last_update: Instant::now() - Duration::from_secs(10),
            _poll_interval: Duration::from_secs(1),
            animation_tick: 0,
            error: None,
            should_quit: false,
            current_tab: Tab::All,
            command_mode: false,
            command_buffer: String::new(),
            command_history: Vec::new(),
            history_pos: None,
            history_draft: String::new(),
            shared_jobs,
            connected,
        }
    }

    fn filter_jobs(&mut self) {
        self.filtered_jobs = self.jobs
            .iter()
            .filter(|j| self.current_tab.filter(&j.status))
            .cloned()
            .collect();

        // Restore selection by ID — if previously selected job no longer visible, select nothing
        if let Some(id) = self.selected_job_id {
            if let Some(pos) = self.filtered_jobs.iter().position(|j| j.id == id) {
                self.table_state.select(Some(pos));
                return;
            }
        }
        // Fallback: keep previous index if valid
        if let Some(sel) = self.table_state.selected() {
            if sel < self.filtered_jobs.len() {
                self.selected_job_id = Some(self.filtered_jobs[sel].id);
                return;
            }
        }
        // Nothing to select
        self.table_state.select(None);
        self.selected_job_id = None;
    }

    fn tab_counts(&self) -> (usize, usize, usize, usize, usize, usize) {
        let all = self.jobs.len();
        let active = self.jobs.iter().filter(|j| j.status == "Active").count();
        let pending = self.jobs.iter().filter(|j| j.status == "Pending").count();
        let paused = self.jobs.iter().filter(|j| j.status == "Paused").count();
        let done = self.jobs.iter().filter(|j| j.status == "Done").count();
        let failed = self.jobs.iter().filter(|j| j.status.starts_with("Failed")).count();
        (all, active, pending, paused, done, failed)
    }

    fn selected_job(&self) -> Option<&JobSummary> {
        self.selected_job_id
            .and_then(|id| self.filtered_jobs.iter().find(|j| j.id == id))
    }

    async fn poll_daemon(&mut self) {
        // Read from shared state updated by persistent connection background task
        let new_jobs = self.shared_jobs.read().await.clone();

        // Background reader sets connected=true after successful subscribe handshake
        if self.connected.load(Ordering::Relaxed) {
            self.last_update = Instant::now();
            self.error = None;
        } else if self.last_update.elapsed() > Duration::from_secs(3) {
            self.error = Some("Daemon unreachable".into());
        } else {
            self.error = None;
        }

        if !new_jobs.is_empty() {
            self.last_update = Instant::now();
        }

        self.jobs = new_jobs;
        self.filter_jobs();
        self.last_poll = Instant::now();
    }

    async fn execute_command(&mut self, cmd: &str) {
        let parts: Vec<&str> = cmd.trim().split_whitespace().collect();
        if parts.is_empty() {
            return;
        }

        match parts[0] {
            "add" | "a" => {
                if parts.len() > 1 {
                    let url = parts[1..].join(" ");
                    let _ = send_request(&Request::Add {
                        urls: vec![url],
                        output_dir: None,
                        connections: 4,
                        headers: vec![],
                    }).await;
                    self.poll_daemon().await;
                }
            }
            "pause" | "p" => {
                if parts.len() > 1 {
                    if let Ok(id) = parts[1].parse::<u64>() {
                        let _ = send_request(&Request::Pause { id }).await;
                        self.poll_daemon().await;
                    }
                } else if let Some(job) = self.selected_job() {
                    let _ = send_request(&Request::Pause { id: job.id }).await;
                    self.poll_daemon().await;
                }
            }
            "cancel" | "x" => {
                if parts.len() > 1 {
                    if let Ok(id) = parts[1].parse::<u64>() {
                        let _ = send_request(&Request::Cancel { id }).await;
                        self.poll_daemon().await;
                    }
                } else if let Some(job) = self.selected_job() {
                    let _ = send_request(&Request::Cancel { id: job.id }).await;
                    self.poll_daemon().await;
                }
            }
            "quit" | "q" => {
                self.should_quit = true;
            }
            "help" | "?" => {
                // Could show help overlay
            }
            _ => {}
        }

        self.command_history.push(cmd.to_string());
        if self.command_history.len() > 50 {
            self.command_history.remove(0);
        }
    }

    fn select_by_id(&mut self, id: Option<u64>) {
        self.selected_job_id = id;
        if let Some(id) = id {
            self.table_state.select(
                self.filtered_jobs.iter().position(|j| j.id == id),
            );
        } else {
            self.table_state.select(None);
        }
    }

    fn next_row(&mut self) {
        if self.filtered_jobs.is_empty() {
            self.table_state.select(None);
            return;
        }
        let i = match self.selected_job_id
            .and_then(|id| self.filtered_jobs.iter().position(|j| j.id == id))
        {
            Some(pos) => (pos + 1) % self.filtered_jobs.len(),
            None => 0,
        };
        self.select_by_id(Some(self.filtered_jobs[i].id));
    }

    fn prev_row(&mut self) {
        if self.filtered_jobs.is_empty() {
            self.table_state.select(None);
            return;
        }
        let i = match self.selected_job_id
            .and_then(|id| self.filtered_jobs.iter().position(|j| j.id == id))
        {
            Some(0) | None => self.filtered_jobs.len() - 1,
            Some(pos) => pos - 1,
        };
        self.select_by_id(Some(self.filtered_jobs[i].id));
    }

    fn next_tab(&mut self) {
        self.current_tab = match self.current_tab {
            Tab::All => Tab::Active,
            Tab::Active => Tab::Pending,
            Tab::Pending => Tab::Paused,
            Tab::Paused => Tab::Done,
            Tab::Done => Tab::Failed,
            Tab::Failed => Tab::All,
        };
        self.table_state.select(Some(0));
        self.filter_jobs();
    }
}

pub async fn run_tui() -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Shared state between background reader and UI
    let shared_jobs: Arc<RwLock<Vec<JobSummary>>> = Arc::new(RwLock::new(Vec::new()));
    let reader_jobs = Arc::clone(&shared_jobs);
    let connected: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
    let reader_connected = Arc::clone(&connected);

    // Spawn persistent connection background task
    tokio::spawn(async move {
        let _ = subscribe_and_stream(reader_jobs, reader_connected).await;
    });

    let mut app = App::new(shared_jobs, connected);
    let result = run_loop(&mut terminal, &mut app).await;

    disable_raw_mode()?;
    execute!(
        terminal.backend_mut(),
        LeaveAlternateScreen,
        DisableMouseCapture
    )?;
    terminal.show_cursor()?;

    result
}

async fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    app: &mut App,
) -> Result<()> {
    loop {
        // Poll shared state every 200ms (shared memory access, zero IPC cost)
        if app.last_poll.elapsed() >= Duration::from_millis(200) {
            app.poll_daemon().await;
        }

        app.animation_tick = app.animation_tick.wrapping_add(1);

        terminal.draw(|frame| draw(frame, app))?;

        if event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if app.command_mode {
                    match key.code {
                        KeyCode::Enter => {
                            let cmd = app.command_buffer.clone();
                            app.execute_command(&cmd).await;
                            app.command_mode = false;
                            app.command_buffer.clear();
                            app.history_pos = None;
                        }
                        KeyCode::Esc => {
                            app.command_mode = false;
                            app.command_buffer.clear();
                            app.history_pos = None;
                        }
                        KeyCode::Up => {
                            if app.command_history.is_empty() {
                                continue;
                            }
                            if app.history_pos.is_none() {
                                app.history_draft = app.command_buffer.clone();
                                app.history_pos = Some(app.command_history.len() - 1);
                            } else if let Some(pos) = app.history_pos {
                                if pos > 0 {
                                    app.history_pos = Some(pos - 1);
                                }
                            }
                            if let Some(pos) = app.history_pos {
                                app.command_buffer = app.command_history[pos].clone();
                            }
                        }
                        KeyCode::Down => {
                            if let Some(pos) = app.history_pos {
                                if pos + 1 < app.command_history.len() {
                                    app.history_pos = Some(pos + 1);
                                    app.command_buffer = app.command_history[pos + 1].clone();
                                } else {
                                    app.history_pos = None;
                                    app.command_buffer = std::mem::take(&mut app.history_draft);
                                }
                            }
                        }
                        KeyCode::Char(c) => {
                            app.command_buffer.push(c);
                        }
                        KeyCode::Backspace => {
                            app.command_buffer.pop();
                        }
                        _ => {}
                    }
                } else {
                    match (key.code, key.modifiers) {
                        (KeyCode::Char('q'), _) | (KeyCode::Char('c'), KeyModifiers::CONTROL) => {
                            app.should_quit = true;
                        }
                        (KeyCode::Down, _) | (KeyCode::Char('j'), _) => app.next_row(),
                        (KeyCode::Up, _) | (KeyCode::Char('k'), _) => app.prev_row(),
                        (KeyCode::Tab, _) => app.next_tab(),
                        (KeyCode::Char('p'), _) => {
                            if let Some(job) = app.selected_job() {
                                match job.status.as_str() {
                                    "Paused" => {
                                        let _ = send_request(&Request::Resume { id: job.id }).await;
                                    }
                                    "Active" => {
                                        let _ = send_request(&Request::Pause { id: job.id }).await;
                                    }
                                    // Pending, Done, Failed — no-op
                                    _ => {}
                                }
                                app.poll_daemon().await;
                            }
                        }
                        (KeyCode::Char('x'), _) => {
                            if let Some(job) = app.selected_job() {
                                let _ = send_request(&Request::Cancel { id: job.id }).await;
                                app.poll_daemon().await;
                            }
                        }
                        (KeyCode::Char('r'), _) => {
                            app.poll_daemon().await;
                        }
                        (KeyCode::Char('/'), _) | (KeyCode::Char(':'), _) => {
                            app.command_mode = true;
                            app.command_buffer.clear();
                        }
                        _ => {}
                    }
                }
            }
        }

        if app.should_quit { break; }
    }
    Ok(())
}

fn draw(frame: &mut Frame, app: &mut App) {
    // Fullscreen black background
    let background = Block::default()
        .style(Style::default().bg(Color::Black));

    frame.render_widget(background, frame.size());

    let chunks = Layout::default()
        .margin(1)
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(7),
            Constraint::Length(4),
        ])
        .split(frame.size());

    draw_header(frame, app, chunks[0]);
    draw_tabs(frame, app, chunks[1]);
    draw_table(frame, app, chunks[2]);
    draw_detail(frame, app, chunks[3]);
    draw_footer(frame, app, chunks[4]);

    // Render overlay ONLY in command mode
    if app.command_mode {
        let overlay = Block::default()
            .style(Style::default().bg(Color::Rgb(6, 6, 6)));

        frame.render_widget(overlay, frame.size());

        draw_command_palette(frame, app);
    }
}

fn draw_command_palette(frame: &mut Frame, app: &App) {
    let area = centered_rect(70, 20, frame.size());

    // Clear background feel using block
    let block = Block::default()
        .title(" COMMAND ")
        .borders(Borders::ALL)
        .border_style(
            Style::default()
                .fg(C_ACCENT)
                .add_modifier(Modifier::BOLD)
        );

    frame.render_widget(block.clone(), area);

    let inner = block.inner(area);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .margin(1)
        .split(inner);

    // Animated cursor
    let cursor = if app.animation_tick % 2 == 0 {
        "█"
    } else {
        ""
    };

    let input = Paragraph::new(Line::from(vec![
        Span::styled(
            "▸ ",
            Style::default()
                .fg(C_ACCENT)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            &app.command_buffer,
            Style::default().fg(C_TEXT),
        ),
        Span::styled(
            cursor,
            Style::default().fg(C_ACCENT),
        ),
    ]));

    frame.render_widget(input, chunks[0]);

    // Context/help line
    let help_text = if app.command_buffer.starts_with("add") {
        "ADD NEW DOWNLOAD"
    } else if app.command_buffer.starts_with("pause") {
        "PAUSE DOWNLOAD"
    } else if app.command_buffer.starts_with("cancel") {
        "CANCEL DOWNLOAD"
    } else {
        "READY"
    };

    let help = Paragraph::new(help_text)
        .style(Style::default().fg(C_MUTED));

    frame.render_widget(help, chunks[1]);

    // Footer actions
    let actions = Paragraph::new(Line::from(vec![
        Span::styled("ENTER", Style::default().fg(C_ACCENT)),
        Span::raw(" EXECUTE    "),
        Span::styled("ESC", Style::default().fg(C_ACCENT)),
        Span::raw(" CANCEL"),
    ]))
    .style(Style::default().fg(C_MUTED));

    frame.render_widget(actions, chunks[2]);
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}



fn draw_header(frame: &mut Frame, app: &mut App, area: Rect) {
    let total_speed = app.filtered_jobs.iter()
        .filter_map(|j| j.speed_bps)
        .sum::<u64>();

    let speed_str = if total_speed > 0 {
        format_speed(total_speed)
    } else {
        "—".to_string()
    };

    // Split header into left + right sections
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Min(0),
            Constraint::Length(20),
        ])
        .split(area);

    // left side
    let left = Paragraph::new(Line::from(vec![
        Span::styled(
            "shard",
            Style::default()
                .fg(C_ACCENT)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" "),
        if app.error.is_none() {
            Span::styled("● connected", Style::default().fg(C_SUCCESS))
        } else {
            Span::styled("○ offline", Style::default().fg(C_ERROR))
        },
    ]));
    
    // right side
    let right = Paragraph::new(Line::from(vec![
        Span::styled("↓ ", Style::default().fg(C_MUTED)),
        Span::styled(speed_str, Style::default().fg(C_ACCENT)),
    ]))
    .alignment(Alignment::Right);

    frame.render_widget(left, chunks[0]);
    frame.render_widget(right, chunks[1]);
}

fn draw_tabs(frame: &mut Frame, app: &mut App, area: Rect) {
    let (all, active, pending, paused, done, failed) = app.tab_counts();
    
    let tabs = [
        (Tab::All, all, "◉"),
        (Tab::Active, active, "↓"),
        (Tab::Pending, pending, "◌"),
        (Tab::Paused, paused, "⏸"),
        (Tab::Done, done, "✓"),
        (Tab::Failed, failed, "✕"),
    ];

    let line: Vec<Span> = tabs
        .iter()
        .enumerate()
        .map(|(i, (tab, count, icon))| {
            let text = if i == 0 {
                format!("{} {} ({})", icon, tab.label(), count)
            } else {
                format!("   {} {} ({})", icon, tab.label(), count)
            };

            let style = if app.current_tab == *tab {
                Style::default()
                    .fg(C_ACCENT)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(C_MUTED)
            };

            Span::styled(text, style)
        })
        .collect();

    let tabs = Paragraph::new(Line::from(line));

    let divider = Block::default()
        .borders(Borders::BOTTOM)
        .border_style(Style::default().fg(C_MUTED));


    frame.render_widget(tabs, area);
    frame.render_widget(divider, area);
}

fn draw_table(frame: &mut Frame, app: &mut App, area: Rect) {
    if app.filtered_jobs.is_empty() {
        let msg = if app.error.is_some() {
            "No connection to daemon"
        } else {
            "No downloads — press : to add a URL"
        };
        let placeholder = Paragraph::new(msg)
            .style(Style::default().fg(C_MUTED))
            .block(Block::default().borders(Borders::ALL).title(" Downloads ").border_style(Style::default().fg(C_MUTED)));
        frame.render_widget(placeholder, area);
        return;
    }

    let rows: Vec<Row> = app.filtered_jobs.iter().map(|job| {
        let status_badge = match job.status.as_str() {
            "Active" => {
                let spinner = SPINNER[app.animation_tick % SPINNER.len()];
                format!("{spinner} DL")
            },
            "Pending" => "◌ QUEUED".into(),
            "Paused" => "⏸ PAUSED".into(),
            "Done" => "✓ DONE".into(),
            s if s.contains("Cancelled") => "✗ CANCELLED".into(),
            s if s.starts_with("Failed") => "✗ FAILED".into(),
            _ => job.status.clone(),
        };

        let (badge_style, prog_color) = match job.status.as_str() {
            "Active" => (Style::default().fg(C_ACCENT).add_modifier(Modifier::BOLD), C_ACCENT),
            "Pending" => (Style::default().fg(C_WARNING), C_WARNING),
            "Paused" => (Style::default().fg(C_PAUSED).add_modifier(Modifier::ITALIC), C_PAUSED),
            "Done" => (Style::default().fg(C_SUCCESS).add_modifier(Modifier::BOLD), C_SUCCESS),
            s if s.contains("Cancelled") => (Style::default().fg(C_ERROR).add_modifier(Modifier::DIM), C_ERROR),
            s if s.starts_with("Failed") => (Style::default().fg(C_ERROR).add_modifier(Modifier::BOLD), C_ERROR),
            _ => (Style::default().fg(C_MUTED), C_MUTED),
        };

        let time_str = format_epoch_range(job.started_at, job.ended_at);

        let url_short = filename_from_url(&job.url);

        let bar_width: usize = 14;

        let filled = ((job.progress_pct / 100.0)
            * bar_width as f32)
            .min(bar_width as f32) as usize;

        let animated_head = if job.status == "Active" {
            match app.animation_tick % 3 {
                0 => "▓",
                1 => "▒",
                _ => "░",
            }
        } else {
            "█"
        };

        let filled_part = if filled > 0 {
            format!(
                "{}{}",
                "█".repeat(filled.saturating_sub(1)),
                animated_head
            )
        } else {
            String::new()
        };

        let bar = format!(
            "{}{}",
            filled_part,
            "▒".repeat(bar_width.saturating_sub(filled))
        );

        let prog_str = format!("{} {:.0}%", bar, job.progress_pct);

        let size_str = format_bytes(job.total_size);
        let speed_str = job.speed_bps.map(format_speed).unwrap_or_else(|| "—".into());

        let eta_str = if job.total_size > job.downloaded {
            if let Some(speed) = job.speed_bps.filter(|s| *s > 0) {
                let remaining = job.total_size - job.downloaded;
                let secs = remaining / speed;
                format_duration(secs)
            } else {
                "—".to_string()
            }
        } else {
            "—".to_string()
        };

        Row::new(vec![
            Span::raw(format!("{}", job.id)),
            Span::raw(time_str),
            Span::raw(url_short),
            Span::styled(status_badge, badge_style),
            Span::styled(prog_str, Style::default().fg(prog_color)),
            Span::raw(size_str),
            Span::raw(speed_str),
            Span::raw(eta_str),
        ])
    }).collect();

    let header_row = Row::new(vec![
        Span::styled("ID", Style::default().fg(C_MUTED)),
        Span::styled("TIME", Style::default().fg(C_MUTED)),
        Span::styled("FILE", Style::default().fg(C_MUTED)),
        Span::styled("STATUS", Style::default().fg(C_MUTED)),
        Span::styled("PROGRESS", Style::default().fg(C_MUTED)),
        Span::styled("SIZE", Style::default().fg(C_MUTED)),
        Span::styled("SPEED", Style::default().fg(C_MUTED)),
        Span::styled("ETA", Style::default().fg(C_MUTED)),
    ])
    .style(Style::default().fg(C_MUTED));

    let table = Table::new(rows, [
        Constraint::Length(4),
        Constraint::Length(20),
        Constraint::Min(20),
        Constraint::Length(10),
        Constraint::Length(20),
        Constraint::Length(10),
        Constraint::Length(10),
        Constraint::Length(8),
    ])
    .header(header_row)
    .block(Block::default().borders(Borders::NONE))
    .highlight_style(Style::default().bg(Color::Rgb(4, 38, 48)).fg(C_TEXT))
    .highlight_symbol("▸ ");

    frame.render_stateful_widget(table, area, &mut app.table_state);
}

fn draw_detail(frame: &mut Frame, app: &mut App, area: Rect) {
    if let Some(job) = app.selected_job() {
        let label_style = Style::default().fg(C_MUTED);
        let accent_style = Style::default().fg(C_ACCENT);

        let chunk_dots: String = (0..job.chunks_total)
            .map(|i| if i < job.chunks_done { "■" } else { "□" })
            .collect();

        let lines = vec![
            Line::from(vec![
                Span::styled("URL:  ", label_style),
                Span::styled(&job.url, accent_style),
            ]),
            Line::from(vec![
                Span::styled("OUT:  ", label_style),
                Span::raw(&job.output_path),
            ]),
            Line::from(vec![
                Span::styled("CHNK: ", label_style),
                Span::raw(format!("{}  {}/{}", chunk_dots, job.chunks_done, job.chunks_total)),
            ]),
            Line::from(vec![
                Span::styled("PROG: ", label_style),
                Span::raw(format!("{} / {} ({:.0}%)",
                    format_bytes(job.downloaded),
                    format_bytes(job.total_size),
                    job.progress_pct)),
            ]),
        ];

        let detail = Paragraph::new(lines)
            .style(Style::default().fg(C_MUTED))
            .block(Block::default().borders(Borders::NONE));
        frame.render_widget(detail, area);
    }
}

fn draw_footer(frame: &mut Frame, _app: &mut App, area: Rect) {
    let footer = Paragraph::new(Line::from(vec![
        Span::styled("jk", Style::default().fg(C_ACCENT)),
        Span::raw(" nav "),
        Span::styled("Tab", Style::default().fg(C_ACCENT)),
        Span::raw(" filter "),
        Span::styled("p", Style::default().fg(C_ACCENT)),
        Span::raw(" pause "),
        Span::styled("x", Style::default().fg(C_ACCENT)),
        Span::raw(" cancel "),
        Span::styled("r", Style::default().fg(C_ACCENT)),
        Span::raw(" refresh "),
        Span::styled("/", Style::default().fg(C_ACCENT)),
        Span::raw(" command "),
        Span::styled("q", Style::default().fg(C_ACCENT)),
        Span::raw(" quit"),
    ]))
    .block(Block::default().borders(Borders::NONE));

    frame.render_widget(footer, area);
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;

    if bytes == 0 { "—".to_string() }
    else if bytes >= GB { format!("{:.1} GB", bytes as f64 / GB as f64) }
    else if bytes >= MB { format!("{:.1} MB", bytes as f64 / MB as f64) }
    else if bytes >= KB { format!("{:.1} KB", bytes as f64 / KB as f64) }
    else { format!("{} B", bytes) }
}

fn format_speed(bps: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;

    if bps >= MB { format!("{:.1} MB/s", bps as f64 / MB as f64) }
    else if bps >= KB { format!("{:.1} KB/s", bps as f64 / KB as f64) }
    else { format!("{} B/s", bps) }
}

fn format_duration(secs: u64) -> String {
    if secs >= 3600 {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    } else if secs >= 60 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else {
        format!("{}s", secs)
    }
}

fn fmt_epoch(epoch_secs: i64) -> String {
    let day_secs = epoch_secs.rem_euclid(86400);
    let h = day_secs / 3600;
    let m = (day_secs % 3600) / 60;
    let s = day_secs % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

fn format_epoch_range(start: Option<i64>, end: Option<i64>) -> String {
    match (start, end) {
        (Some(s), Some(e)) => format!("{} - {}", fmt_epoch(s), fmt_epoch(e)),
        (Some(s), None) => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            format!("{} - {}", fmt_epoch(s), fmt_epoch(now))
        }
        _ => "—".into(),
    }
}