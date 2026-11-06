//! One ordered log of what a [`Supervisor`](crate::Supervisor) is doing, and
//! watchers that read it. Publishing never waits for a watcher: the log keeps
//! the newest events up to a fixed size, and a watcher that falls behind is
//! told how many it missed. Output bytes have a byte budget; opaque framework
//! payloads have a separate event-count budget because their size is unknown.
use crate::{AgentRun, ExecutableIdentity, ProcessReceipt, SupervisorProcessId};
use std::any::Any;
use std::collections::{BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::watch;

const MAX_RETAINED_BYTES: usize = 8 * 1024 * 1024;
const MAX_RETAINED_EVENTS: usize = 65_536;
// Agent payloads are opaque `Any` values, so their allocation size cannot be
// measured. Keep their count separately bounded instead of retaining one per
// event forever when output traffic is otherwise quiet.
const MAX_RETAINED_AGENT_EVENTS: usize = 256;
/// Size charged per event for fields other than output bytes.
const EVENT_OVERHEAD_BYTES: usize = 128;

/// A framework's own event, carried unchanged. Use `downcast_ref` to read it.
pub type AgentEventPayload = Arc<dyn Any + Send + Sync>;

/// What happened.
#[derive(Debug, Clone)]
pub enum SupervisorEventKind {
    /// An agent run began.
    RunStarted,
    /// An agent run finished and its processes were stopped.
    RunEnded,
    /// A process started.
    ProcessStarted { executable: ExecutableIdentity },
    /// Bytes the process wrote to stdout.
    Stdout(Vec<u8>),
    /// Bytes the process wrote to stderr.
    Stderr(Vec<u8>),
    /// A process finished. The receipt is `None` when its handle was dropped
    /// before the exit was observed.
    ProcessExited(Option<ProcessReceipt>),
    /// An event published by an agent framework integration.
    Agent(AgentEventPayload),
    /// This watcher fell behind and `dropped` log entries were lost. Not every
    /// lost entry was necessarily in this watcher's scope.
    Lagged { dropped: u64 },
}

/// One entry in the log.
#[derive(Debug, Clone)]
pub struct SupervisorEvent {
    /// Position in this supervisor's log. Later events have larger numbers.
    pub seq: u64,
    /// The agent run this belongs to, if any.
    pub run_id: Option<u64>,
    /// The process this belongs to, if any.
    pub process_id: Option<SupervisorProcessId>,
    pub kind: SupervisorEventKind,
}

/// What a watcher sees.
#[derive(Clone, Copy)]
pub enum StreamScope<'a> {
    /// Everything this supervisor does. Ends when the supervisor is dropped.
    All,
    /// One agent run: agent events and its processes. Ends when the run ends.
    Run(&'a AgentRun),
    /// One process or background job. Ends when it exits.
    Process(SupervisorProcessId),
}

#[derive(Clone, Copy)]
enum Target {
    All,
    Run(u64),
    Process(SupervisorProcessId),
}

impl Target {
    fn matches(self, event: &SupervisorEvent) -> bool {
        match self {
            Self::All => true,
            Self::Run(id) => event.run_id == Some(id),
            Self::Process(id) => event.process_id == Some(id),
        }
    }

    fn is_final(self, event: &SupervisorEvent) -> bool {
        match self {
            Self::All => false,
            Self::Run(_) => matches!(event.kind, SupervisorEventKind::RunEnded),
            Self::Process(_) => matches!(event.kind, SupervisorEventKind::ProcessExited(_)),
        }
    }
}

#[derive(Default)]
struct LogState {
    events: VecDeque<SupervisorEvent>,
    retained_bytes: usize,
    retained_agent_events: usize,
    next_seq: u64,
    active_runs: BTreeSet<u64>,
    active_processes: BTreeSet<SupervisorProcessId>,
    closed: bool,
}

impl LogState {
    fn oldest_seq(&self) -> u64 {
        self.events.front().map_or(self.next_seq, |event| event.seq)
    }

    fn push(
        &mut self,
        run_id: Option<u64>,
        process_id: Option<SupervisorProcessId>,
        kind: SupervisorEventKind,
    ) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        let event = SupervisorEvent {
            seq,
            run_id,
            process_id,
            kind,
        };
        self.retained_bytes += event_size(&event);
        if matches!(&event.kind, SupervisorEventKind::Agent(_)) {
            self.retained_agent_events += 1;
        }
        self.events.push_back(event);
        while self.retained_agent_events > MAX_RETAINED_AGENT_EVENTS {
            self.evict_oldest_agent_event();
        }
        while self.events.len() > MAX_RETAINED_EVENTS || self.retained_bytes > MAX_RETAINED_BYTES {
            self.evict_front();
        }
        self.next_seq
    }

    fn evict_oldest_agent_event(&mut self) {
        if let Some(index) = self
            .events
            .iter()
            .position(|event| matches!(&event.kind, SupervisorEventKind::Agent(_)))
        {
            if let Some(event) = self.events.remove(index) {
                self.retained_bytes -= event_size(&event);
                self.retained_agent_events -= 1;
            }
        }
    }

    fn evict_front(&mut self) {
        if let Some(event) = self.events.pop_front() {
            self.retained_bytes -= event_size(&event);
            if matches!(&event.kind, SupervisorEventKind::Agent(_)) {
                self.retained_agent_events -= 1;
            }
        }
    }
}

fn event_size(event: &SupervisorEvent) -> usize {
    EVENT_OVERHEAD_BYTES
        + match &event.kind {
            SupervisorEventKind::Stdout(bytes) | SupervisorEventKind::Stderr(bytes) => bytes.len(),
            _ => 0,
        }
}

pub(crate) struct EventLog {
    state: Mutex<LogState>,
    changes: watch::Sender<u64>,
}

impl EventLog {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(LogState::default()),
            changes: watch::Sender::new(0),
        })
    }

    fn lock(&self) -> MutexGuard<'_, LogState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn update(&self, change: impl FnOnce(&mut LogState) -> u64) {
        let version = change(&mut self.lock());
        self.changes.send_replace(version);
    }

    pub(crate) fn publish(
        &self,
        run_id: Option<u64>,
        process_id: Option<SupervisorProcessId>,
        kind: SupervisorEventKind,
    ) {
        self.update(|state| state.push(run_id, process_id, kind));
    }

    pub(crate) fn run_started(&self, run_id: u64) {
        self.update(|state| {
            state.active_runs.insert(run_id);
            state.push(Some(run_id), None, SupervisorEventKind::RunStarted)
        });
    }

    pub(crate) fn run_ended(&self, run_id: u64) {
        self.update(|state| {
            state.active_runs.remove(&run_id);
            state.push(Some(run_id), None, SupervisorEventKind::RunEnded)
        });
    }

    fn process_started(
        &self,
        run_id: Option<u64>,
        process_id: SupervisorProcessId,
        executable: ExecutableIdentity,
    ) {
        self.update(|state| {
            state.active_processes.insert(process_id);
            state.push(
                run_id,
                Some(process_id),
                SupervisorEventKind::ProcessStarted { executable },
            )
        });
    }

    fn process_exited(
        &self,
        run_id: Option<u64>,
        process_id: SupervisorProcessId,
        receipt: Option<ProcessReceipt>,
    ) {
        self.update(|state| {
            state.active_processes.remove(&process_id);
            state.push(
                run_id,
                Some(process_id),
                SupervisorEventKind::ProcessExited(receipt),
            )
        });
    }

    /// End every watcher once it has read what is retained.
    pub(crate) fn close(&self) {
        self.update(|state| {
            state.closed = true;
            state.next_seq
        });
    }
}

struct TapInner {
    log: Arc<EventLog>,
    run_id: Option<u64>,
    process_id: SupervisorProcessId,
    started: AtomicBool,
    finished: AtomicBool,
}

impl Drop for TapInner {
    fn drop(&mut self) {
        // Reached when the process handle and its output readers are gone
        // without an observed exit, so a watcher is never left waiting.
        if self.started.load(Ordering::Acquire) && !self.finished.swap(true, Ordering::AcqRel) {
            self.log.process_exited(self.run_id, self.process_id, None);
        }
    }
}

/// Publishes one process's events to its supervisor's log.
#[derive(Clone)]
pub(crate) struct ProcessTap {
    inner: Arc<TapInner>,
}

impl ProcessTap {
    pub(crate) fn new(
        log: Arc<EventLog>,
        run_id: Option<u64>,
        process_id: SupervisorProcessId,
    ) -> Self {
        Self {
            inner: Arc::new(TapInner {
                log,
                run_id,
                process_id,
                started: AtomicBool::new(false),
                finished: AtomicBool::new(false),
            }),
        }
    }

    pub(crate) fn started(&self, executable: ExecutableIdentity) {
        let inner = &self.inner;
        inner.started.store(true, Ordering::Release);
        inner
            .log
            .process_started(inner.run_id, inner.process_id, executable);
    }

    pub(crate) fn stdout(&self, bytes: &[u8]) {
        self.output(SupervisorEventKind::Stdout(bytes.to_vec()));
    }

    pub(crate) fn stderr(&self, bytes: &[u8]) {
        self.output(SupervisorEventKind::Stderr(bytes.to_vec()));
    }

    fn output(&self, kind: SupervisorEventKind) {
        let inner = &self.inner;
        inner
            .log
            .publish(inner.run_id, Some(inner.process_id), kind);
    }

    /// Report the exit once; later calls do nothing.
    pub(crate) fn finish(&self, receipt: Option<ProcessReceipt>) {
        let inner = &self.inner;
        if !inner.finished.swap(true, Ordering::AcqRel) {
            inner
                .log
                .process_exited(inner.run_id, inner.process_id, receipt);
        }
    }
}

/// A watcher over a supervisor's events, from [`Supervisor::stream`](crate::Supervisor::stream).
///
/// Watching never affects the work: dropping this stops nothing, and a slow
/// watcher never slows a process. It starts with the oldest retained event, so
/// a watcher opened just after a process started still sees its first output.
pub struct EventStream {
    log: Arc<EventLog>,
    changes: watch::Receiver<u64>,
    target: Target,
    cursor: u64,
    done: bool,
}

impl EventStream {
    pub(crate) fn new(log: Arc<EventLog>, scope: StreamScope<'_>, own_run: bool) -> Self {
        let target = match scope {
            StreamScope::All => Target::All,
            StreamScope::Run(run) => Target::Run(run.id()),
            StreamScope::Process(id) => Target::Process(id),
        };
        let cursor = log.lock().oldest_seq();
        let changes = log.changes.subscribe();
        Self {
            log,
            changes,
            target,
            cursor,
            // A run from another supervisor has no events here.
            done: !own_run,
        }
    }

    /// The next event in scope, or `None` when the scope has ended. Safe to
    /// cancel: no event is lost if this future is dropped.
    pub async fn next(&mut self) -> Option<SupervisorEvent> {
        loop {
            if self.done {
                return None;
            }
            // Mark changes as seen before reading, so a publish that lands
            // after the read below still wakes this watcher.
            self.changes.borrow_and_update();
            if let Some(event) = self.read() {
                return Some(event);
            }
            if self.done {
                return None;
            }
            if self.changes.changed().await.is_err() {
                self.done = true;
            }
        }
    }

    fn read(&mut self) -> Option<SupervisorEvent> {
        let log = Arc::clone(&self.log);
        let state = log.lock();
        let oldest = state.oldest_seq();
        if self.cursor < oldest {
            let dropped = oldest - self.cursor;
            self.cursor = oldest;
            return Some(SupervisorEvent {
                // The marker describes the gap before `oldest`; keep sequence
                // numbers strictly increasing when the oldest event follows.
                seq: oldest.saturating_sub(1),
                run_id: None,
                process_id: None,
                kind: SupervisorEventKind::Lagged { dropped },
            });
        }
        let start = usize::try_from(self.cursor - oldest).unwrap_or(usize::MAX);
        for event in state.events.iter().skip(start) {
            self.cursor = event.seq + 1;
            if self.target.matches(event) {
                self.done = self.target.is_final(event);
                return Some(event.clone());
            }
        }
        self.done = state.closed
            || match self.target {
                Target::All => false,
                Target::Run(id) => !state.active_runs.contains(&id),
                Target::Process(id) => !state.active_processes.contains(&id),
            };
        None
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::{AgentRunPolicy, Command, Policy, Supervisor, TerminationReason};
    use std::time::Duration;

    fn sh(script: &str) -> Command {
        Command {
            argv: vec!["/bin/sh".into(), "-c".into(), script.into()],
            cwd: None,
        }
    }

    /// Read a stream until it ends.
    async fn collect(stream: &mut EventStream) -> Vec<SupervisorEvent> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut events = Vec::new();
            while let Some(event) = stream.next().await {
                events.push(event);
            }
            events
        })
        .await
        .expect("stream should end")
    }

    fn stdout_of(events: &[SupervisorEvent]) -> Vec<u8> {
        events
            .iter()
            .filter_map(|event| match &event.kind {
                SupervisorEventKind::Stdout(bytes) => Some(bytes.as_slice()),
                _ => None,
            })
            .flatten()
            .copied()
            .collect()
    }

    fn receipt_of(events: &[SupervisorEvent]) -> Option<&ProcessReceipt> {
        events.iter().find_map(|event| match &event.kind {
            SupervisorEventKind::ProcessExited(receipt) => receipt.as_ref(),
            _ => None,
        })
    }

    #[tokio::test]
    async fn background_job_can_be_watched_live() {
        let supervisor = Supervisor::new();
        let job = supervisor
            .start_background(
                &sh("echo first; sleep 0.2; echo second"),
                &Policy::default(),
            )
            .unwrap();
        let mut stream = supervisor.stream(StreamScope::Process(job));
        // The first line arrives while the job is still running.
        let first_output = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let event = stream.next().await.expect("job output");
                if let SupervisorEventKind::Stdout(bytes) = event.kind {
                    return bytes;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(first_output, b"first\n");
        assert_eq!(
            supervisor.process_status(job),
            Some(crate::ManagedProcessStatus::Running)
        );
        let rest = collect(&mut stream).await;
        assert_eq!(stdout_of(&rest), b"second\n");
        assert!(receipt_of(&rest).is_some());
    }

    #[tokio::test]
    async fn late_watcher_replays_and_sees_output_before_the_exit() {
        let supervisor = Supervisor::new();
        let job = supervisor
            .start_background(&sh("echo done"), &Policy::default())
            .unwrap();
        supervisor.wait_process(job).await.unwrap();
        let events = collect(&mut supervisor.stream(StreamScope::Process(job))).await;
        assert!(matches!(
            events[0].kind,
            SupervisorEventKind::ProcessStarted { .. }
        ));
        assert_eq!(stdout_of(&events), b"done\n");
        assert!(matches!(
            events.last().unwrap().kind,
            SupervisorEventKind::ProcessExited(Some(_))
        ));
    }

    #[tokio::test]
    async fn dropping_a_watcher_does_not_stop_the_job() {
        let supervisor = Supervisor::new();
        let job = supervisor
            .start_background(&sh("sleep 0.2; echo survived"), &Policy::default())
            .unwrap();
        drop(supervisor.stream(StreamScope::Process(job)));
        let result = supervisor.wait_process(job).await.unwrap().unwrap();
        assert_eq!(result.stdout, b"survived\n");
    }

    #[tokio::test]
    async fn run_scope_shows_agent_events_and_its_tools_in_order() {
        let supervisor = Supervisor::new();
        let run = supervisor.start_agent_run(AgentRunPolicy::default());
        let sibling = supervisor.start_agent_run(AgentRunPolicy::default());
        let mut stream = supervisor.stream(StreamScope::Run(&run));

        run.publish_agent_event(Arc::new("thinking".to_string()));
        let mut policy = Policy::default();
        policy.limits.timeout_ms = 5_000;
        run.run_command(&sh("echo one-shot"), &policy, b"")
            .await
            .unwrap();
        let child = run.start(&sh("echo interactive"), &policy).unwrap();
        child.into_event_stream().unwrap().wait().await.unwrap();
        sibling
            .run_command(&sh("echo sibling"), &policy, b"")
            .await
            .unwrap();
        run.cancel().await;
        sibling.cancel().await;

        let events = collect(&mut stream).await;
        assert!(matches!(events[0].kind, SupervisorEventKind::RunStarted));
        let SupervisorEventKind::Agent(payload) = &events[1].kind else {
            panic!("expected the agent event, got {:?}", events[1].kind);
        };
        assert_eq!(payload.downcast_ref::<String>().unwrap(), "thinking");
        assert_eq!(stdout_of(&events), b"one-shot\ninteractive\n");
        assert!(matches!(
            events.last().unwrap().kind,
            SupervisorEventKind::RunEnded
        ));
        assert!(events.iter().all(|event| event.run_id == Some(run.id())));
        assert!(events.windows(2).all(|pair| pair[0].seq < pair[1].seq));
    }

    #[tokio::test]
    async fn all_scope_ends_when_the_supervisor_is_dropped() {
        let supervisor = Supervisor::new();
        let job = supervisor
            .start_background(&sh("echo hello"), &Policy::default())
            .unwrap();
        supervisor.wait_process(job).await.unwrap();
        let mut stream = supervisor.stream(StreamScope::All);
        drop(supervisor);
        assert_eq!(stdout_of(&collect(&mut stream).await), b"hello\n");
    }

    #[tokio::test]
    async fn watcher_is_empty_for_a_foreign_run_or_unknown_process() {
        let supervisor = Supervisor::new();
        let other = Supervisor::new();
        // Run IDs are per supervisor, so both of these runs have the same ID.
        let own_run = supervisor.start_agent_run(AgentRunPolicy::default());
        let run = other.start_agent_run(AgentRunPolicy::default());
        assert_eq!(own_run.id(), run.id());
        assert!(collect(&mut supervisor.stream(StreamScope::Run(&run)))
            .await
            .is_empty());
        own_run.cancel().await;
        let job = other
            .start_background(&sh("sleep 5"), &Policy::default())
            .unwrap();
        assert!(collect(&mut supervisor.stream(StreamScope::Process(job)))
            .await
            .is_empty());
        other.shutdown_all(Duration::ZERO).await.unwrap();
    }

    #[tokio::test]
    async fn dropped_handle_still_ends_the_watcher() {
        let supervisor = Supervisor::new();
        let child = supervisor
            .start(&sh("sleep 30"), &Policy::default())
            .unwrap();
        let id = child.supervisor_id().unwrap();
        let mut stream = supervisor.stream(StreamScope::Process(id));
        drop(child);
        let events = collect(&mut stream).await;
        assert!(matches!(
            events.last().unwrap().kind,
            SupervisorEventKind::ProcessExited(None)
        ));
    }

    #[tokio::test]
    async fn slow_watcher_is_told_what_it_missed_and_publishing_never_waits() {
        let log = EventLog::new();
        let mut stream = EventStream::new(Arc::clone(&log), StreamScope::All, true);
        let published = MAX_RETAINED_EVENTS as u64 + 10;
        for _ in 0..published {
            log.publish(None, None, SupervisorEventKind::RunStarted);
        }
        let first = stream.next().await.unwrap();
        assert!(matches!(
            first.kind,
            SupervisorEventKind::Lagged { dropped: 10 }
        ));
        assert_eq!(stream.next().await.unwrap().seq, 10);
    }

    #[test]
    fn log_retention_is_bounded_by_bytes() {
        let log = EventLog::new();
        let chunk = vec![0_u8; 1024 * 1024];
        for _ in 0..32 {
            log.publish(None, None, SupervisorEventKind::Stdout(chunk.clone()));
        }
        assert!(log.lock().retained_bytes <= MAX_RETAINED_BYTES);
    }

    #[tokio::test]
    async fn background_job_keeps_running_past_its_output_limit() {
        let supervisor = Supervisor::new();
        let mut policy = Policy::default();
        policy.limits.output_bytes = 16;
        let script = "i=0; while [ $i -lt 2000 ]; do echo line-$i; i=$((i+1)); done";
        let job = supervisor.start_background(&sh(script), &policy).unwrap();
        let result = supervisor.wait_process(job).await.unwrap().unwrap();
        assert!(matches!(
            result.receipt.termination,
            TerminationReason::Exited { code: Some(0), .. }
        ));
        assert!(result.output_truncated);
        assert_eq!(result.stdout.len(), 16);
        assert!(result.stdout.ends_with(b"line-1999\n"));
    }

    #[tokio::test]
    async fn background_job_within_its_limit_is_not_truncated() {
        let supervisor = Supervisor::new();
        let job = supervisor
            .start_background(&sh("echo small"), &Policy::default())
            .unwrap();
        let result = supervisor.wait_process(job).await.unwrap().unwrap();
        assert!(!result.output_truncated);
    }
}
