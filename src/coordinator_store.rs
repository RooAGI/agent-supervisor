//! Versioned journal for Supervisor coordinator state.
//!
//! The service injects a journal path beneath its configured state directory.
//! A command is accepted only after its complete JSONL record has been
//! appended and `sync_data` succeeds. The caller must publish the event and
//! acknowledge the command only after `commit` returns. This persists
//! Supervisor bookkeeping; it cannot make framework side effects exactly
//! once and does not restore a live framework task.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;
use uuid::Uuid;

const JOURNAL_SCHEMA_VERSION: u32 = 2;
const MAX_JOURNAL_BYTES: usize = 8 * 1024 * 1024;
const MAX_RECORDS_BEFORE_COMPACTION: usize = 1024;
const MAX_SNAPSHOT_BYTES: usize = 4 * 1024 * 1024;
const MAX_COMMAND_BYTES: usize = 128 * 1024;
const MAX_RESULT_BYTES: usize = 128 * 1024;
const MAX_EVENT_BYTES: usize = 32 * 1024;
const MAX_RETAINED_EVENTS: usize = 256;
const MAX_RETAINED_EVENT_BYTES: usize = 512 * 1024;
const MAX_IDEMPOTENCY_RECEIPTS: usize = 4096;
const MAX_IDEMPOTENCY_BYTES: usize = 2 * 1024 * 1024;
const MAX_PENDING_RESERVATIONS: usize = 4096;
const MAX_DIAGNOSTIC_WARNINGS: usize = 32;

#[derive(Debug, Error)]
pub(crate) enum CoordinatorStoreError {
    #[error("coordinator store I/O error at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("unsupported coordinator journal schema version {version} at line {line}")]
    UnsupportedVersion { line: usize, version: u32 },
    #[error("corrupt coordinator journal at line {line}: {message}")]
    Corrupt { line: usize, message: String },
    #[error("coordinator journal command ID is empty or exceeds 256 bytes")]
    InvalidRequestId,
    #[error("request ID was already committed with a different command payload")]
    IdempotencyConflict,
    #[error("coordinator journal payload exceeds its configured bound")]
    PayloadTooLarge,
    #[error("coordinator journal is unavailable after a failed append; reopen it to recover")]
    Poisoned,
    #[error("coordinator journal compaction failed: {0}")]
    Compaction(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DurableEvent {
    pub cursor: u64,
    pub value: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CommandReceipt {
    pub request_id: String,
    pub command: Value,
    pub result: Value,
    pub sequence: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub(crate) struct CommandReservation {
    pub request_id: String,
    pub command: Value,
    pub reserved_sequence: u64,
}

#[derive(Clone, Debug)]
pub(crate) enum ReservationOutcome {
    New { sequence: u64 },
    ReservedUnknown { sequence: u64 },
    Completed { sequence: u64, result: Value },
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecoveryDiagnostics {
    pub journal_schema_version: u32,
    pub records_replayed: u64,
    pub events_retained: usize,
    pub idempotency_receipts_retained: usize,
    pub pending_command_reservations: usize,
    pub truncated_incomplete_tail: bool,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecoveryState {
    pub sequence: u64,
    pub previous_epoch: Option<String>,
    pub snapshot: Option<Value>,
    pub events: VecDeque<DurableEvent>,
    pub receipts: VecDeque<CommandReceipt>,
    pub reservations: VecDeque<CommandReservation>,
    pub recovered_attempts: Vec<RecoveredAttempt>,
    pub diagnostics: RecoveryDiagnostics,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub(crate) enum RestartClassification {
    Recovered,
    Interrupted,
    Unverified,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FrameworkSessionEvidence {
    Verified,
    Unavailable,
    NotChecked,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct RecoveredAttempt {
    pub attempt_id: String,
    pub generation: u64,
    pub classification: RestartClassification,
    pub reason: String,
}

/// Classify an attempt after restart using explicit framework evidence.
/// Process IDs are intentionally absent: an old PID by itself cannot restore
/// Supervisor ownership or prove that it still refers to the same process.
pub(crate) fn classify_attempt_restart(
    status: &str,
    framework_session: FrameworkSessionEvidence,
) -> (RestartClassification, &'static str) {
    match status {
        "succeeded" | "failed" | "cancelled" => (
            RestartClassification::Recovered,
            "a terminal framework outcome was durably recorded",
        ),
        "queued" => (
            RestartClassification::Interrupted,
            "accepted work had not reached a confirmed running attempt",
        ),
        _ if framework_session == FrameworkSessionEvidence::Verified => (
            RestartClassification::Recovered,
            "the framework verified its session or checkpoint for continuation",
        ),
        _ => (
            RestartClassification::Unverified,
            "the prior active attempt lacks verified framework session evidence",
        ),
    }
}

#[derive(Clone, Debug)]
pub(crate) struct CommitOutcome {
    pub sequence: u64,
    pub result: Value,
    pub duplicate: bool,
    pub compaction_warning: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JournalRecord {
    schema_version: u32,
    sequence: u64,
    #[serde(flatten)]
    body: JournalBody,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "recordType", rename_all = "camelCase")]
enum JournalBody {
    Commit {
        request_id: String,
        command: Value,
        result: Value,
        event: Value,
        snapshot: Value,
    },
    Checkpoint {
        snapshot: Value,
        events: VecDeque<DurableEvent>,
        receipts: VecDeque<CommandReceipt>,
        #[serde(default)]
        reservations: VecDeque<CommandReservation>,
    },
    Reservation {
        request_id: String,
        command: Value,
    },
    CommandCompleted {
        request_id: String,
        command: Value,
        result: Value,
    },
}

/// A synchronous journal. Call `commit` off a Tokio worker thread (for
/// example with `spawn_blocking`) so filesystem sync latency cannot stall
/// process stream handling. A failed append poisons this instance because a
/// partial line may remain; reopen performs tail recovery before accepting
/// more commands.
pub(crate) struct CoordinatorStore {
    path: PathBuf,
    file: File,
    state: RecoveryState,
    file_bytes: usize,
    records_in_file: usize,
    poisoned: bool,
}

impl CoordinatorStore {
    /// Open or create the journal at a path chosen by the service owner.
    /// Returns the recovered snapshot, bounded event history, idempotency
    /// receipts, and explicit recovery diagnostics.
    pub(crate) fn open(
        path: impl AsRef<Path>,
    ) -> Result<(Self, RecoveryState), CoordinatorStoreError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|source| store_io(&path, source))?;
        }
        let mut open_options = OpenOptions::new();
        open_options.create(true).read(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            open_options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        let mut file = open_options
            .open(&path)
            .map_err(|source| store_io(&path, source))?;
        validate_private_file(&path, &file)?;
        set_private_permissions(&path, &file)?;

        let (mut state, file_bytes, records_in_file) = recover(&path, &mut file)?;
        state.diagnostics.journal_schema_version = JOURNAL_SCHEMA_VERSION;
        state.diagnostics.events_retained = state.events.len();
        state.diagnostics.idempotency_receipts_retained = state.receipts.len();
        state.diagnostics.pending_command_reservations = state.reservations.len();
        state.recovered_attempts = classify_snapshot_attempts(state.snapshot.as_ref());
        file.seek(SeekFrom::End(0))
            .map_err(|source| store_io(&path, source))?;
        let recovered = state.clone();
        Ok((
            Self {
                path,
                file,
                state,
                file_bytes,
                records_in_file,
                poisoned: false,
            },
            recovered,
        ))
    }

    pub(crate) fn state(&self) -> &RecoveryState {
        &self.state
    }

    pub(crate) fn diagnostics(&self) -> RecoveryDiagnostics {
        self.state.diagnostics.clone()
    }

    /// Return a retained receipt so the coordinator can short-circuit a
    /// retried command before applying its state transition again.
    pub(crate) fn receipt(&self, request_id: &str) -> Option<&CommandReceipt> {
        self.state
            .receipts
            .iter()
            .find(|receipt| receipt.request_id == request_id)
    }

    /// Durably reserve an externally side-effecting command before dispatch.
    /// A reservation without a completion receipt is intentionally reported
    /// as unknown after restart; callers must not repeat the side effect.
    pub(crate) fn reserve_command(
        &mut self,
        request_id: &str,
        command: Value,
    ) -> Result<ReservationOutcome, CoordinatorStoreError> {
        if self.poisoned {
            return Err(CoordinatorStoreError::Poisoned);
        }
        if request_id.is_empty() || request_id.len() > 256 {
            return Err(CoordinatorStoreError::InvalidRequestId);
        }
        if encoded_len(&command) > MAX_COMMAND_BYTES {
            return Err(CoordinatorStoreError::PayloadTooLarge);
        }
        if let Some(receipt) = self.receipt(request_id) {
            if receipt.command != command {
                return Err(CoordinatorStoreError::IdempotencyConflict);
            }
            return Ok(ReservationOutcome::Completed {
                sequence: receipt.sequence,
                result: receipt.result.clone(),
            });
        }
        if let Some(reservation) = self
            .state
            .reservations
            .iter()
            .find(|reservation| reservation.request_id == request_id)
        {
            if reservation.command != command {
                return Err(CoordinatorStoreError::IdempotencyConflict);
            }
            return Ok(ReservationOutcome::ReservedUnknown {
                sequence: reservation.reserved_sequence,
            });
        }
        if self.state.reservations.len() >= MAX_PENDING_RESERVATIONS {
            return Err(CoordinatorStoreError::PayloadTooLarge);
        }

        let sequence = self.state.sequence.saturating_add(1);
        let record = JournalRecord {
            schema_version: JOURNAL_SCHEMA_VERSION,
            sequence,
            body: JournalBody::Reservation {
                request_id: request_id.to_owned(),
                command: command.clone(),
            },
        };
        let mut bytes =
            serde_json::to_vec(&record).map_err(|error| CoordinatorStoreError::Corrupt {
                line: 0,
                message: format!("could not serialize reservation: {error}"),
            })?;
        bytes.push(b'\n');
        if let Err(source) = self.file.write_all(&bytes) {
            self.poisoned = true;
            return Err(store_io(&self.path, source));
        }
        if let Err(source) = self.file.sync_data() {
            self.poisoned = true;
            return Err(store_io(&self.path, source));
        }
        self.file_bytes = self.file_bytes.saturating_add(bytes.len());
        self.records_in_file = self.records_in_file.saturating_add(1);
        self.state.sequence = sequence;
        self.state.reservations.push_back(CommandReservation {
            request_id: request_id.to_owned(),
            command,
            reserved_sequence: sequence,
        });
        let warning = if self.file_bytes > MAX_JOURNAL_BYTES
            || self.records_in_file > MAX_RECORDS_BEFORE_COMPACTION
        {
            self.compact().err().map(|error| error.to_string())
        } else {
            None
        };
        if let Some(message) = warning {
            self.state
                .diagnostics
                .warnings
                .push(format!("journal compaction deferred: {message}"));
        }
        self.refresh_diagnostics();
        Ok(ReservationOutcome::New { sequence })
    }

    /// Complete a previously reserved command after its external side effect
    /// and the associated coordinator state are durably recorded. This receipt
    /// lets a retried request return the original process result without
    /// launching a second process.
    pub(crate) fn complete_reservation(
        &mut self,
        request_id: &str,
        result: Value,
    ) -> Result<CommitOutcome, CoordinatorStoreError> {
        if self.poisoned {
            return Err(CoordinatorStoreError::Poisoned);
        }
        if request_id.is_empty() || request_id.len() > 256 {
            return Err(CoordinatorStoreError::InvalidRequestId);
        }
        if encoded_len(&result) > MAX_RESULT_BYTES {
            return Err(CoordinatorStoreError::PayloadTooLarge);
        }
        if let Some(receipt) = self.receipt(request_id) {
            return Ok(CommitOutcome {
                sequence: receipt.sequence,
                result: receipt.result.clone(),
                duplicate: true,
                compaction_warning: None,
            });
        }
        let command = self
            .state
            .reservations
            .iter()
            .find(|reservation| reservation.request_id == request_id)
            .map(|reservation| reservation.command.clone())
            .ok_or_else(|| CoordinatorStoreError::Corrupt {
                line: 0,
                message: "cannot complete an unreserved command".to_owned(),
            })?;

        let sequence = self.state.sequence.saturating_add(1);
        let record = JournalRecord {
            schema_version: JOURNAL_SCHEMA_VERSION,
            sequence,
            body: JournalBody::CommandCompleted {
                request_id: request_id.to_owned(),
                command: command.clone(),
                result: result.clone(),
            },
        };
        let mut bytes =
            serde_json::to_vec(&record).map_err(|error| CoordinatorStoreError::Corrupt {
                line: 0,
                message: format!("could not serialize completion receipt: {error}"),
            })?;
        bytes.push(b'\n');
        if let Err(source) = self.file.write_all(&bytes) {
            self.poisoned = true;
            return Err(store_io(&self.path, source));
        }
        if let Err(source) = self.file.sync_data() {
            self.poisoned = true;
            return Err(store_io(&self.path, source));
        }
        self.file_bytes = self.file_bytes.saturating_add(bytes.len());
        self.records_in_file = self.records_in_file.saturating_add(1);
        self.state.sequence = sequence;
        self.state
            .reservations
            .retain(|reservation| reservation.request_id != request_id);
        self.state.receipts.push_back(CommandReceipt {
            request_id: request_id.to_owned(),
            command,
            result: result.clone(),
            sequence,
        });
        trim_receipts(&mut self.state.receipts);
        self.refresh_diagnostics();
        let compaction_warning = if self.file_bytes > MAX_JOURNAL_BYTES
            || self.records_in_file > MAX_RECORDS_BEFORE_COMPACTION
        {
            self.compact().err().map(|error| error.to_string())
        } else {
            None
        };
        Ok(CommitOutcome {
            sequence,
            result,
            duplicate: false,
            compaction_warning,
        })
    }

    /// Durably commit one accepted command and the corresponding coordinator
    /// snapshot. Reusing a request ID with the same command returns the
    /// original result without appending or dispatching it again. Reusing the
    /// ID with different command data is rejected.
    pub(crate) fn commit(
        &mut self,
        request_id: &str,
        command: Value,
        event: Value,
        snapshot: Value,
        result: Value,
    ) -> Result<CommitOutcome, CoordinatorStoreError> {
        if self.poisoned {
            return Err(CoordinatorStoreError::Poisoned);
        }
        if request_id.is_empty() || request_id.len() > 256 {
            return Err(CoordinatorStoreError::InvalidRequestId);
        }
        if encoded_len(&command) > MAX_COMMAND_BYTES
            || encoded_len(&event) > MAX_EVENT_BYTES
            || encoded_len(&snapshot) > MAX_SNAPSHOT_BYTES
            || encoded_len(&result) > MAX_RESULT_BYTES
        {
            return Err(CoordinatorStoreError::PayloadTooLarge);
        }

        if let Some(receipt) = self
            .state
            .receipts
            .iter()
            .find(|receipt| receipt.request_id == request_id)
        {
            if receipt.command != command {
                return Err(CoordinatorStoreError::IdempotencyConflict);
            }
            return Ok(CommitOutcome {
                sequence: receipt.sequence,
                result: receipt.result.clone(),
                duplicate: true,
                compaction_warning: None,
            });
        }
        if let Some(reservation) = self
            .state
            .reservations
            .iter()
            .find(|reservation| reservation.request_id == request_id)
        {
            if reservation.command != command {
                return Err(CoordinatorStoreError::IdempotencyConflict);
            }
        }

        let sequence = self.state.sequence.saturating_add(1);
        let record = JournalRecord {
            schema_version: JOURNAL_SCHEMA_VERSION,
            sequence,
            body: JournalBody::Commit {
                request_id: request_id.to_owned(),
                command: command.clone(),
                result: result.clone(),
                event: event.clone(),
                snapshot: snapshot.clone(),
            },
        };
        let mut bytes =
            serde_json::to_vec(&record).map_err(|error| CoordinatorStoreError::Corrupt {
                line: 0,
                message: format!("could not serialize commit: {error}"),
            })?;
        bytes.push(b'\n');
        if let Err(source) = self.file.write_all(&bytes) {
            self.poisoned = true;
            return Err(store_io(&self.path, source));
        }
        if let Err(source) = self.file.sync_data() {
            self.poisoned = true;
            return Err(store_io(&self.path, source));
        }

        self.file_bytes = self.file_bytes.saturating_add(bytes.len());
        self.records_in_file = self.records_in_file.saturating_add(1);
        self.state.sequence = sequence;
        self.state.previous_epoch = extract_epoch(&snapshot);
        self.state.snapshot = Some(snapshot);
        let cursor = event
            .get("cursor")
            .and_then(Value::as_u64)
            .unwrap_or(sequence);
        self.state.events.push_back(DurableEvent {
            cursor,
            value: event,
        });
        trim_events(&mut self.state.events);
        self.state.receipts.push_back(CommandReceipt {
            request_id: request_id.to_owned(),
            command,
            result: result.clone(),
            sequence,
        });
        self.state
            .reservations
            .retain(|reservation| reservation.request_id != request_id);
        trim_receipts(&mut self.state.receipts);
        self.refresh_diagnostics();

        let compaction_warning = if self.file_bytes > MAX_JOURNAL_BYTES
            || self.records_in_file > MAX_RECORDS_BEFORE_COMPACTION
        {
            self.compact().err().map(|error| {
                let message = error.to_string();
                self.state
                    .diagnostics
                    .warnings
                    .push(format!("journal compaction deferred: {message}"));
                while self.state.diagnostics.warnings.len() > MAX_DIAGNOSTIC_WARNINGS {
                    self.state.diagnostics.warnings.remove(0);
                }
                message
            })
        } else {
            None
        };

        Ok(CommitOutcome {
            sequence,
            result,
            duplicate: false,
            compaction_warning,
        })
    }

    fn refresh_diagnostics(&mut self) {
        self.state.diagnostics.journal_schema_version = JOURNAL_SCHEMA_VERSION;
        self.state.diagnostics.records_replayed = self.records_in_file as u64;
        self.state.diagnostics.events_retained = self.state.events.len();
        self.state.diagnostics.idempotency_receipts_retained = self.state.receipts.len();
        self.state.diagnostics.pending_command_reservations = self.state.reservations.len();
    }

    fn compact(&mut self) -> Result<(), CoordinatorStoreError> {
        let snapshot = self.state.snapshot.clone().unwrap_or(Value::Null);
        let record = JournalRecord {
            schema_version: JOURNAL_SCHEMA_VERSION,
            sequence: self.state.sequence,
            body: JournalBody::Checkpoint {
                snapshot,
                events: self.state.events.clone(),
                receipts: self.state.receipts.clone(),
                reservations: self.state.reservations.clone(),
            },
        };
        let mut bytes = serde_json::to_vec(&record).map_err(|error| {
            CoordinatorStoreError::Compaction(format!("serialize checkpoint: {error}"))
        })?;
        bytes.push(b'\n');
        if bytes.len() > MAX_JOURNAL_BYTES {
            return Err(CoordinatorStoreError::Compaction(
                "retained snapshot and receipts exceed journal size limit".to_owned(),
            ));
        }

        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        let temp_path = parent.join(format!(".coordinator-journal-{}.tmp", Uuid::new_v4()));
        let mut temp_options = OpenOptions::new();
        temp_options.create_new(true).write(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            temp_options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
        }
        let mut temp = temp_options
            .open(&temp_path)
            .map_err(|source| store_io(&temp_path, source))?;
        let write_result = temp
            .write_all(&bytes)
            .and_then(|()| temp.sync_all())
            .map_err(|source| store_io(&temp_path, source));
        drop(temp);
        if let Err(error) = write_result {
            let _ = fs::remove_file(&temp_path);
            return Err(error);
        }

        if let Err(source) = replace_file(&temp_path, &self.path) {
            let _ = fs::remove_file(&temp_path);
            return Err(store_io(&self.path, source));
        }
        if let Err(source) = sync_parent_directory(&self.path) {
            self.poisoned = true;
            return Err(store_io(&self.path, source));
        }
        let new_file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .open(&self.path)
            .map_err(|source| {
                self.poisoned = true;
                store_io(&self.path, source)
            })?;
        self.file = new_file;
        self.file_bytes = bytes.len();
        self.records_in_file = 1;
        Ok(())
    }
}

fn recover(
    path: &Path,
    file: &mut File,
) -> Result<(RecoveryState, usize, usize), CoordinatorStoreError> {
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|source| store_io(path, source))?;

    let truncated_incomplete_tail = !bytes.is_empty() && bytes.last() != Some(&b'\n');
    if truncated_incomplete_tail {
        let complete_len = bytes
            .iter()
            .rposition(|byte| *byte == b'\n')
            .map_or(0, |index| index + 1);
        file.set_len(complete_len as u64)
            .map_err(|source| store_io(path, source))?;
        file.sync_all().map_err(|source| store_io(path, source))?;
        bytes.truncate(complete_len);
    }

    let mut state = RecoveryState::default();
    state.diagnostics.truncated_incomplete_tail = truncated_incomplete_tail;
    if truncated_incomplete_tail {
        state.diagnostics.warnings.push(
            "truncated an incomplete trailing journal record; it was not acknowledged as committed"
                .to_owned(),
        );
    }

    let mut previous_sequence = 0_u64;
    let mut saw_checkpoint = false;
    let mut record_count = 0_usize;
    for (index, line) in bytes.split(|byte| *byte == b'\n').enumerate() {
        if line.is_empty() {
            continue;
        }
        let line_number = index + 1;
        let record: JournalRecord =
            serde_json::from_slice(line).map_err(|error| CoordinatorStoreError::Corrupt {
                line: line_number,
                message: error.to_string(),
            })?;
        if record.schema_version != JOURNAL_SCHEMA_VERSION {
            return Err(CoordinatorStoreError::UnsupportedVersion {
                line: line_number,
                version: record.schema_version,
            });
        }
        match record.body {
            JournalBody::Checkpoint {
                snapshot,
                events,
                receipts,
                reservations,
            } => {
                if saw_checkpoint || record_count != 0 {
                    return Err(CoordinatorStoreError::Corrupt {
                        line: line_number,
                        message: "checkpoint is only valid as the first journal record".to_owned(),
                    });
                }
                if encoded_len(&snapshot) > MAX_SNAPSHOT_BYTES {
                    return Err(CoordinatorStoreError::Corrupt {
                        line: line_number,
                        message: "checkpoint snapshot exceeds size limit".to_owned(),
                    });
                }
                state.sequence = record.sequence;
                state.snapshot = Some(snapshot);
                state.events = events;
                state.receipts = receipts;
                state.reservations = reservations;
                trim_events(&mut state.events);
                trim_receipts(&mut state.receipts);
                if state.reservations.len() > MAX_PENDING_RESERVATIONS {
                    return Err(CoordinatorStoreError::Corrupt {
                        line: line_number,
                        message: "checkpoint has too many unresolved reservations".to_owned(),
                    });
                }
                saw_checkpoint = true;
                previous_sequence = record.sequence;
            }
            JournalBody::Commit {
                request_id,
                command,
                result,
                event,
                snapshot,
            } => {
                if record.sequence != previous_sequence.saturating_add(1) {
                    return Err(CoordinatorStoreError::Corrupt {
                        line: line_number,
                        message: format!(
                            "journal sequence gap: expected {}, found {}",
                            previous_sequence.saturating_add(1),
                            record.sequence
                        ),
                    });
                }
                if request_id.is_empty() || request_id.len() > 256 {
                    return Err(CoordinatorStoreError::Corrupt {
                        line: line_number,
                        message: "invalid request ID in committed record".to_owned(),
                    });
                }
                if state
                    .receipts
                    .iter()
                    .any(|receipt| receipt.request_id == request_id)
                {
                    return Err(CoordinatorStoreError::Corrupt {
                        line: line_number,
                        message: format!("duplicate committed request ID {request_id}"),
                    });
                }
                if let Some(reservation) = state
                    .reservations
                    .iter()
                    .find(|reservation| reservation.request_id == request_id)
                {
                    if reservation.command != command {
                        return Err(CoordinatorStoreError::Corrupt {
                            line: line_number,
                            message: "completed command differs from its reservation".to_owned(),
                        });
                    }
                }
                if encoded_len(&snapshot) > MAX_SNAPSHOT_BYTES
                    || encoded_len(&command) > MAX_COMMAND_BYTES
                    || encoded_len(&event) > MAX_EVENT_BYTES
                    || encoded_len(&result) > MAX_RESULT_BYTES
                {
                    return Err(CoordinatorStoreError::Corrupt {
                        line: line_number,
                        message: "committed payload exceeds configured size limit".to_owned(),
                    });
                }
                state.sequence = record.sequence;
                state.snapshot = Some(snapshot);
                state.events.push_back(DurableEvent {
                    cursor: record.sequence,
                    value: event,
                });
                trim_events(&mut state.events);
                state.receipts.push_back(CommandReceipt {
                    request_id: request_id.clone(),
                    command,
                    result,
                    sequence: record.sequence,
                });
                state
                    .reservations
                    .retain(|reservation| reservation.request_id != request_id);
                trim_receipts(&mut state.receipts);
                previous_sequence = record.sequence;
            }
            JournalBody::Reservation {
                request_id,
                command,
            } => {
                if record.sequence != previous_sequence.saturating_add(1) {
                    return Err(CoordinatorStoreError::Corrupt {
                        line: line_number,
                        message: format!(
                            "journal sequence gap: expected {}, found {}",
                            previous_sequence.saturating_add(1),
                            record.sequence
                        ),
                    });
                }
                if request_id.is_empty()
                    || request_id.len() > 256
                    || encoded_len(&command) > MAX_COMMAND_BYTES
                    || state
                        .receipts
                        .iter()
                        .any(|receipt| receipt.request_id == request_id)
                    || state
                        .reservations
                        .iter()
                        .any(|reservation| reservation.request_id == request_id)
                {
                    return Err(CoordinatorStoreError::Corrupt {
                        line: line_number,
                        message: "invalid or duplicate command reservation".to_owned(),
                    });
                }
                state.reservations.push_back(CommandReservation {
                    request_id,
                    command,
                    reserved_sequence: record.sequence,
                });
                if state.reservations.len() > MAX_PENDING_RESERVATIONS {
                    return Err(CoordinatorStoreError::Corrupt {
                        line: line_number,
                        message: "too many unresolved command reservations".to_owned(),
                    });
                }
                state.sequence = record.sequence;
                previous_sequence = record.sequence;
            }
            JournalBody::CommandCompleted {
                request_id,
                command,
                result,
            } => {
                if record.sequence != previous_sequence.saturating_add(1) {
                    return Err(CoordinatorStoreError::Corrupt {
                        line: line_number,
                        message: format!(
                            "journal sequence gap: expected {}, found {}",
                            previous_sequence.saturating_add(1),
                            record.sequence
                        ),
                    });
                }
                let Some(reservation) = state
                    .reservations
                    .iter()
                    .find(|reservation| reservation.request_id == request_id)
                else {
                    return Err(CoordinatorStoreError::Corrupt {
                        line: line_number,
                        message: "completion receipt has no matching reservation".to_owned(),
                    });
                };
                if reservation.command != command
                    || encoded_len(&command) > MAX_COMMAND_BYTES
                    || encoded_len(&result) > MAX_RESULT_BYTES
                {
                    return Err(CoordinatorStoreError::Corrupt {
                        line: line_number,
                        message: "completion receipt does not match its reservation".to_owned(),
                    });
                }
                state
                    .reservations
                    .retain(|reservation| reservation.request_id != request_id);
                state.receipts.push_back(CommandReceipt {
                    request_id,
                    command,
                    result,
                    sequence: record.sequence,
                });
                trim_receipts(&mut state.receipts);
                state.sequence = record.sequence;
                previous_sequence = record.sequence;
            }
        }
        record_count += 1;
    }

    let file_bytes = bytes.len();
    state.diagnostics.records_replayed = record_count as u64;
    state.diagnostics.events_retained = state.events.len();
    state.diagnostics.idempotency_receipts_retained = state.receipts.len();
    state.diagnostics.pending_command_reservations = state.reservations.len();
    state.recovered_attempts = classify_snapshot_attempts(state.snapshot.as_ref());
    state.previous_epoch = state.snapshot.as_ref().and_then(extract_epoch);
    Ok((state, file_bytes, record_count))
}

fn classify_snapshot_attempts(snapshot: Option<&Value>) -> Vec<RecoveredAttempt> {
    snapshot
        .and_then(|value| value.get("attempts"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|attempt| {
            let attempt_id = attempt.get("attemptId")?.as_str()?.to_owned();
            let generation = attempt
                .get("generation")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            let status = attempt
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let (classification, reason) =
                classify_attempt_restart(status, FrameworkSessionEvidence::NotChecked);
            Some(RecoveredAttempt {
                attempt_id,
                generation,
                classification,
                reason: reason.to_owned(),
            })
        })
        .collect()
}

fn trim_events(events: &mut VecDeque<DurableEvent>) {
    while events.len() > MAX_RETAINED_EVENTS
        || events.iter().map(|event| encoded_len(event)).sum::<usize>() > MAX_RETAINED_EVENT_BYTES
    {
        if events.pop_front().is_none() {
            break;
        }
    }
}

fn trim_receipts(receipts: &mut VecDeque<CommandReceipt>) {
    while receipts.len() > MAX_IDEMPOTENCY_RECEIPTS
        || receipts
            .iter()
            .map(|receipt| encoded_len(receipt))
            .sum::<usize>()
            > MAX_IDEMPOTENCY_BYTES
    {
        if receipts.pop_front().is_none() {
            break;
        }
    }
}

fn encoded_len(value: &impl Serialize) -> usize {
    serde_json::to_vec(value).map_or(usize::MAX, |bytes| bytes.len())
}

fn extract_epoch(snapshot: &Value) -> Option<String> {
    snapshot
        .get("epoch")
        .or_else(|| snapshot.get("managerId"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
}

fn store_io(path: &Path, source: io::Error) -> CoordinatorStoreError {
    CoordinatorStoreError::Io {
        path: path.to_path_buf(),
        source,
    }
}

fn validate_private_file(path: &Path, file: &File) -> Result<(), CoordinatorStoreError> {
    let metadata = file.metadata().map_err(|source| store_io(path, source))?;
    if !metadata.file_type().is_file() {
        return Err(store_io(
            path,
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "coordinator journal must be a regular file",
            ),
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let mode = metadata.permissions().mode();
        let current_uid = unsafe { libc::geteuid() };
        if metadata.uid() != current_uid || mode & 0o077 != 0 {
            return Err(store_io(
                path,
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "coordinator journal must be current-user owned with no group/world permissions",
                ),
            ));
        }
    }
    Ok(())
}

fn set_private_permissions(path: &Path, file: &File) -> Result<(), CoordinatorStoreError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|source| store_io(path, source))?;
    }
    let _ = (path, file);
    Ok(())
}

fn replace_file(source: &Path, destination: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        fs::rename(source, destination)
    }
    #[cfg(not(unix))]
    {
        if destination.exists() {
            fs::remove_file(destination)?;
        }
        fs::rename(source, destination)
    }
}

fn sync_parent_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        File::open(parent)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

#[cfg(test)]
mod reservation_tests {
    use super::*;

    #[test]
    fn unresolved_launch_reservation_never_becomes_a_new_dispatch_after_restart() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("coordinator-v1.jsonl");
        let fingerprint = serde_json::json!({"type":"start","command":"echo hi"});
        let (mut store, _) = CoordinatorStore::open(&path).expect("open store");

        assert!(matches!(
            store.reserve_command("start-1", fingerprint.clone()),
            Ok(ReservationOutcome::New { .. })
        ));
        drop(store);

        let (mut store, recovered) = CoordinatorStore::open(&path).expect("recover store");
        assert_eq!(recovered.reservations.len(), 1);
        assert!(matches!(
            store.reserve_command("start-1", fingerprint.clone()),
            Ok(ReservationOutcome::ReservedUnknown { .. })
        ));
        store
            .complete_reservation("start-1", serde_json::json!({"processId":"process-1"}))
            .expect("complete reservation");
        drop(store);

        let (mut store, recovered) = CoordinatorStore::open(&path).expect("recover completion");
        assert!(recovered.reservations.is_empty());
        match store
            .reserve_command("start-1", fingerprint)
            .expect("deduplicate")
        {
            ReservationOutcome::Completed { result, .. } => {
                assert_eq!(result["processId"], "process-1");
            }
            other => panic!("expected completed result, got {other:?}"),
        }
    }
}
