use super::*;
use serial_protocol::{MacroExecutionInfo, MacroRunSpec};

pub(crate) enum MacroAction {
    Start {
        daemon_epoch: Uuid,
        generation: u64,
        operation_id: Uuid,
        expected_run_id: Uuid,
        run_token: Uuid,
        sequence_precondition: SequenceWritePrecondition,
        spec: MacroRunSpec,
    },
    Status {
        execution_id: Uuid,
    },
    Cancel {
        execution_id: Uuid,
        expected_run_id: Uuid,
        run_token: Uuid,
    },
}

impl SessionState {
    pub(super) async fn macro_request(
        &mut self,
        port: String,
        action: MacroAction,
    ) -> Result<MacroExecutionInfo> {
        match action {
            MacroAction::Start {
                daemon_epoch,
                generation,
                operation_id,
                expected_run_id,
                run_token,
                sequence_precondition,
                spec,
            } => {
                let lease = self
                    .renew_owned_run_control(&port, expected_run_id, run_token)
                    .await?;
                let request_id = Uuid::new_v4();
                // Exactly one start is sent. A lost acknowledgement never replays a script.
                let request = ClientMessage::MacroStart {
                    request_id,
                    port,
                    control_id: lease.id,
                    fence: lease.fence,
                    daemon_epoch,
                    generation,
                    operation_id,
                    expected_run_id: Some(expected_run_id),
                    sequence_precondition: Some(sequence_precondition),
                    spec,
                };
                match self.call(request).await {
                    Ok(CommandResult::MacroStarted { execution }) => Ok(*execution),
                    Ok(other) => bail!("unexpected macro-start result: {other:?}"),
                    Err(error) if is_user_read_required(&error) => {
                        Err(anyhow::Error::new(UserCommandUsed {
                            message: error.to_string(),
                        }))
                    }
                    Err(error) if is_sequence_boundary_rejection(&error) => {
                        Err(anyhow::Error::new(SequenceBoundaryRejected {
                            message: error.to_string(),
                        }))
                    }
                    Err(error) if daemon_reports_write_outcome_uncertain(&error) => {
                        Err(physical_write_outcome_uncertain(
                            "Macro start",
                            request_id,
                            operation_id,
                            error,
                        ))
                    }
                    Err(error) if is_control_loss_rejection(&error) => {
                        self.disconnect();
                        bail!(
                            "human_takeover_or_control_revoked: Macro was rejected before starting; run_id={expected_run_id}; no_bytes_written=true: {error}"
                        )
                    }
                    Err(error) if error.downcast_ref::<DaemonRequestError>().is_some() => {
                        Err(error
                            .context("seriald rejected macro_start before accepting an execution"))
                    }
                    Err(error) => Err(physical_write_outcome_uncertain(
                        "Macro start (never retry automatically)",
                        request_id,
                        operation_id,
                        error,
                    )),
                }
            }
            MacroAction::Status { execution_id } => {
                let request = || ClientMessage::MacroStatus {
                    request_id: Uuid::new_v4(),
                    port: port.clone(),
                    execution_id,
                };
                // Only read-only status may reconnect/retry; the daemon identity guard
                // remains active. This also observes terminal state after Human takeover.
                let result = match self.call(request()).await {
                    Err(error) if is_transport_error(&error) || is_timeout_error(&error) => {
                        self.call(request()).await?
                    }
                    result => result?,
                };
                match result {
                    CommandResult::MacroStatus { execution } => Ok(*execution),
                    other => bail!("unexpected macro-status result: {other:?}"),
                }
            }
            MacroAction::Cancel {
                execution_id,
                expected_run_id,
                run_token,
            } => {
                let lease = self
                    .renew_owned_run_control(&port, expected_run_id, run_token)
                    .await?;
                let request = ClientMessage::MacroCancel {
                    request_id: Uuid::new_v4(),
                    port,
                    control_id: lease.id,
                    fence: lease.fence,
                    execution_id,
                };
                match self.call(request).await? {
                    CommandResult::MacroCancelled { execution } => Ok(*execution),
                    other => bail!("unexpected macro-cancel result: {other:?}"),
                }
            }
        }
    }
}
