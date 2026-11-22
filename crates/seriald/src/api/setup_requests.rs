use super::{ApiError, AppState};
use axum::{
    Json,
    extract::{Path, State},
};
use serial_protocol::{SetupBrief, SetupCheckKind, SetupPlan, SetupRequest};
use std::time::{Duration, Instant};
use uuid::Uuid;

const LIFETIME: Duration = Duration::from_secs(24 * 60 * 60);
fn line(value: &str, limit: usize) -> bool {
    value.len() <= limit && !value.chars().any(char::is_control)
}

pub(super) async fn create(
    State(state): State<AppState>,
    Json(brief): Json<SetupBrief>,
) -> Result<Json<SetupRequest>, ApiError> {
    if brief.port.trim().is_empty()
        || !line(&brief.port, 512)
        || !line(&brief.prompt_hint, 512)
        || brief.intent.len() > 2048
    {
        return Err(ApiError::BadRequest("invalid first-use answers".into()));
    }
    let mut requests = state.inner.setup_requests.lock().await;
    requests.retain(|_, (created, _)| created.elapsed() < LIFETIME);
    if requests.len() >= 128 {
        return Err(ApiError::BadRequest(
            "too many pending setup requests".into(),
        ));
    }
    let task = SetupRequest {
        id: Uuid::new_v4(),
        brief,
        plan: None,
        agent: None,
    };
    requests.insert(task.id, (Instant::now(), task.clone()));
    Ok(Json(task))
}

fn missing() -> ApiError {
    ApiError::NotFound("配置任务已过期，请在控制台重新生成任务。".into())
}

pub(super) async fn read(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<SetupRequest>, ApiError> {
    let requests = state.inner.setup_requests.lock().await;
    let (_, task) = requests
        .get(&id)
        .filter(|(created, _)| created.elapsed() < LIFETIME)
        .ok_or_else(missing)?;
    Ok(Json(task.clone()))
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Submission {
    agent: String,
    plan: SetupPlan,
}

pub(super) async fn propose(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
    Json(answer): Json<Submission>,
) -> Result<Json<SetupRequest>, ApiError> {
    let mut requests = state.inner.setup_requests.lock().await;
    let (_, task) = requests
        .get_mut(&id)
        .filter(|(created, _)| created.elapsed() < LIFETIME)
        .ok_or_else(missing)?;
    let plan = &answer.plan;
    let valid_check = match task.brief.check {
        SetupCheckKind::Heartbeat => plan.command.is_empty() && plan.expected_reply.is_empty(),
        SetupCheckKind::Status => {
            !plan.command.trim().is_empty()
                && !plan.expected_reply.trim().is_empty()
                && !plan.command.contains(&plan.expected_reply)
                && !plan.shell_prompt.contains(&plan.expected_reply)
        }
    };
    if !valid_check
        || plan.shell_prompt.trim().is_empty()
        || !line(&plan.shell_prompt, 512)
        || !line(&plan.command, 512)
        || !line(&plan.expected_reply, 512)
        || answer.agent.trim().is_empty()
        || !line(&answer.agent, 128)
    {
        return Err(ApiError::BadRequest("provide a literal Shell prompt and a check matching the requested kind; a status reply must be distinguishable from the command echo and the Shell prompt".into()));
    }
    if let Some(existing) = &task.plan {
        if existing != plan || task.agent.as_deref() != Some(&answer.agent) {
            return Err(ApiError::BadRequest(
                "a different proposal was already submitted".into(),
            ));
        }
    } else {
        task.plan = Some(answer.plan);
        task.agent = Some(answer.agent);
    }
    Ok(Json(task.clone()))
}
