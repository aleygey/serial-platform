//! On-demand, cancellable replay of retained server evidence into a disk
//! search projection. No event-window cap and no in-memory list of matches.
use crate::{
    api::ApiClient,
    display::{StreamDisplayBatch, TerminalStreamParser, gap_line},
    session_search::SessionArchive,
};
use serial_protocol::{ArchiveSummary, EventQuery};
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use uuid::Uuid;

#[derive(Debug, Clone, Default)]
pub struct Progress {
    pub loading: bool,
    pub events: u64,
    pub archives_done: usize,
    pub archives_total: usize,
    pub error: Option<String>,
}

#[derive(Debug)]
pub struct RetainedHistory {
    cancel: Arc<AtomicBool>,
    progress: Arc<Mutex<Progress>>,
}
impl Drop for RetainedHistory {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Release);
    }
}
impl RetainedHistory {
    pub fn progress(&self) -> Progress {
        self.progress
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn start(api: ApiClient, port: String, archive: SessionArchive) -> Self {
        let cancel = Arc::new(AtomicBool::new(false));
        let progress = Arc::new(Mutex::new(Progress {
            loading: true,
            ..Default::default()
        }));
        let result = Self {
            cancel: cancel.clone(),
            progress: progress.clone(),
        };
        tokio::spawn(async move {
            let work = replay(&api, &port, &archive, &cancel, &progress);
            // Dropping an HTTP future is safe here: this worker is read-only.
            tokio::pin!(work);
            let outcome = loop {
                tokio::select! {
                    result = &mut work => break Some(result),
                    _ = tokio::time::sleep(Duration::from_millis(100)) => {
                        if cancel.load(Ordering::Acquire) { break None; }
                    }
                }
            };
            if let Some(Err(error)) = outcome {
                let mut state = progress.lock().unwrap_or_else(|e| e.into_inner());
                state.loading = false;
                state.error = Some(error.to_string());
            }
        });
        result
    }
}

fn page_cursor(
    previous: u64,
    through: u64,
    truncated: bool,
    next: Option<&serial_protocol::Cursor>,
    epoch: Uuid,
) -> anyhow::Result<Option<u64>> {
    if !truncated {
        return Ok(None);
    }
    let cursor = next.ok_or_else(|| anyhow::anyhow!("历史查询缺少续读位置，未完成全量搜索"))?;
    anyhow::ensure!(
        cursor.epoch == epoch && cursor.after_seq > previous && cursor.after_seq <= through,
        "历史查询续读位置异常，未完成全量搜索"
    );
    Ok((cursor.after_seq < through).then_some(cursor.after_seq))
}

async fn replay(
    api: &ApiClient,
    port: &str,
    archive: &SessionArchive,
    cancel: &Arc<AtomicBool>,
    progress: &Arc<Mutex<Progress>>,
) -> anyhow::Result<()> {
    let server_id = api.health().await?.server_id;
    let catalog = api.archives(Some(port)).await?;
    anyhow::ensure!(
        !catalog.truncated,
        "服务端历史目录不完整，不能确认已搜索全部历史"
    );
    let mut archives = catalog.archives;
    anyhow::ensure!(
        archives.iter().all(|range| range.port == port),
        "历史目录端口不一致"
    );
    archives.reverse(); // catalog is newest first; preserve cross-epoch order
    progress
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .archives_total = archives.len();
    let mut heads = HashMap::<Uuid, u64>::new();
    let mut parser = TerminalStreamParser::new();
    // Match the normal output pane, including its exact echo reconciliation.
    parser.set_echo_reconciliation(true);
    for (index, range) in archives.iter().enumerate() {
        read_range(
            api,
            archive,
            cancel,
            progress,
            &mut parser,
            range,
            range.first_seq.saturating_sub(1),
        )
        .await?;
        heads.insert(range.epoch, range.last_seq);
        progress
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .archives_done = index + 1;
    }
    progress.lock().unwrap_or_else(|e| e.into_inner()).loading = false;

    // Refresh only the new suffix. Long sessions never rescan the old prefix.
    loop {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let status = api.status().await?;
        anyhow::ensure!(
            status.server_id == server_id,
            "服务端身份已变化，请重新打开历史搜索"
        );
        let slot = status
            .ports
            .into_iter()
            .find(|slot| slot.config.port == port)
            .ok_or_else(|| anyhow::anyhow!("串口已移除；当前搜索仅包含已加载历史"))?;
        let after = heads.get(&slot.daemon_epoch).copied().unwrap_or(0);
        if slot.head_seq <= after {
            continue;
        }
        // On a daemon restart reload the catalog, including any final suffix
        // sealed between the previous poll and restart. Never silently skip it.
        let catalog = api.archives(Some(port)).await?;
        anyhow::ensure!(!catalog.truncated, "历史目录已达到上限，搜索覆盖不完整");
        let mut ranges = catalog.archives;
        anyhow::ensure!(
            ranges.iter().all(|range| range.port == port),
            "历史目录端口不一致"
        );
        ranges.reverse();
        for range in ranges {
            let after = heads
                .get(&range.epoch)
                .copied()
                .unwrap_or(range.first_seq.saturating_sub(1));
            if range.last_seq <= after {
                continue;
            }
            read_range(api, archive, cancel, progress, &mut parser, &range, after).await?;
            heads.insert(range.epoch, range.last_seq);
        }
    }
}

async fn read_range(
    api: &ApiClient,
    archive: &SessionArchive,
    cancel: &Arc<AtomicBool>,
    progress: &Arc<Mutex<Progress>>,
    parser: &mut TerminalStreamParser,
    range: &ArchiveSummary,
    mut after: u64,
) -> anyhow::Result<()> {
    let port = range.port.as_str();
    loop {
        anyhow::ensure!(!cancel.load(Ordering::Acquire), "历史加载已取消");
        let response = api
            .events(
                port,
                &EventQuery {
                    epoch: Some(range.epoch),
                    after_seq: Some(after),
                    through_seq: Some(range.last_seq),
                    before_wall_time_ns: None,
                    after_wall_time_ns: None,
                    direction: None,
                    kind: None,
                    actor_id: None,
                    run_id: None,
                    operation_id: None,
                    contains: None,
                    regex: None,
                    limit_events: Some(1000),
                    limit_bytes: Some(1024 * 1024),
                },
            )
            .await?;
        let next = page_cursor(
            after,
            range.last_seq,
            response.truncated,
            response.next_cursor.as_ref(),
            range.epoch,
        )?;
        let mut previous = after;
        for event in &response.events {
            anyhow::ensure!(
                event.port == port
                    && event.daemon_epoch == range.epoch
                    && event.seq > previous
                    && event.seq <= range.last_seq,
                "服务端返回的历史范围不一致，已停止索引"
            );
            previous = event.seq;
        }
        if !response.truncated
            && previous < range.last_seq
            && !response
                .gaps
                .iter()
                .any(|gap| gap.last_seq >= range.last_seq)
        {
            anyhow::bail!("历史读取未覆盖请求范围，不能确认已搜索全部历史");
        }
        // Parsing and spool backpressure belong off the async/UI executor.
        let mut owned_parser = std::mem::take(parser);
        let archive = archive.clone();
        let cancel = cancel.clone();
        let count = response.events.len() as u64;
        *parser = tokio::task::spawn_blocking(move || -> anyhow::Result<TerminalStreamParser> {
            for gap in response.gaps {
                archive.note_coverage_gap(format!(
                    "{} · {}–{} · {:?}",
                    gap.epoch, gap.first_seq, gap.last_seq, gap.reason
                ));
            }
            let mut previous = after;
            for event in response.events {
                if event.seq != previous.saturating_add(1) {
                    archive.note_coverage_gap(format!(
                        "历史序号 {}–{} 不连续",
                        previous + 1,
                        event.seq - 1
                    ));
                    let mut completed = owned_parser.flush();
                    completed.push(gap_line(event.seq, "历史日志存在缺口"));
                    owned_parser.reset();
                    archive.append_history(
                        StreamDisplayBatch {
                            completed,
                            pending: None,
                            pending_committed: true,
                        },
                        &cancel,
                    )?;
                }
                archive.append_history(owned_parser.push_event(&event), &cancel)?;
                previous = event.seq;
            }
            Ok(owned_parser)
        })
        .await??;
        progress.lock().unwrap_or_else(|e| e.into_inner()).events += count;
        let Some(cursor) = next else {
            return Ok(());
        };
        after = cursor;
        // Yield between bounded requests so serial operations retain priority.
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_protocol::{Direction, EventKind, EventQueryResponse, TimelineEvent};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[test]
    fn paging_cannot_loop_or_cross_epochs_or_hide_incomplete_coverage() {
        let epoch = Uuid::new_v4();
        assert!(page_cursor(10, 100, true, None, epoch).is_err());
        assert!(
            page_cursor(
                10,
                100,
                true,
                Some(&serial_protocol::Cursor {
                    epoch,
                    after_seq: 10
                }),
                epoch
            )
            .is_err()
        );
        assert!(
            page_cursor(
                10,
                100,
                true,
                Some(&serial_protocol::Cursor {
                    epoch: Uuid::new_v4(),
                    after_seq: 20
                }),
                epoch
            )
            .is_err()
        );
        assert_eq!(
            page_cursor(
                10,
                100,
                true,
                Some(&serial_protocol::Cursor {
                    epoch,
                    after_seq: 20
                }),
                epoch
            )
            .unwrap(),
            Some(20)
        );
        assert_eq!(
            page_cursor(
                10,
                100,
                true,
                Some(&serial_protocol::Cursor {
                    epoch,
                    after_seq: 100
                }),
                epoch
            )
            .unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn retained_replay_reads_every_page_across_epochs_and_keeps_exact_context() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let api = ApiClient::new(format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let epochs = [Uuid::new_v4(), Uuid::new_v4()];
        let server = tokio::spawn(async move {
            let mut requests = Vec::new();
            for epoch in epochs {
                for page in 0..3 {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut request = Vec::new();
                    loop {
                        let mut bytes = [0; 4096];
                        let count = socket.read(&mut bytes).await.unwrap();
                        assert!(count > 0);
                        request.extend_from_slice(&bytes[..count]);
                        if request.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                            break;
                        }
                    }
                    let request = String::from_utf8(request).unwrap();
                    assert!(request.contains(&epoch.to_string()));
                    assert!(request.contains(&format!("after_seq={}", page * 1000)));
                    requests.push(request);
                    let end = ((page + 1) * 1000).min(2501);
                    let events = (page * 1000 + 1..=end)
                        .map(|seq| TimelineEvent {
                            port: "COM3".into(),
                            daemon_epoch: epoch,
                            seq,
                            generation: 1,
                            wall_time_ns: seq as i64,
                            monotonic_time_ns: seq,
                            kind: EventKind::Rx,
                            direction: Direction::Rx,
                            actor: None,
                            run_id: None,
                            operation_id: None,
                            stream_offset_start: None,
                            stream_offset_end: None,
                            data: format!("retained-row-{seq}\r\n").into_bytes(),
                            metadata: Default::default(),
                            durable: true,
                        })
                        .collect();
                    let body = serde_json::to_vec(&EventQueryResponse {
                        events,
                        next_cursor: Some(serial_protocol::Cursor {
                            epoch,
                            after_seq: end,
                        }),
                        truncated: end < 2501,
                        first_available_seq: Some(1),
                        gaps: vec![],
                    })
                    .unwrap();
                    let header = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        body.len()
                    );
                    socket.write_all(header.as_bytes()).await.unwrap();
                    socket.write_all(&body).await.unwrap();
                }
            }
            requests.len()
        });
        let archive = SessionArchive::new().unwrap();
        let cancel = Arc::new(AtomicBool::new(false));
        let progress = Arc::new(Mutex::new(Progress::default()));
        let mut parser = TerminalStreamParser::new();
        for epoch in epochs {
            let range = ArchiveSummary {
                port: "COM3".into(),
                epoch,
                first_seq: 1,
                last_seq: 2501,
                first_segment_wall_time_ns: 1,
                last_segment_wall_time_ns: 2,
                segment_count: 1,
                total_bytes: 100,
                has_open_segment: false,
            };
            read_range(&api, &archive, &cancel, &progress, &mut parser, &range, 0)
                .await
                .unwrap();
        }
        assert_eq!(server.await.unwrap(), 6);
        assert_eq!(progress.lock().unwrap().events, 5002);
        let search = archive
            .search(crate::session_search::SearchQuery {
                query: "retained-row-1".into(),
                case_sensitive: true,
            })
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while !search.progress().complete {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(search.progress().gaps.is_empty());
        let first = search.page(0, 1).unwrap().remove(0);
        let context = search.context(&first, 0, 2).unwrap();
        assert_eq!(context[0].line.daemon_epoch, Some(epochs[0]));
        assert_eq!(context[0].line.seq, 1);
        assert!(context[0].line.text.contains("retained-row-1"));
        assert!(search.progress().matches > 2000);
    }
}
