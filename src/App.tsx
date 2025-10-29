import { useEffect, useMemo, useReducer, useRef, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import "./App.css";

type TaskStatus = "queued" | "downloading" | "paused" | "completed" | "error";
type SegmentStatus = "pending" | "downloading" | "completed" | "failed" | "retrying";
type EncryptionStatus = "unknown" | "fetching" | "ready" | "failed";

interface SegmentSnapshot {
  seq: number;
  status: SegmentStatus;
  speed_bps?: number | null;
  size_bytes?: number | null;
  retries: number;
  last_updated?: string | null;
}

interface TaskLog {
  timestamp: string;
  message: string;
}

interface EncryptionSnapshot {
  method?: string | null;
  key_uri?: string | null;
  status: EncryptionStatus;
  iv?: string | null;
  last_error?: string | null;
}

interface TaskSnapshot {
  id: string;
  title: string;
  playlist_url: string;
  quality?: string | null;
  resolution?: string | null;
  audio?: string | null;
  playlist_type: string;
  save_dir: string;
  total_segments: number;
  downloaded_segments: number;
  progress: number;
  speed_bps: number;
  status: TaskStatus;
  eta_seconds?: number | null;
  created_at: string;
  last_event?: string | null;
  average_segment_size_bytes?: number | null;
  retries: number;
  encryption?: EncryptionSnapshot | null;
  segments: SegmentSnapshot[];
  logs: TaskLog[];
  output_path?: string | null;
}

type FrontendEvent =
  | { type: "snapshot"; tasks: TaskSnapshot[] }
  | { type: "taskCreated"; task: TaskSnapshot }
  | { type: "taskUpdated"; task: TaskSnapshot }
  | { type: "taskCompleted"; task_id: string; task: TaskSnapshot }
  | { type: "taskStatus"; task_id: string; status: TaskStatus; message?: string | null }
  | { type: "segmentUpdated"; task_id: string; segment: SegmentSnapshot }
  | { type: "logAppended"; task_id: string; log: TaskLog }
  | { type: "keyStatus"; task_id: string; encryption: EncryptionSnapshot }
  | { type: "taskError"; task_id: string; message: string };

interface AppState {
  tasks: Record<string, TaskSnapshot>;
  order: string[];
  activeTaskId: string | null;
  selected: Set<string>;
}

type Action =
  | { type: "snapshot"; tasks: TaskSnapshot[] }
  | { type: "taskCreated"; task: TaskSnapshot }
  | { type: "taskUpdated"; task: TaskSnapshot }
  | { type: "taskCompleted"; task: TaskSnapshot }
  | { type: "taskStatus"; task_id: string; status: TaskStatus; message?: string | null }
  | { type: "segmentUpdated"; task_id: string; segment: SegmentSnapshot }
  | { type: "logAppended"; task_id: string; log: TaskLog }
  | { type: "keyStatus"; task_id: string; encryption: EncryptionSnapshot }
  | { type: "taskError"; task_id: string; message: string }
  | { type: "setActive"; task_id: string }
  | { type: "toggleSelect"; task_id: string }
  | { type: "clearSelection" };

const initialState: AppState = {
  tasks: {},
  order: [],
  activeTaskId: null,
  selected: new Set(),
};

function reducer(state: AppState, action: Action): AppState {
  switch (action.type) {
    case "snapshot": {
      const tasks: Record<string, TaskSnapshot> = {};
      action.tasks.forEach((task) => {
        tasks[task.id] = task;
      });
      const order = action.tasks.map((task) => task.id);
      return {
        tasks,
        order,
        activeTaskId: state.activeTaskId ?? order[0] ?? null,
        selected: new Set(),
      };
    }
    case "taskCreated": {
      const order = state.order.includes(action.task.id)
        ? state.order
        : [action.task.id, ...state.order];
      return {
        ...state,
        tasks: {
          ...state.tasks,
          [action.task.id]: action.task,
        },
        order,
        activeTaskId: state.activeTaskId ?? action.task.id,
        selected: new Set(state.selected),
      };
    }
    case "taskUpdated":
    case "taskCompleted": {
      if (!state.tasks[action.task.id]) {
        return reducer(state, { type: "taskCreated", task: action.task });
      }
      return {
        ...state,
        tasks: {
          ...state.tasks,
          [action.task.id]: action.task,
        },
        selected: new Set(state.selected),
      };
    }
    case "taskStatus": {
      const task = state.tasks[action.task_id];
      if (!task) return state;
      return {
        ...state,
        tasks: {
          ...state.tasks,
          [action.task_id]: {
            ...task,
            status: action.status,
            last_event: action.message ?? task.last_event,
          },
        },
        selected: new Set(state.selected),
      };
    }
    case "segmentUpdated": {
      const task = state.tasks[action.task_id];
      if (!task) return state;
      const segments = [...task.segments];
      const idx = segments.findIndex((seg) => seg.seq === action.segment.seq);
      if (idx >= 0) {
        segments[idx] = action.segment;
      } else {
        segments.push(action.segment);
      }
      return {
        ...state,
        tasks: {
          ...state.tasks,
          [action.task_id]: {
            ...task,
            segments,
          },
        },
      };
    }
    case "logAppended": {
      const task = state.tasks[action.task_id];
      if (!task) return state;
      const logs = [...task.logs, action.log].slice(-100);
      return {
        ...state,
        tasks: {
          ...state.tasks,
          [action.task_id]: {
            ...task,
            logs,
            last_event: action.log.message,
          },
        },
        selected: new Set(state.selected),
      };
    }
    case "keyStatus": {
      const task = state.tasks[action.task_id];
      if (!task) return state;
      return {
        ...state,
        tasks: {
          ...state.tasks,
          [action.task_id]: {
            ...task,
            encryption: action.encryption,
          },
        },
      };
    }
    case "taskError": {
      const task = state.tasks[action.task_id];
      if (!task) return state;
      return {
        ...state,
        tasks: {
          ...state.tasks,
          [action.task_id]: {
            ...task,
            status: "error",
            last_event: action.message,
          },
        },
        selected: new Set(state.selected),
      };
    }
    case "setActive":
      return { ...state, activeTaskId: action.task_id };
    case "toggleSelect": {
      const selected = new Set(state.selected);
      if (selected.has(action.task_id)) {
        selected.delete(action.task_id);
      } else {
        selected.add(action.task_id);
      }
      return { ...state, selected };
    }
    case "clearSelection":
      return { ...state, selected: new Set() };
    default:
      return state;
  }
}

function App() {
  const [state, dispatch] = useReducer(reducer, initialState);
  const [wsStatus, setWsStatus] = useState<"connecting" | "connected" | "error">("connecting");
  const wsRef = useRef<WebSocket | null>(null);
  const [form, setForm] = useState({
    title: "",
    playlistUrl: "",
    saveDir: "",
    referer: "",
    headers: "",
    cookies: "",
    manualKey: "",
    manualIv: "",
    threads: "6",
  });
  const [formBusy, setFormBusy] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let mounted = true;
    invoke<TaskSnapshot[]>("list_tasks")
      .then((tasks) => {
        if (mounted && tasks.length > 0) {
          dispatch({ type: "snapshot", tasks });
        }
      })
      .catch((err) => {
        console.error(err);
      });
    return () => {
      mounted = false;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const endpoint = await invoke<string>("websocket_endpoint");
        if (cancelled) return;
        const socket = new WebSocket(endpoint);
        wsRef.current = socket;

        socket.onopen = () => setWsStatus("connected");
        socket.onerror = () => setWsStatus("error");
        socket.onclose = () => setWsStatus((status) => (status === "connected" ? "error" : status));
        socket.onmessage = (event) => {
          try {
            const payload: FrontendEvent = JSON.parse(event.data);
            handleEvent(payload);
          } catch (err) {
            console.error("Failed to parse websocket event", err);
          }
        };
      } catch (err) {
        console.error(err);
        if (!cancelled) setWsStatus("error");
      }
    })();
    return () => {
      cancelled = true;
      wsRef.current?.close();
      wsRef.current = null;
    };
  }, []);

  const handleEvent = (event: FrontendEvent) => {
    switch (event.type) {
      case "snapshot":
        dispatch({ type: "snapshot", tasks: event.tasks });
        break;
      case "taskCreated":
        dispatch({ type: "taskCreated", task: event.task });
        break;
      case "taskUpdated":
        dispatch({ type: "taskUpdated", task: event.task });
        break;
      case "taskCompleted":
        dispatch({ type: "taskCompleted", task: event.task });
        break;
      case "taskStatus":
        dispatch({
          type: "taskStatus",
          task_id: event.task_id,
          status: event.status,
          message: event.message,
        });
        break;
      case "segmentUpdated":
        dispatch({
          type: "segmentUpdated",
          task_id: event.task_id,
          segment: event.segment,
        });
        break;
      case "logAppended":
        dispatch({
          type: "logAppended",
          task_id: event.task_id,
          log: event.log,
        });
        break;
      case "keyStatus":
        dispatch({
          type: "keyStatus",
          task_id: event.task_id,
          encryption: event.encryption,
        });
        break;
      case "taskError":
        dispatch({
          type: "taskError",
          task_id: event.task_id,
          message: event.message,
        });
        break;
      default:
        break;
    }
  };

  const selectedIds = useMemo(() => Array.from(state.selected), [state.selected]);
  const hasSelection = selectedIds.length > 0;
  const summary = useMemo(() => computeSummary(Object.values(state.tasks)), [state.tasks]);

  const handlePauseSelected = async (ids: string[]) => {
    if (ids.length === 0) return;
    try {
      await invoke("pause_task", { taskIds: ids });
    } catch (err) {
      console.error(err);
    }
  };
  const statusLabel = wsStatus === "connected" ? "WebSocket 已连接" : wsStatus === "connecting" ? "WebSocket 连接中" : "WebSocket 异常";
  const statusDotClass = wsStatus === "connected" ? "status-online" : wsStatus === "connecting" ? "status-pending" : "status-offline";

  const handleInputChange = (field: keyof typeof form, value: string) => {
    setForm((prev) => ({ ...prev, [field]: value }));
  };

  const handleSubmit = async () => {
    if (!form.playlistUrl.trim()) {
      setError("请提供 m3u8 链接");
      return;
    }
    setError(null);
    setFormBusy(true);
    try {
      await invoke("create_task", {
        payload: {
          playlist_url: form.playlistUrl.trim(),
          quality: null,
          save_dir: form.saveDir.trim(),
          referer: optional(form.referer),
          headers: optional(form.headers),
          cookies: optional(form.cookies),
          manual_key: optional(form.manualKey),
          manual_iv: optional(form.manualIv),
          title: optional(form.title),
          max_threads: Number(form.threads) || 6,
        },
      });
      setForm((prev) => ({
        ...prev,
        title: "",
        playlistUrl: "",
      }));
      setModalOpen(false);
    } catch (err: any) {
      console.error(err);
      setError(typeof err === "string" ? err : err?.message ?? "创建任务失败");
    } finally {
      setFormBusy(false);
    }
  };

  const stats = [
    { label: "进行中", value: summary.downloading },
    { label: "排队", value: summary.waitingKey },
    { label: "已完成", value: summary.completed },
    { label: "异常", value: summary.errors },
  ];

  return (
    <div className="app-shell">
      <header className="app-header">
        <div className="header-spacer" />
        <div className="header-actions">
          <div className="status-chip" title={statusLabel}>
            <span className={`status-indicator ${statusDotClass}`} aria-hidden />
            <span className="sr-only">{statusLabel}</span>
          </div>
          <button
            className="icon-button"
            title="设置 (待实现)"
            disabled
            aria-label="设置"
          >
            <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
              <path d="M12 15.5A3.5 3.5 0 1 0 12 8.5a3.5 3.5 0 0 0 0 7z" />
              <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 1 1-4 0v-.09a1.65 1.65 0 0 0-1-1.51 1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 1 1 0-4h.09a1.65 1.65 0 0 0 1.51-1 1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9A1.65 1.65 0 0 0 10 3.09V3a2 2 0 1 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 1 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z" />
            </svg>
          </button>
        </div>
      </header>

      <section className="stats-row">
        {stats.map((stat) => (
          <div key={stat.label} className="stat-card">
            <span className="stat-label">{stat.label}</span>
            <span className="stat-value">{stat.value}</span>
          </div>
        ))}
      </section>

      <section className="control-bar">
        <div className="toolbar-left">
          <button
            className="toolbar-button primary"
            onClick={() => {
              setError(null);
              setModalOpen(true);
            }}
          >
            新建任务
          </button>
          <button className="toolbar-button" disabled={!hasSelection} onClick={() => handlePauseSelected(selectedIds)}>
            暂停任务
          </button>
          <button className="toolbar-button" disabled>
            导入 m3u8
          </button>
        </div>
        <div className="toolbar-right">
          <div className="filter-chip" title="功能待实现">
            <span>状态</span>
            <span className="filter-value">全部</span>
          </div>
          <div className="filter-search" title="功能待实现">
            <input type="search" placeholder="快速筛选" disabled />
          </div>
        </div>
      </section>

      <section className="task-section">
        <div className="task-list-panel">
          <div className="panel-title">
            <h2>下载任务</h2>
            <span>{summary.totalSegmentsDownloaded} 个分片已完成</span>
          </div>
          <div className="task-table">
            <div className="task-table-header">
              <span>任务</span>
              <span>片段</span>
              <span>进度</span>
              <span>速度</span>
              <span>ETA</span>
              <span>状态</span>
            </div>
            <div className="task-table-body">
              {state.order.length === 0 && <div className="empty-hint">暂无任务，请创建新的 HLS 下载</div>}
              {state.order.map((taskId) => {
                const task = state.tasks[taskId];
                return (
                  <TaskRow
                    key={taskId}
                    task={task}
                    checked={state.selected.has(taskId)}
                    onToggle={() => dispatch({ type: "toggleSelect", task_id: taskId })}
                  />
                );
              })}
            </div>
          </div>
        </div>
      </section>

      {modalOpen && (
        <div
          className="modal-backdrop"
          onClick={() => {
            if (!formBusy) {
              setModalOpen(false);
              setError(null);
            }
          }}
        >
          <div
            className="modal"
            onClick={(event) => event.stopPropagation()}
          >
            <header className="modal-header">
              <div>
                <h2>新建 HLS 下载任务</h2>
                <p>解析 m3u8 后将通过 WebSocket 广播分片计划与 KEY 请求结果</p>
              </div>
              <button
                className="ghost-button"
                onClick={() => {
                  if (!formBusy) {
                    setModalOpen(false);
                    setError(null);
                  }
                }}
              >
                关闭
              </button>
            </header>
            <form
              className="modal-form"
              onSubmit={(event) => {
                event.preventDefault();
                handleSubmit();
              }}
            >
              <input
                type="text"
                placeholder="任务标题（可选）"
                value={form.title}
                onChange={(e) => handleInputChange("title", e.target.value)}
                disabled={formBusy}
              />
              <input
                type="url"
                placeholder="m3u8 链接，例如 https://example.com/master.m3u8"
                value={form.playlistUrl}
                onChange={(e) => handleInputChange("playlistUrl", e.target.value)}
                disabled={formBusy}
                required
              />
              <input
                type="text"
                placeholder="保存路径，例如 /Users/demo/Videos"
                value={form.saveDir}
                onChange={(e) => handleInputChange("saveDir", e.target.value)}
                disabled={formBusy}
              />
              <input
                type="text"
                placeholder="自定义 Referer / Origin（可选）"
                value={form.referer}
                onChange={(e) => handleInputChange("referer", e.target.value)}
                disabled={formBusy}
              />
              <textarea
                rows={3}
                placeholder="自定义 Header，每行一个（可选）"
                value={form.headers}
                onChange={(e) => handleInputChange("headers", e.target.value)}
                disabled={formBusy}
              />
              <input
                type="text"
                placeholder="Cookies / Authorization（可选）"
                value={form.cookies}
                onChange={(e) => handleInputChange("cookies", e.target.value)}
                disabled={formBusy}
              />
              <input
                type="text"
                placeholder="手动输入 KEY（hex/base64，可选）"
                value={form.manualKey}
                onChange={(e) => handleInputChange("manualKey", e.target.value)}
                disabled={formBusy}
              />
              <input
                type="text"
                placeholder="手动输入 IV（hex，可选）"
                value={form.manualIv}
                onChange={(e) => handleInputChange("manualIv", e.target.value)}
                disabled={formBusy}
              />
              <select
                value={form.threads}
                onChange={(e) => handleInputChange("threads", e.target.value)}
                disabled={formBusy}
              >
                <option value="4">线程 4</option>
                <option value="6">线程 6</option>
                <option value="8">线程 8</option>
              </select>
              {error && <p className="error-text">{error}</p>}
              <div className="modal-actions">
                <button
                  type="button"
                  className="ghost-button"
                  onClick={() => {
                    if (!formBusy) {
                      setModalOpen(false);
                      setError(null);
                    }
                  }}
                >
                  取消
                </button>
                <button type="submit" className="primary-button" disabled={formBusy}>
                  {formBusy ? "创建中..." : "解析并添加任务"}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}

function TaskRow({ task, checked, onToggle }: { task: TaskSnapshot; checked: boolean; onToggle: () => void }) {
  return (
    <div className="task-row" onClick={onToggle}>
      <div className="task-select" onClick={(e) => { e.stopPropagation(); onToggle(); }}>
        <input type="checkbox" checked={checked} readOnly />
      </div>
      <div className="task-main">
        <strong>
          {task.title}
          <span className="task-quality">{task.quality ?? "自动"}</span>
        </strong>
        <span className="task-source">{task.playlist_url}</span>
      </div>
      <div className="task-segments">
        <span>
          {task.downloaded_segments}/{task.total_segments}
        </span>
        <span className="segments-meta">{task.playlist_type.toUpperCase()}</span>
      </div>
      <div className="task-progress">
        <div className="progress-bar" aria-hidden>
          <div className={`progress-fill status-${task.status}`} style={{ width: `${task.progress}%` }} />
        </div>
        <span>{task.progress.toFixed(1)}%</span>
      </div>
      <span>{formatSpeed(task.speed_bps)}</span>
      <span>{task.eta_seconds ? formatEta(task.eta_seconds) : "估算中"}</span>
      <span className={`status-chip status-${task.status}`}>{taskStatusLabel(task.status)}</span>
    </div>
  );
}

function computeSummary(tasks: TaskSnapshot[]) {
  let downloading = 0;
  let waitingKey = 0;
  let completed = 0;
  let errors = 0;
  let totalSegmentsDownloaded = 0;

  tasks.forEach((task) => {
    if (task.status === "downloading") downloading += 1;
    if (task.encryption?.status === "fetching") waitingKey += 1;
    if (task.status === "completed") completed += 1;
    if (task.status === "error") errors += 1;
    totalSegmentsDownloaded += task.downloaded_segments;
  });

  return { downloading, waitingKey, completed, errors, totalSegmentsDownloaded };
}

function formatSpeed(speed: number) {
  if (speed <= 0 || Number.isNaN(speed)) return "—";
  const mb = speed / (1024 * 1024);
  return `${mb.toFixed(2)} MB/s`;
}



function formatEta(seconds: number) {
  const mins = Math.floor(seconds / 60);
  const secs = Math.floor(seconds % 60);
  return `${mins}m ${secs}s`;
}





function taskStatusLabel(status: TaskStatus) {
  switch (status) {
    case "queued":
      return "排队";
    case "downloading":
      return "进行中";
    case "paused":
      return "已暂停";
    case "completed":
      return "已完成";
    case "error":
      return "异常";
    default:
      return status;
  }
}





function optional(value: string) {
  const trimmed = value.trim();
  return trimmed.length === 0 ? null : trimmed;
}

export default App;
