import { useEffect, useMemo, useReducer, useRef, useState } from "react";
import { invoke } from "@tauri-apps/api/core";
import { openPath } from "@tauri-apps/plugin-opener";
import "./App.css";

type TaskStatus = "queued" | "downloading" | "paused" | "completed" | "error";
type SegmentStatus = "pending" | "downloading" | "completed" | "failed" | "retrying";
type EncryptionStatus = "unknown" | "fetching" | "ready" | "failed";
type TaskStatusFilter = "downloading" | "standby" | "completed" | "error";
type StatusFilterValue = "all" | TaskStatus;

const FILTER_STATUS_MAP: Record<TaskStatusFilter, TaskStatus[]> = {
  downloading: ["downloading"],
  standby: ["queued", "paused"],
  completed: ["completed"],
  error: ["error"],
};

const ALL_TASK_STATUSES: TaskStatus[] = ["queued", "downloading", "paused", "completed", "error"];

const STATUS_TO_TASK_FILTER: Record<Exclude<StatusFilterValue, "all">, TaskStatusFilter> = {
  downloading: "downloading",
  queued: "standby",
  paused: "standby",
  completed: "completed",
  error: "error",
};

const STATUS_FILTER_OPTIONS: Array<{ value: StatusFilterValue; label: string }> = [
  { value: "all", label: "全部" },
  { value: "downloading", label: "下载中" },
  { value: "queued", label: "排队中" },
  { value: "paused", label: "已暂停" },
  { value: "completed", label: "已完成" },
  { value: "error", label: "异常" },
];
interface SegmentSnapshot {
  seq: number;
  status: SegmentStatus;
  speedBps?: number | null;
  sizeBytes?: number | null;
  retries: number;
  lastUpdated?: string | null;
}

interface TaskLog {
  timestamp: string;
  message: string;
}

interface EncryptionSnapshot {
  method?: string | null;
  keyUri?: string | null;
  status: EncryptionStatus;
  iv?: string | null;
  lastError?: string | null;
}

interface TaskSnapshot {
  id: string;
  title: string;
  playlistUrl: string;
  quality?: string | null;
  resolution?: string | null;
  audio?: string | null;
  playlistType: string;
  saveDir: string;
  totalSegments: number;
  downloadedSegments: number;
  downloadedBytes: number;
  progress: number;
  speedBps: number;
  status: TaskStatus;
  etaSeconds?: number | null;
  createdAt: string;
  lastEvent?: string | null;
  averageSegmentSizeBytes?: number | null;
  retries: number;
  encryption?: EncryptionSnapshot | null;
  segments: SegmentSnapshot[];
  logs: TaskLog[];
  outputPath?: string | null;
}

interface LoggingConfig {
  maxFileSizeBytes: number;
  maxTotalSizeBytes: number;
}

interface LogFileInfo {
  name: string;
  size: number;
  modified?: string | null;
}

type FrontendEvent =
  | { type: "snapshot"; tasks: TaskSnapshot[] }
  | { type: "taskCreated"; task: TaskSnapshot }
  | { type: "taskUpdated"; task: TaskSnapshot }
  | { type: "taskCompleted"; taskId: string; task: TaskSnapshot }
  | { type: "taskStatus"; taskId: string; status: TaskStatus; message?: string | null }
  | { type: "segmentUpdated"; taskId: string; segment: SegmentSnapshot }
  | { type: "logAppended"; taskId: string; log: TaskLog }
  | { type: "keyStatus"; taskId: string; encryption: EncryptionSnapshot }
  | { type: "taskError"; taskId: string; message: string }
  | { type: "taskRemoved"; taskId: string };

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
  | { type: "taskStatus"; taskId: string; status: TaskStatus; message?: string | null }
  | { type: "segmentUpdated"; taskId: string; segment: SegmentSnapshot }
  | { type: "logAppended"; taskId: string; log: TaskLog }
  | { type: "keyStatus"; taskId: string; encryption: EncryptionSnapshot }
  | { type: "taskError"; taskId: string; message: string }
  | { type: "taskRemoved"; taskId: string }
  | { type: "setActive"; taskId: string }
  | { type: "toggleSelect"; taskId: string }
  | { type: "clearSelection" };

const initialState: AppState = {
  tasks: {},
  order: [],
  activeTaskId: null,
  selected: new Set(),
};

const LAST_SAVE_DIR_KEY = "downloader:last-save-dir";

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
      const task = state.tasks[action.taskId];
      if (!task) return state;
      return {
        ...state,
        tasks: {
          ...state.tasks,
          [action.taskId]: {
            ...task,
            status: action.status,
            lastEvent: action.message ?? task.lastEvent,
          },
        },
        selected: new Set(state.selected),
      };
    }
    case "segmentUpdated": {
      const task = state.tasks[action.taskId];
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
          [action.taskId]: {
            ...task,
            segments,
          },
        },
      };
    }
    case "logAppended": {
      const task = state.tasks[action.taskId];
      if (!task) return state;
      const logs = [...task.logs, action.log].slice(-100);
      return {
        ...state,
        tasks: {
          ...state.tasks,
          [action.taskId]: {
            ...task,
            logs,
            lastEvent: action.log.message,
          },
        },
        selected: new Set(state.selected),
      };
    }
    case "keyStatus": {
      const task = state.tasks[action.taskId];
      if (!task) return state;
      return {
        ...state,
        tasks: {
          ...state.tasks,
          [action.taskId]: {
            ...task,
            encryption: action.encryption,
          },
        },
      };
    }
    case "taskError": {
      const task = state.tasks[action.taskId];
      if (!task) return state;
      return {
        ...state,
        tasks: {
          ...state.tasks,
          [action.taskId]: {
            ...task,
            status: "error",
            lastEvent: action.message,
          },
        },
        selected: new Set(state.selected),
      };
    }
    case "taskRemoved": {
      if (!state.tasks[action.taskId]) {
        return state;
      }
      const tasks = { ...state.tasks };
      delete tasks[action.taskId];
      const order = state.order.filter((id) => id !== action.taskId);
      const selected = new Set(state.selected);
      selected.delete(action.taskId);
      const activeTaskId = state.activeTaskId === action.taskId ? order.find((id) => id !== action.taskId) ?? null : state.activeTaskId;
      return {
        tasks,
        order,
        activeTaskId,
        selected,
      };
    }
    case "setActive":
      return { ...state, activeTaskId: action.taskId };
    case "toggleSelect": {
      const selected = new Set(state.selected);
      if (selected.has(action.taskId)) {
        selected.delete(action.taskId);
      } else {
        selected.add(action.taskId);
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
    pageUrl: "",
    cookies: "",
    manualKey: "",
    manualIv: "",
    threads: "6",
  });
  const [formBusy, setFormBusy] = useState(false);
  const [modalOpen, setModalOpen] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [logConfig, setLogConfig] = useState<LoggingConfig | null>(null);
  const [logForm, setLogForm] = useState({ maxFileSizeMb: "", maxTotalSizeMb: "" });
  const [logFiles, setLogFiles] = useState<LogFileInfo[]>([]);
  const [selectedLog, setSelectedLog] = useState<string | null>(null);
  const [logPreview, setLogPreview] = useState("");
  const [logLoading, setLogLoading] = useState(false);
  const [configSaving, setConfigSaving] = useState(false);
  const [detailTaskId, setDetailTaskId] = useState<string | null>(null);
  const [taskFilter, setTaskFilter] = useState<TaskStatusFilter>("downloading");
  const [statusFilter, setStatusFilter] = useState<StatusFilterValue>("all");
  const [searchTerm, setSearchTerm] = useState("");
  const [isComposing, setIsComposing] = useState(false);

  useEffect(() => {
    if (detailTaskId && !state.tasks[detailTaskId]) {
      setDetailTaskId(null);
    }
  }, [detailTaskId, state.tasks]);

  useEffect(() => {
    const last = window.localStorage.getItem(LAST_SAVE_DIR_KEY);
    if (last) {
      setForm((prev) => ({ ...prev, saveDir: last }));
    }
  }, []);

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
    let reconnectTimer: number | null = null;
    let retryCount = 0;
    let socket: WebSocket | null = null;

    const scheduleReconnect = () => {
      if (cancelled || reconnectTimer !== null) return;
      const delay = Math.min(5_000, 300 * 2 ** Math.min(retryCount, 4));
      retryCount += 1;
      setWsStatus("connecting");
      reconnectTimer = window.setTimeout(() => {
        reconnectTimer = null;
        connect();
      }, delay);
    };

    async function connect() {
      try {
        const endpoint = await invoke<string>("websocket_endpoint");
        if (cancelled) return;
        const currentSocket = new WebSocket(endpoint);
        socket = currentSocket;
        wsRef.current = currentSocket;

        currentSocket.onopen = () => {
          retryCount = 0;
          setWsStatus("connected");
        };
        currentSocket.onerror = () => {
          if (!cancelled) setWsStatus("error");
        };
        currentSocket.onclose = () => {
          if (wsRef.current === currentSocket) {
            wsRef.current = null;
          }
          scheduleReconnect();
        };
        currentSocket.onmessage = (event) => {
          try {
            const payload: FrontendEvent = JSON.parse(event.data);
            handleEvent(payload);
          } catch (err) {
            console.error("Failed to parse websocket event", err);
          }
        };
      } catch (err) {
        console.error(err);
        if (!cancelled) {
          setWsStatus("error");
          scheduleReconnect();
        }
      }
    }

    connect();
    return () => {
      cancelled = true;
      if (reconnectTimer !== null) {
        window.clearTimeout(reconnectTimer);
      }
      socket?.close();
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
          taskId: event.taskId,
          status: event.status,
          message: event.message,
        });
        break;
      case "segmentUpdated":
        dispatch({
          type: "segmentUpdated",
          taskId: event.taskId,
          segment: event.segment,
        });
        break;
      case "logAppended":
        dispatch({
          type: "logAppended",
          taskId: event.taskId,
          log: event.log,
        });
        break;
      case "keyStatus":
        dispatch({
          type: "keyStatus",
          taskId: event.taskId,
          encryption: event.encryption,
        });
        break;
      case "taskError":
        dispatch({
          type: "taskError",
          taskId: event.taskId,
          message: event.message,
        });
        break;
      case "taskRemoved":
        dispatch({
          type: "taskRemoved",
          taskId: event.taskId,
        });
        break;
      default:
        break;
    }
  };

  const selectedIds = useMemo(() => Array.from(state.selected), [state.selected]);
  const hasSelection = selectedIds.length > 0;
  const summary = useMemo(() => computeSummary(Object.values(state.tasks)), [state.tasks]);
  const filterOptions = useMemo(
    () => [
      { key: "downloading" as TaskStatusFilter, label: "进行中", count: summary.downloading },
      { key: "standby" as TaskStatusFilter, label: "排队/暂停", count: summary.standby },
      { key: "completed" as TaskStatusFilter, label: "已完成", count: summary.completed },
      { key: "error" as TaskStatusFilter, label: "异常", count: summary.errors },
    ],
    [summary],
  );
  useEffect(() => {
    if (statusFilter === "all") {
      return;
    }
    const mapped = STATUS_TO_TASK_FILTER[statusFilter];
    if (mapped && mapped !== taskFilter) {
      setTaskFilter(mapped);
    }
  }, [statusFilter, taskFilter]);
  const normalizedSearch = useMemo(
    () => (isComposing ? "" : searchTerm.trim().toLowerCase()),
    [searchTerm, isComposing],
  );
  const filteredOrder = useMemo(() => {
    const allowedStatuses: TaskStatus[] =
      statusFilter !== "all"
        ? [statusFilter]
        : normalizedSearch
          ? ALL_TASK_STATUSES
          : FILTER_STATUS_MAP[taskFilter];
    return state.order.filter((taskId) => {
      const task = state.tasks[taskId];
      if (!task) return false;
      if (!allowedStatuses.includes(task.status)) {
        return false;
      }
      if (normalizedSearch) {
        const haystack = [
          task.title,
          task.playlistUrl,
          task.outputPath ?? "",
          task.id,
          task.quality ?? "",
          task.resolution ?? "",
        ]
          .join(" ")
          .toLowerCase();
        if (!haystack.includes(normalizedSearch)) {
          return false;
        }
      }
      return true;
    });
  }, [state.order, state.tasks, taskFilter, statusFilter, normalizedSearch]);
  const detailTask = detailTaskId ? state.tasks[detailTaskId] ?? null : null;

  const handlePauseSelected = async (ids: string[]) => {
    if (ids.length === 0) return;
    try {
      await invoke("pause_tasks", { taskIds: ids });
      dispatch({ type: "clearSelection" });
    } catch (err) {
      console.error(err);
    }
  };

  const handleRestartSelected = async (ids: string[]) => {
    if (ids.length === 0) return;
    ids.forEach((taskId) => {
      dispatch({
        type: "taskStatus",
        taskId,
        status: "queued",
        message: "正在重启任务…",
      });
    });
    try {
      await invoke("restart_tasks", { taskIds: ids });
      const tasks = await invoke<TaskSnapshot[]>("list_tasks");
      dispatch({ type: "snapshot", tasks });
      dispatch({ type: "clearSelection" });
    } catch (err) {
      console.error("重启任务失败", err);
      try {
        const tasks = await invoke<TaskSnapshot[]>("list_tasks");
        dispatch({ type: "snapshot", tasks });
      } catch (refreshErr) {
        console.error("刷新任务状态失败", refreshErr);
      }
    }
  };

  const handleDeleteSelected = async (ids: string[]) => {
    if (ids.length === 0) return;
    try {
      await invoke("remove_tasks", { taskIds: ids });
      dispatch({ type: "clearSelection" });
      if (detailTaskId && ids.includes(detailTaskId)) {
        setDetailTaskId(null);
      }
    } catch (err) {
      console.error("删除任务失败", err);
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
    if (!form.saveDir.trim()) {
      setError("请选择保存路径");
      return;
    }
    if (!form.pageUrl.trim()) {
      setError("请提供视频页面地址，它会作为首个 m3u8 请求的 Referer");
      return;
    }
    setError(null);
    setFormBusy(true);
    try {
      const parsedThreads = Number(form.threads);
      const threads = Math.min(16, Math.max(1, Number.isFinite(parsedThreads) ? parsedThreads : 6));
      const saveDir = form.saveDir.trim();
      await invoke("create_task", {
        payload: {
          playlist_url: form.playlistUrl.trim(),
          quality: null,
          save_dir: saveDir,
          page_url: form.pageUrl.trim(),
          headers: null,
          cookies: optional(form.cookies),
          manual_key: optional(form.manualKey),
          manual_iv: optional(form.manualIv),
          title: optional(form.title),
          max_threads: threads,
        },
      });
      if (saveDir.length > 0) {
        window.localStorage.setItem(LAST_SAVE_DIR_KEY, saveDir);
      }
      setForm((prev) => ({
        ...prev,
        title: "",
        playlistUrl: "",
        pageUrl: "",
        cookies: "",
      }));
      setModalOpen(false);
    } catch (err: any) {
      console.error(err);
      setError(typeof err === "string" ? err : err?.message ?? "创建任务失败");
    } finally {
      setFormBusy(false);
    }
  };

  const handlePickDirectory = async () => {
    if (formBusy) return;
    try {
      const selected = await invoke<string | null>("select_save_directory", {
        payload: {
          default_path: form.saveDir || window.localStorage.getItem(LAST_SAVE_DIR_KEY),
        },
      });
      if (selected && selected.length > 0) {
        setForm((prev) => ({ ...prev, saveDir: selected }));
        window.localStorage.setItem(LAST_SAVE_DIR_KEY, selected);
      }
    } catch (err) {
      console.error("选择目录失败", err);
    }
  };

  const refreshLogFiles = async (autoSelect?: string) => {
    try {
      const files = await invoke<LogFileInfo[]>("list_log_files");
      setLogFiles(files);
      const target = autoSelect
        || (files.some((f) => f.name === selectedLog) ? selectedLog : files[0]?.name ?? null);
      if (target) {
        await handleSelectLogFile(target);
      } else {
        setSelectedLog(null);
        setLogPreview("");
      }
    } catch (err) {
      console.error("加载日志列表失败", err);
    }
  };

  const openSettings = async () => {
    try {
      const config = await invoke<LoggingConfig>("get_logging_config");
      setLogConfig(config);
      setLogForm({
        maxFileSizeMb: (config.maxFileSizeBytes / (1024 * 1024)).toFixed(2),
        maxTotalSizeMb: (config.maxTotalSizeBytes / (1024 * 1024)).toFixed(2),
      });
      await refreshLogFiles();
      setSettingsOpen(true);
    } catch (err) {
      console.error("打开设置失败", err);
    }
  };

  const handleSelectLogFile = async (fileName: string) => {
    setSelectedLog(fileName);
    setLogLoading(true);
    try {
      const content = await invoke<string>("read_log_file", { fileName, limit: 200_000 });
      setLogPreview(content);
    } catch (err) {
      console.error("读取日志失败", err);
      setLogPreview("读取日志失败");
    } finally {
      setLogLoading(false);
    }
  };

  const handleSaveLoggingConfig = async () => {
    const fileMb = Number(logForm.maxFileSizeMb);
    const totalMb = Number(logForm.maxTotalSizeMb);
    if (!Number.isFinite(fileMb) || !Number.isFinite(totalMb) || fileMb <= 0 || totalMb <= 0) {
      return;
    }
    const maxFileBytes = Math.max(1, Math.floor(fileMb * 1024 * 1024));
    const maxTotalBytes = Math.max(maxFileBytes, Math.floor(totalMb * 1024 * 1024));
    setConfigSaving(true);
    try {
      await invoke("update_logging_config", {
        config: {
          max_file_size_bytes: maxFileBytes,
          max_total_size_bytes: maxTotalBytes,
        },
      });
      const updated = await invoke<LoggingConfig>("get_logging_config");
      setLogConfig(updated);
      await refreshLogFiles(selectedLog ?? undefined);
    } catch (err) {
      console.error("保存日志配置失败", err);
    } finally {
      setConfigSaving(false);
    }
  };

  const handleOpenLogDirectory = async () => {
    try {
      const dir = await invoke<string>("log_directory_path");
      await openPath(dir);
    } catch (err) {
      console.error("打开日志目录失败", err);
    }
  };

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
            title="设置"
            aria-label="设置"
            onClick={openSettings}
          >
            <svg viewBox="0 0 24 24" width="16" height="16" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round">
              <path d="M12 15.5A3.5 3.5 0 1 0 12 8.5a3.5 3.5 0 0 0 0 7z" />
              <path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 1 1-4 0v-.09a1.65 1.65 0 0 0-1-1.51 1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 1 1 0-4h.09a1.65 1.65 0 0 0 1.51-1 1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9A1.65 1.65 0 0 0 10 3.09V3a2 2 0 1 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 1 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z" />
            </svg>
          </button>
        </div>
      </header>

      <section className="filter-row">
        {filterOptions.map((option) => (
          <button
            key={option.key}
            type="button"
            className={`filter-button${taskFilter === option.key ? " is-active" : ""}`}
            onClick={() => {
              setTaskFilter(option.key);
              setStatusFilter("all");
            }}
            aria-pressed={taskFilter === option.key}
          >
            <span className="filter-label">{option.label}</span>
            <span className="filter-count">{option.count}</span>
          </button>
        ))}
      </section>

      <section className="control-bar">
        <div className="toolbar-left">
          <button
            className="toolbar-button primary"
            onClick={() => {
              setError(null);
              setForm((prev) => ({ ...prev, pageUrl: "" }));
              setModalOpen(true);
            }}
          >
            新建任务
          </button>
          <button className="toolbar-button" disabled={!hasSelection} onClick={() => handlePauseSelected(selectedIds)}>
            暂停任务
          </button>
          <button className="toolbar-button" disabled={!hasSelection} onClick={() => handleRestartSelected(selectedIds)}>
            重启任务
          </button>
          <button className="toolbar-button danger" disabled={!hasSelection} onClick={() => handleDeleteSelected(selectedIds)}>
            删除任务
          </button>
        </div>
        <div className="toolbar-right">
          <div className="filter-chip">
            <label htmlFor="status-filter">状态</label>
            <select
              id="status-filter"
              value={statusFilter}
              onChange={(event) => setStatusFilter(event.target.value as StatusFilterValue)}
            >
              {STATUS_FILTER_OPTIONS.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </div>
          <div className="filter-search" title="快速筛选">
            <input
              type="text"
              placeholder="搜索标题、链接或输出路径"
              value={searchTerm}
              inputMode="search"
              onCompositionStart={() => setIsComposing(true)}
              onCompositionEnd={(event) => {
                setIsComposing(false);
                setSearchTerm(event.currentTarget.value);
              }}
              onChange={(event) => {
                setSearchTerm(event.target.value);
              }}
            />
          </div>
        </div>
      </section>

      <section className="task-section">
        <div className="task-list-panel">
          <div className="task-table">
            <div className="task-table-header">
              <span>任务</span>
              <span>进度</span>
              <span>速度</span>
              <span>ETA</span>
              <span>状态</span>
            </div>
            <div className="task-table-body">
              {state.order.length === 0 && <div className="empty-hint">暂无任务，请创建新的 HLS 下载</div>}
              {state.order.length > 0 && filteredOrder.length === 0 && (
                <div className="empty-hint">暂无该状态的任务</div>
              )}
              {filteredOrder.map((taskId) => {
                const task = state.tasks[taskId];
                if (!task) return null;
                return (
                  <TaskRow
                    key={taskId}
                    task={task}
                    selected={state.selected.has(taskId)}
                    onToggle={() => dispatch({ type: "toggleSelect", taskId })}
                    onOpenDetail={() => setDetailTaskId(taskId)}
                  />
                );
              })}
            </div>
          </div>
        </div>
      </section>

      {settingsOpen && (
        <div
          className="modal-backdrop"
          onClick={() => {
            setSettingsOpen(false);
          }}
        >
          <div
            className="modal settings-modal"
            onClick={(event) => event.stopPropagation()}
          >
            <header className="modal-header">
              <div>
                <h2>应用设置</h2>
              </div>
              <button className="ghost-button" onClick={() => setSettingsOpen(false)}>
                关闭
              </button>
            </header>
            <div className="settings-grid">
              <section className="settings-section">
                <h3>日志策略</h3>
                <label>
                  单个日志文件上限 (MB)
                  <input
                    type="number"
                    min={1}
                    step={1}
                    value={logForm.maxFileSizeMb}
                    onChange={(e) => setLogForm((prev) => ({ ...prev, maxFileSizeMb: e.target.value }))}
                  />
                </label>
                <label>
                  日志总容量上限 (MB)
                  <input
                    type="number"
                    min={5}
                    step={1}
                    value={logForm.maxTotalSizeMb}
                    onChange={(e) => setLogForm((prev) => ({ ...prev, maxTotalSizeMb: e.target.value }))}
                  />
                </label>
                <div className="settings-actions">
                  <button
                    className="primary-button"
                    onClick={handleSaveLoggingConfig}
                    disabled={configSaving}
                  >
                    {configSaving ? "保存中..." : "保存设置"}
                  </button>
                  <button className="ghost-button" onClick={handleOpenLogDirectory}>
                    打开日志目录
                  </button>
                </div>
                {logConfig && (
                  <p className="settings-hint">
                    当前生效：单个文件 {formatMegabytes(logConfig.maxFileSizeBytes)} / 总容量 {formatMegabytes(logConfig.maxTotalSizeBytes)}
                  </p>
                )}
              </section>
              <section className="log-viewer">
                <aside className="log-file-list">
                  <h3>日志文件</h3>
                  <ul>
                    {logFiles.map((file) => (
                      <li key={file.name}>
                        <button
                          className={selectedLog === file.name ? "active" : ""}
                          onClick={() => handleSelectLogFile(file.name)}
                        >
                          <span className="log-name">{file.name}</span>
                          <span className="log-size">{formatBytes(file.size)}</span>
                          <span className="log-time">{file.modified ? new Date(file.modified).toLocaleString() : ""}</span>
                        </button>
                      </li>
                    ))}
                  </ul>
                </aside>
                <div className="log-preview">
                  <h3>日志内容</h3>
                  {logLoading ? (
                    <div className="log-placeholder">加载中...</div>
                  ) : (
                    <pre>{logPreview || "请选择日志文件"}</pre>
                  )}
                </div>
              </section>
            </div>
          </div>
        </div>
      )}

      {detailTask && (
        <TaskDetailModal
          task={detailTask}
          onClose={() => setDetailTaskId(null)}
          onRestart={() => handleRestartSelected([detailTask.id])}
          onDelete={() => handleDeleteSelected([detailTask.id])}
        />
      )}

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
                <h2>新建下载任务</h2>
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
              <div className="directory-input-group">
                <input
                  type="text"
                  placeholder="保存路径，例如 /Users/demo/Videos"
                  value={form.saveDir}
                  onChange={(e) => handleInputChange("saveDir", e.target.value)}
                  disabled={formBusy}
                />
                <button
                  type="button"
                  className="pick-directory-button"
                  onClick={handlePickDirectory}
                  disabled={formBusy}
                >
                  选择目录
                </button>
              </div>
              <input
                type="text"
                placeholder="视频页面地址（必须与浏览器成功的 m3u8 请求 Referer 完全一致）"
                value={form.pageUrl}
                onChange={(e) => handleInputChange("pageUrl", e.target.value)}
                disabled={formBusy}
                required
              />
              <input
                type="text"
                placeholder="Cookie 请求头（可选，其它需要登录或验证的网站可填写）"
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
              <input
                type="number"
                min={1}
                max={16}
                step={1}
                placeholder="并发线程数（1-16）"
                value={form.threads}
                onChange={(e) => handleInputChange("threads", e.target.value)}
                disabled={formBusy}
              />
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
                  {formBusy ? "创建中..." : "添加任务"}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
}

function TaskRow({
  task,
  selected,
  onToggle,
  onOpenDetail,
}: {
  task: TaskSnapshot;
  selected: boolean;
  onToggle: () => void;
  onOpenDetail: () => void;
}) {
  const progressText = `${task.progress.toFixed(2)}%`;
  const isLongTitle = task.title.length > 18;
  const etaLabel = typeof task.etaSeconds === "number" ? formatEta(task.etaSeconds) : "估算中";
  const [titleHover, setTitleHover] = useState(false);
  const titleSpanRef = useRef<HTMLSpanElement>(null);

  return (
    <div
      className={`task-row ${selected ? "is-selected" : ""}`}
      onClick={onToggle}
      onDoubleClick={(event) => {
        event.stopPropagation();
        onOpenDetail();
      }}
      title={task.title}
    >
      <div className="task-main">
        <div
          className={`task-name ${isLongTitle ? "is-marquee" : ""} ${titleHover ? "is-marquee-active" : ""}`}
          title={task.title}
          onMouseEnter={() => {
            if (isLongTitle) {
              setTitleHover(true);
            }
          }}
          onMouseLeave={() => {
            if (isLongTitle) {
              setTitleHover(false);
              const span = titleSpanRef.current;
              if (span) {
                span.style.animation = "none";
                // force reflow to reset animation progress
                void span.offsetWidth;
                span.style.animation = "";
                span.style.transform = "";
              }
            }
          }}
        >
          <span ref={titleSpanRef}>{task.title}</span>
        </div>
      </div>
      <div className="task-progress">
        <div className="progress-bar" aria-hidden>
          <div className={`progress-fill status-${task.status}`} style={{ width: `${task.progress}%` }} />
        </div>
        <span className="progress-value">{progressText}</span>
      </div>
      <span className="task-speed">{formatSpeed(task.speedBps)}</span>
      <span className="task-eta">{etaLabel}</span>
      <span className={`status-pill status-${task.status}`}>{taskStatusLabel(task.status)}</span>
    </div>
  );
}

function TaskDetailModal({
  task,
  onClose,
  onRestart,
  onDelete,
}: {
  task: TaskSnapshot;
  onClose: () => void;
  onRestart: () => void;
  onDelete: () => void;
}) {
  const etaLabel = typeof task.etaSeconds === "number" ? formatEta(task.etaSeconds) : "估算中";
  const stats = [
    { label: "状态", value: taskStatusLabel(task.status) },
    { label: "进度", value: `${task.progress.toFixed(2)}%` },
    { label: "已下载", value: formatBytes(task.downloadedBytes) },
    { label: "当前速度", value: formatSpeed(task.speedBps) },
    { label: "总分片", value: task.totalSegments },
    { label: "剩余时间", value: etaLabel },
  ];
  const recentLogs = task.logs.slice(-50).reverse();

  return (
    <div
      className="modal-backdrop"
      onClick={onClose}
    >
      <div
        className="modal task-detail-modal"
        onClick={(event) => event.stopPropagation()}
      >
        <header className="modal-header">
          <div className="task-detail-header">
            <h2>{task.title}</h2>
            <div className="task-detail-subtitle">
              <span className={`status-pill status-${task.status}`}>{taskStatusLabel(task.status)}</span>
              <span className="task-detail-id">ID: {task.id}</span>
            </div>
            <p className="task-detail-url" title={task.playlistUrl}>
              {task.playlistUrl}
            </p>
          </div>
          <div className="task-detail-actions">
            <button className="ghost-button" onClick={onRestart}>
              重启
            </button>
            <button className="ghost-button danger" onClick={onDelete}>
              删除
            </button>
            <button className="ghost-button" onClick={onClose}>
              关闭
            </button>
          </div>
        </header>
        <div className="task-detail-grid">
          <section className="task-detail-info">
            <h3>概览</h3>
            <ul>
              {stats.map((item) => (
                <li key={item.label}>
                  <span>{item.label}</span>
                  <strong>{item.value}</strong>
                </li>
              ))}
              <li>
                <span>创建时间</span>
                <strong>{formatLogTime(task.createdAt)}</strong>
              </li>
              <li>
                <span>最近事件</span>
                <strong>{task.lastEvent ?? "—"}</strong>
              </li>
            </ul>
          </section>
          <section className="task-detail-logs">
            <h3>任务日志</h3>
            <ul className="log-list">
              {recentLogs.length === 0 ? (
                <li className="log-entry empty">暂无日志</li>
              ) : (
                recentLogs.map((log, index) => (
                  <li key={`${log.timestamp}-${index}`} className="log-entry">
                    <span className="log-time">{formatLogTime(log.timestamp)}</span>
                    <span className="log-message">{log.message}</span>
                  </li>
                ))
              )}
            </ul>
          </section>
        </div>
      </div>
    </div>
  );
}

function formatEta(seconds: number) {
  if (!Number.isFinite(seconds) || seconds <= 0) {
    return "0s";
  }
  const secs = Math.floor(seconds);
  const hours = Math.floor(secs / 3600);
  const mins = Math.floor((secs % 3600) / 60);
  const rem = secs % 60;
  if (hours > 0) {
    return `${hours}h ${mins.toString().padStart(2, "0")}m`;
  }
  if (mins > 0) {
    return `${mins}m ${rem.toString().padStart(2, "0")}s`;
  }
  return `${Math.max(rem, 1)}s`;
}

function computeSummary(tasks: TaskSnapshot[]) {
  let downloading = 0;
  let waitingKey = 0;
  let standby = 0;
  let completed = 0;
  let errors = 0;

  tasks.forEach((task) => {
    if (task.status === "downloading") downloading += 1;
    if (task.encryption?.status === "fetching") waitingKey += 1;
    if (task.status === "queued" || task.status === "paused") standby += 1;
    if (task.status === "completed") completed += 1;
    if (task.status === "error") errors += 1;
  });

  return { downloading, standby, waitingKey, completed, errors };
}

function formatSpeed(speed: number) {
  if (speed <= 0 || Number.isNaN(speed)) return "—";
  const mb = speed / (1024 * 1024);
  return `${mb.toFixed(2)} MB/s`;
}

function formatBytes(bytes: number) {
  if (bytes <= 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  let value = bytes;
  let idx = 0;
  while (value >= 1024 && idx < units.length - 1) {
    value /= 1024;
    idx += 1;
  }
  return `${value.toFixed(idx === 0 ? 0 : 2)} ${units[idx]}`;
}

function formatMegabytes(bytes: number) {
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

function formatLogTime(timestamp: string) {
  if (!timestamp) return "";
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) {
    return timestamp;
  }
  return date.toLocaleString("zh-CN", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
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
