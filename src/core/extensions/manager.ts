import { unsafeWindow } from '$';
import { options } from '@/core/options';
import logger from '@/utils/logger';
import { Signal } from '@preact/signals';
import { Extension, ExtensionConstructor } from './extension';

/**
 * Global object reference. In some cases, the `unsafeWindow` is not available.
 */
type HookGlobal = Window & typeof globalThis & Record<string, unknown>;

function getUnsafeWindowCandidate(): unknown {
  try {
    return unsafeWindow ?? null;
  } catch {
    return null;
  }
}

function getWrappedPageWindowCandidate(unsafeCandidate: unknown): HookGlobal | null {
  try {
    const unsafeObject = unsafeCandidate as { wrappedJSObject?: unknown } | null;
    if (!unsafeObject?.wrappedJSObject || typeof unsafeObject.wrappedJSObject !== 'object') {
      return null;
    }
    return unsafeObject.wrappedJSObject as HookGlobal;
  } catch {
    return null;
  }
}

let cachedHookGlobalObject: HookGlobal | null = null;
function getHookGlobalObject(): HookGlobal {
  if (cachedHookGlobalObject) {
    return cachedHookGlobalObject;
  }
  const unsafeCandidate = getUnsafeWindowCandidate();
  const wrappedCandidate = getWrappedPageWindowCandidate(unsafeCandidate);
  const windowCandidate = typeof window !== 'undefined' ? (window as unknown as HookGlobal) : null;
  const globalCandidate = globalThis as HookGlobal;
  cachedHookGlobalObject = (wrappedCandidate ??
    (unsafeCandidate as HookGlobal | null) ??
    windowCandidate ??
    globalCandidate) as HookGlobal;
  return cachedHookGlobalObject;
}

const hookGlobalObject = new Proxy({} as HookGlobal, {
  get(_target, prop) {
    const globalObject = getHookGlobalObject();
    return Reflect.get(globalObject, prop, globalObject);
  },
  set(_target, prop, value) {
    const globalObject = getHookGlobalObject();
    return Reflect.set(globalObject, prop, value, globalObject);
  },
});

type HookCallable = (...args: unknown[]) => unknown;
type ExportFunction = (
  fn: HookCallable,
  target: object,
  options?: { defineAs?: string },
) => unknown;
// Firefox-only: `exportFunction` makes functions callable from the page realm.
function getExportFunctionMaybe(): ExportFunction | undefined {
  try {
    return (globalThis as unknown as { exportFunction?: unknown }).exportFunction as
      | ExportFunction
      | undefined;
  } catch {
    return undefined;
  }
}

const HOOK_MESSAGE_FLAG = '__twe_mcp_hook_v1';
const ORIG_XHR_OPEN_KEY = '__twe_orig_xhr_open_v1';
const ORIG_XHR_SEND_KEY = '__twe_orig_xhr_send_v1';
const ORIG_FETCH_KEY = '__twe_orig_fetch_v1';
const HOOK_RUNTIME_KEY = '__twe_runtime_v1';
const HOOK_STATS_KEY = '__twe_hook_stats_v1';
const RUNTIME_MODES_KEY = '__twe_runtime_modes_v1';
const BOOKMARK_CONTEXT_KEY = '__twe_bookmark_context_v1';
const BOOKMARK_CONTEXT_DUMP_KEY = '__twe_bookmark_context_dump_v1';
const HOOK_BOOTSTRAP_ERROR_KEY = '__twe_bootstrap_error_v1';
const HOOK_RUNTIME_SIGNATURE = 'twitter-web-exporter-hook-v1';
const BOOKMARK_CONTEXT_BOOKMARKS_ONLY_STALE_MS = 45000;
const BOOKMARK_CONTEXT_LOCK_TTL_MS = 180000;
const BOOKMARK_CONTEXT_SCAN_DEPTH = 6;
const BOOKMARK_CONTEXT_MIN_CONFIDENCE = 12;
const BOOKMARK_CONTEXT_DUMP_LIMIT = 200;
const BOOKMARK_CONTEXT_LOCK_KEY = '__twe_bookmark_context_lock_v1';
const HOOK_REVISION = 3;
const EXTENSION_MANAGER_SIGNATURE = 'twitter-web-exporter-extension-manager-v1';
const EXTENSION_MANAGER_REVISION = 3;
const HOOK_REPAIR_INTERVAL_MS = 1100;
const HOOK_REPAIR_BACKOFF_MAX_MS = 60000;
const HOOK_REPAIR_FAILURE_LIMIT = 5;
const RESPONSE_DEDUPE_WINDOW_MS = 2600;
const RESPONSE_DEDUPE_MAX_ENTRIES = 500;
const RESPONSE_DEDUPE_CLEANUP_COUNT = 120;

export { EXTENSION_MANAGER_SIGNATURE, EXTENSION_MANAGER_REVISION };

type EndpointHookMetrics = {
  received: number;
  processed: number;
  skippedDuplicate: number;
  newUniqueTweets: number;
  legacyShape: number;
  missingContext: number;
  lastAt: number;
  lastStatus: number;
  lastUrl: string;
};

type HookStats = {
  xhrMessages: number;
  fetchMessages: number;
  lastUrl: string;
  lastAt: number;
  loggedUrls: number;
  messagesTotal: number;
  messagesLegacyShape: number;
  messagesMissingContext: number;
  messagesRepairedAtBridge: number;
  messagesMissingBody: number;
  responsesProcessed: number;
  responsesSkippedDuplicate: number;
  lastMessageAt: number;
  activeInstanceId: string;
  rev: number;
  repairCount: number;
  endpointStats: Record<string, EndpointHookMetrics>;
};

type RecentSig = { sig: string; at: number };

type HookMode = 'both' | 'xhr' | 'fetch' | 'off';
type RepairMode = 'watchdog' | 'off';
type RuntimeModes = {
  safeMode: boolean;
  hookMode: HookMode;
  repairMode: RepairMode;
};

const LOCAL_STORAGE_SAFE_MODE_KEY = 'twe_safe_mode_v1';
const LOCAL_STORAGE_HOOK_MODE_KEY = 'twe_hook_mode_v1';
const LOCAL_STORAGE_REPAIR_MODE_KEY = 'twe_repair_mode_v1';

function normalizeHookMode(value: unknown): HookMode {
  return value === 'xhr' || value === 'fetch' || value === 'off' ? value : 'both';
}

function normalizeRepairMode(value: unknown): RepairMode {
  return value === 'off' ? 'off' : 'watchdog';
}

function readLocalStorageValue(key: string): string | null {
  try {
    if (typeof localStorage === 'undefined') return null;
    return localStorage.getItem(key);
  } catch {
    return null;
  }
}

function writeLocalStorageValue(key: string, value: string): void {
  try {
    if (typeof localStorage === 'undefined') return;
    localStorage.setItem(key, value);
  } catch {
    // ignore
  }
}

function resolveRuntimeModes(): RuntimeModes {
  const optionSafeMode = !!options.get('safeMode', false);
  const optionHookMode = normalizeHookMode(options.get('hookMode', 'both'));
  const optionRepairMode = normalizeRepairMode(options.get('repairMode', 'watchdog'));

  const storageSafeMode = readLocalStorageValue(LOCAL_STORAGE_SAFE_MODE_KEY);
  const storageHookMode = readLocalStorageValue(LOCAL_STORAGE_HOOK_MODE_KEY);
  const storageRepairMode = readLocalStorageValue(LOCAL_STORAGE_REPAIR_MODE_KEY);

  const globalModes = (globalThis as Record<string, unknown>)[RUNTIME_MODES_KEY] as
    | Partial<RuntimeModes>
    | undefined;

  const safeMode =
    typeof globalModes?.safeMode === 'boolean'
      ? globalModes.safeMode
      : storageSafeMode === '1' || storageSafeMode === 'true'
        ? true
        : storageSafeMode === '0' || storageSafeMode === 'false'
          ? false
          : optionSafeMode;
  const hookMode = normalizeHookMode(globalModes?.hookMode ?? storageHookMode ?? optionHookMode);
  const repairMode = normalizeRepairMode(
    globalModes?.repairMode ?? storageRepairMode ?? optionRepairMode,
  );

  return { safeMode, hookMode, repairMode };
}

function getRuntimeCapabilities(): {
  hasUnsafeWindow: boolean;
  hasWrappedJSObject: boolean;
  hasExportFunction: boolean;
} {
  const unsafeCandidate = getUnsafeWindowCandidate();
  return {
    hasUnsafeWindow: !!unsafeCandidate,
    hasWrappedJSObject: !!getWrappedPageWindowCandidate(unsafeCandidate),
    hasExportFunction: !!getExportFunctionMaybe(),
  };
}

function hashText(text: string): string {
  let hash = 2166136261;
  for (let i = 0; i < text.length; i++) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return (hash >>> 0).toString(16);
}

function createRequestSignature(
  method: string,
  url: string,
  status: number,
  responseText: string,
): string {
  return `${method.toUpperCase()} ${url} ${status} ${hashText(responseText)}`;
}

function cleanupSignatureCache(map: Map<string, RecentSig>) {
  const now = Date.now();
  if (map.size <= RESPONSE_DEDUPE_MAX_ENTRIES) return;

  const entries = [...map.entries()]
    .filter(([, value]) => now - value.at > RESPONSE_DEDUPE_WINDOW_MS)
    .sort((a, b) => a[1].at - b[1].at);
  for (const [key] of entries.slice(0, RESPONSE_DEDUPE_CLEANUP_COUNT)) {
    map.delete(key);
  }
}

function createHookStats(instanceId: string): HookStats {
  return {
    xhrMessages: 0,
    fetchMessages: 0,
    lastUrl: '',
    lastAt: 0,
    loggedUrls: 0,
    messagesTotal: 0,
    messagesLegacyShape: 0,
    messagesMissingContext: 0,
    messagesRepairedAtBridge: 0,
    messagesMissingBody: 0,
    responsesProcessed: 0,
    responsesSkippedDuplicate: 0,
    lastMessageAt: 0,
    activeInstanceId: instanceId,
    rev: HOOK_REVISION,
    repairCount: 0,
    endpointStats: Object.create(null),
  };
}

type BookmarkContextPayload = {
  folderId: string | null;
  pageUrl: string;
  source: string;
  capturedAt: number;
  requestId?: string;
  routeSource?: string;
  pageRouteUrl?: string;
};

let activeBookmarkContext: BookmarkContextPayload = {
  folderId: null,
  pageUrl: '',
  source: 'startup',
  capturedAt: 0,
};

let bookmarkContextLock: BookmarkContextPayload | null = null;

type BookmarkContextDumpEntry = {
  requestId: string;
  ts: number;
  method: string;
  url: string;
  hasBody: boolean;
  confidenceSource: string;
  context: BookmarkContextPayload;
  normalizedRoute: string;
};

type BookmarkRequestSource = {
  method: string;
  url: string;
  requestId?: string;
  body?: string;
};

type HookedXhr = XMLHttpRequest & {
  __twe_req_method_v1?: string;
  __twe_req_url_v1?: string;
  __twe_req_body_v1?: string;
  __twe_req_id_v1?: string;
  __twe_req_bookmark_context_v1?: BookmarkContextPayload | null;
  __twe_hooked_v1?: boolean;
};

type InterceptedRequest = {
  method: string;
  url: string;
  body?: string;
  bookmarkContext?: unknown;
  requestId?: string;

  __twe_hook_revision_v1?: number;
  hookRevision?: number;
};

type BootstrapErrorReport = {
  message: string;
  stack?: string;
  phase: string;
  instanceId: string;
  at: number;
};

function recordBootstrapError(
  phase: string,
  instanceId: string,
  error: unknown,
): BootstrapErrorReport {
  const message = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
  const stack = error instanceof Error ? error.stack : undefined;
  const report: BootstrapErrorReport = {
    message,
    stack,
    phase,
    instanceId,
    at: Date.now(),
  };

  try {
    const g = hookGlobalObject as Record<string, unknown>;
    g[HOOK_BOOTSTRAP_ERROR_KEY] = report;
    (window as unknown as Record<string, unknown>)[HOOK_BOOTSTRAP_ERROR_KEY] = report;
    (globalThis as unknown as Record<string, unknown>)[HOOK_BOOTSTRAP_ERROR_KEY] = report;
  } catch {
    // ignore
  }

  return report;
}

function clearBootstrapErrorMarker(): void {
  try {
    const g = hookGlobalObject as Record<string, unknown>;
    const deleted = (obj: unknown, key: string) => {
      if (obj && typeof obj === 'object') {
        delete (obj as Record<string, unknown>)[key];
      }
    };

    deleted(g, HOOK_BOOTSTRAP_ERROR_KEY);
    deleted(window, HOOK_BOOTSTRAP_ERROR_KEY);
    deleted(globalThis, HOOK_BOOTSTRAP_ERROR_KEY);
  } catch {
    // ignore
  }
}

function publishExtensionManagerGlobal(manager: unknown): void {
  const targets = [
    hookGlobalObject as unknown as Record<string, unknown>,
    window as unknown as Record<string, unknown>,
    globalThis as unknown as Record<string, unknown>,
  ];
  for (const target of targets) {
    if (!target || typeof target !== 'object') continue;
    try {
      target['__twe_extension_manager_v1'] = manager;
    } catch {
      // ignore
    }
  }
}

function publishHookGlobals(hookStats: HookStats, runtimePayload: Record<string, unknown>): void {
  const targets = [
    hookGlobalObject as unknown as Record<string, unknown>,
    window as unknown as Record<string, unknown>,
    globalThis as unknown as Record<string, unknown>,
  ];
  for (const target of targets) {
    if (!target || typeof target !== 'object') continue;
    try {
      target[HOOK_STATS_KEY] = hookStats;
      const existing = target[HOOK_RUNTIME_KEY];
      const existingObject =
        existing && typeof existing === 'object' ? (existing as Record<string, unknown>) : {};
      target[HOOK_RUNTIME_KEY] = {
        ...existingObject,
        ...runtimePayload,
      };
    } catch {
      // ignore
    }
  }
}

function normalizeRuntimeForRead(value: unknown, fallback: HookStats): HookStats {
  if (!value || typeof value !== 'object') {
    return fallback;
  }

  const candidate = value as Partial<HookStats>;
  return {
    ...candidate,
    xhrMessages: Number(candidate.xhrMessages) || 0,
    fetchMessages: Number(candidate.fetchMessages) || 0,
    lastUrl: typeof candidate.lastUrl === 'string' ? candidate.lastUrl : '',
    lastAt: Number(candidate.lastAt) || 0,
    loggedUrls: Number(candidate.loggedUrls) || 0,
    messagesTotal: Number(candidate.messagesTotal) || 0,
    messagesLegacyShape: Number(candidate.messagesLegacyShape) || 0,
    messagesMissingContext: Number(candidate.messagesMissingContext) || 0,
    messagesRepairedAtBridge: Number(candidate.messagesRepairedAtBridge) || 0,
    messagesMissingBody: Number(candidate.messagesMissingBody) || 0,
    responsesProcessed: Number(candidate.responsesProcessed) || 0,
    responsesSkippedDuplicate: Number(candidate.responsesSkippedDuplicate) || 0,
    lastMessageAt: Number(candidate.lastMessageAt) || 0,
    activeInstanceId:
      typeof candidate.activeInstanceId === 'string'
        ? candidate.activeInstanceId
        : fallback.activeInstanceId,
    rev: Number(candidate.rev) || HOOK_REVISION,
    repairCount: Number(candidate.repairCount) || 0,
    endpointStats:
      (candidate.endpointStats as Record<string, EndpointHookMetrics> | undefined) ||
      Object.create(null),
  };
}

function toPlainFunctionSource(value: unknown): string {
  if (typeof value !== 'function') return '';
  try {
    return Function.prototype.toString.call(value);
  } catch {
    return '';
  }
}

function hasHookVersion(
  target: unknown,
  marker: '__twe_is_hook_open_v1' | '__twe_is_hook_send_v1' | '__twe_is_hook_fetch_v1',
): boolean {
  if (!target || typeof target !== 'function') return false;
  const candidate = target as {
    __twe_is_hook_open_v1?: boolean;
    __twe_is_hook_send_v1?: boolean;
    __twe_is_hook_fetch_v1?: boolean;
    __twe_is_hook_revision_v1?: number;
  };
  const markerMap = candidate as { [key: string]: unknown };
  if (!markerMap[marker]) return false;
  return candidate.__twe_is_hook_revision_v1 === HOOK_REVISION;
}

function hasHookShape(
  target: unknown,
  marker?: '__twe_is_hook_open_v1' | '__twe_is_hook_send_v1' | '__twe_is_hook_fetch_v1',
): boolean {
  if (!target || typeof target !== 'function') return false;
  const candidate = target as {
    __twe_is_hook_open_v1?: boolean;
    __twe_is_hook_send_v1?: boolean;
    __twe_is_hook_fetch_v1?: boolean;
    __twe_orig_xhr_open_v1?: unknown;
    __twe_orig_xhr_send_v1?: unknown;
    __twe_orig_fetch_v1?: unknown;
  };
  if (
    candidate.__twe_is_hook_open_v1 ||
    candidate.__twe_is_hook_send_v1 ||
    candidate.__twe_is_hook_fetch_v1 ||
    candidate.__twe_orig_xhr_open_v1 ||
    candidate.__twe_orig_xhr_send_v1 ||
    candidate.__twe_orig_fetch_v1
  ) {
    return true;
  }

  if (marker && candidate[marker]) {
    return true;
  }

  const source = toPlainFunctionSource(target);
  if (!source) return false;
  return (
    source.includes('__twe_mcp_hook_v1') ||
    source.includes('__twe_is_hook_open_v1') ||
    source.includes('__twe_is_hook_send_v1') ||
    source.includes('__twe_is_hook_fetch_v1') ||
    source.includes('__twe_is_hook_revision_v1') ||
    source.includes('__twe_orig_xhr_open_v1') ||
    source.includes('__twe_orig_xhr_send_v1') ||
    source.includes('__twe_orig_fetch_v1') ||
    source.includes('__twe_req_body_v1') ||
    source.includes('__twe_req_url_v1') ||
    source.includes('__twe_req_bookmark_context_v1')
  );
}

const BOOKMARK_CONTEXT_REQUEST_ID_PREFIX = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
let bookmarkRequestCounter = 0;

function nextBookmarkRequestId(): string {
  bookmarkRequestCounter += 1;
  return `${BOOKMARK_CONTEXT_REQUEST_ID_PREFIX}-${bookmarkRequestCounter}`;
}

function normalizeContextDumpList(current: unknown): BookmarkContextDumpEntry[] {
  if (!Array.isArray(current)) return [];
  const out: BookmarkContextDumpEntry[] = [];

  for (const item of current) {
    const candidate = item as Partial<BookmarkContextDumpEntry>;
    if (
      candidate &&
      typeof candidate.requestId === 'string' &&
      typeof candidate.ts === 'number' &&
      typeof candidate.url === 'string'
    ) {
      out.push({
        requestId: candidate.requestId,
        ts: candidate.ts,
        method: typeof candidate.method === 'string' ? candidate.method : 'GET',
        url: candidate.url,
        hasBody: !!candidate.hasBody,
        confidenceSource:
          typeof candidate.confidenceSource === 'string' && candidate.confidenceSource
            ? candidate.confidenceSource
            : typeof candidate.context?.source === 'string'
              ? candidate.context.source
              : 'unknown',
        context: {
          folderId:
            typeof candidate.context?.folderId === 'string' ? candidate.context.folderId : null,
          pageUrl: typeof candidate.context?.pageUrl === 'string' ? candidate.context.pageUrl : '',
          source:
            typeof candidate.context?.source === 'string' ? candidate.context.source : 'unknown',
          capturedAt:
            typeof candidate.context?.capturedAt === 'number'
              ? candidate.context.capturedAt
              : Date.now(),
          requestId:
            typeof candidate.context?.requestId === 'string'
              ? candidate.context.requestId
              : undefined,
          routeSource:
            typeof candidate.context?.routeSource === 'string'
              ? candidate.context.routeSource
              : undefined,
          pageRouteUrl:
            typeof candidate.context?.pageRouteUrl === 'string'
              ? candidate.context.pageRouteUrl
              : undefined,
        },
        normalizedRoute:
          typeof candidate.normalizedRoute === 'string' ? candidate.normalizedRoute : '',
      });
    }
  }

  return out.sort((a, b) => b.ts - a.ts).slice(0, BOOKMARK_CONTEXT_DUMP_LIMIT);
}

function appendBookmarkContextDump(entry: BookmarkContextDumpEntry) {
  const now = Date.now();
  const safeEntry: BookmarkContextDumpEntry = {
    requestId: entry.requestId || `${BOOKMARK_CONTEXT_REQUEST_ID_PREFIX}-${now}`,
    ts: Number.isFinite(entry.ts) ? entry.ts : now,
    method: entry.method || 'GET',
    url: entry.url || '',
    hasBody: !!entry.hasBody,
    confidenceSource:
      typeof entry.confidenceSource === 'string' && entry.confidenceSource
        ? entry.confidenceSource
        : entry.context?.source || 'unknown',
    context: {
      folderId: entry.context?.folderId ?? null,
      pageUrl: entry.context?.pageUrl || '',
      source: entry.context?.source || 'unknown',
      capturedAt: Number.isFinite(entry.context?.capturedAt) ? entry.context.capturedAt : now,
      requestId: entry.context?.requestId,
      routeSource: entry.context?.routeSource,
      pageRouteUrl: entry.context?.pageRouteUrl,
    },
    normalizedRoute: entry.normalizedRoute || '',
  };

  try {
    const current = normalizeContextDumpList(
      (hookGlobalObject as Record<string, unknown>)[BOOKMARK_CONTEXT_DUMP_KEY],
    );
    current.unshift(safeEntry);
    const deduped = new Map<string, BookmarkContextDumpEntry>();
    for (const candidate of current) {
      if (!deduped.has(candidate.requestId)) {
        deduped.set(candidate.requestId, candidate);
      }
    }
    const next = Array.from(deduped.values())
      .slice(0, BOOKMARK_CONTEXT_DUMP_LIMIT)
      .sort((a, b) => b.ts - a.ts);
    const staleCutoff = Date.now() - BOOKMARK_CONTEXT_BOOKMARKS_ONLY_STALE_MS * 6;
    const trimmed = next.filter((candidate) => !staleCutoff || candidate.ts >= staleCutoff);

    (hookGlobalObject as Record<string, unknown>)[BOOKMARK_CONTEXT_DUMP_KEY] = trimmed;
    (window as unknown as Record<string, unknown>)[BOOKMARK_CONTEXT_DUMP_KEY] = trimmed;
    (globalThis as unknown as Record<string, unknown>)[BOOKMARK_CONTEXT_DUMP_KEY] = trimmed;
  } catch {
    // ignore
  }
}

function setBookmarkContextLock(value: BookmarkContextPayload): void {
  const payload: BookmarkContextPayload = {
    folderId: value.folderId,
    pageUrl: value.pageUrl || '',
    source: value.source || 'lock',
    capturedAt: value.capturedAt || Date.now(),
    requestId: value.requestId,
    routeSource: value.routeSource,
    pageRouteUrl: value.pageRouteUrl,
  };

  if (!payload.folderId) {
    return;
  }

  bookmarkContextLock = payload;
  try {
    const g = hookGlobalObject as Record<string, unknown>;
    g[BOOKMARK_CONTEXT_LOCK_KEY] = payload;
    (window as unknown as Record<string, unknown>)[BOOKMARK_CONTEXT_LOCK_KEY] = payload;
    (globalThis as unknown as Record<string, unknown>)[BOOKMARK_CONTEXT_LOCK_KEY] = payload;
  } catch {
    // ignore
  }
}

function getBookmarkContextLock(now = Date.now()): BookmarkContextPayload | null {
  const candidates = [bookmarkContextLock] as Array<unknown>;
  try {
    candidates.push((hookGlobalObject as Record<string, unknown>)[BOOKMARK_CONTEXT_LOCK_KEY]);
    candidates.push((window as unknown as Record<string, unknown>)[BOOKMARK_CONTEXT_LOCK_KEY]);
    candidates.push((globalThis as unknown as Record<string, unknown>)[BOOKMARK_CONTEXT_LOCK_KEY]);
  } catch {
    // ignore
  }

  for (const raw of candidates) {
    if (!raw || typeof raw !== 'object') continue;
    const candidate = raw as Partial<BookmarkContextPayload>;
    if (typeof candidate.folderId !== 'string' || !candidate.folderId) continue;
    const capturedAt =
      typeof candidate.capturedAt === 'number' && Number.isFinite(candidate.capturedAt)
        ? candidate.capturedAt
        : 0;
    if (!capturedAt || now - capturedAt > BOOKMARK_CONTEXT_LOCK_TTL_MS) {
      continue;
    }

    return {
      folderId: candidate.folderId,
      pageUrl: typeof candidate.pageUrl === 'string' ? candidate.pageUrl : '',
      source: typeof candidate.source === 'string' ? candidate.source : 'lock',
      capturedAt,
      requestId: typeof candidate.requestId === 'string' ? candidate.requestId : undefined,
      routeSource: typeof candidate.routeSource === 'string' ? candidate.routeSource : undefined,
      pageRouteUrl: typeof candidate.pageRouteUrl === 'string' ? candidate.pageRouteUrl : undefined,
    };
  }

  return null;
}

function clearBookmarkContextLock(): void {
  bookmarkContextLock = null;
  try {
    const g = hookGlobalObject as Record<string, unknown>;
    delete g[BOOKMARK_CONTEXT_LOCK_KEY];
    delete (window as unknown as Record<string, unknown>)[BOOKMARK_CONTEXT_LOCK_KEY];
    delete (globalThis as unknown as Record<string, unknown>)[BOOKMARK_CONTEXT_LOCK_KEY];
  } catch {
    // ignore
  }
}

function resolveCanonicalRouteFromUrl(url: string): { folderId: string | null; pageUrl: string } {
  return captureBookmarkRouteFromUrl(url) || { folderId: null, pageUrl: url };
}

function markHookFunction(
  fn: unknown,
  marker: '__twe_is_hook_open_v1' | '__twe_is_hook_send_v1' | '__twe_is_hook_fetch_v1',
) {
  if (typeof fn !== 'function') return;
  try {
    const hookFn = fn as HookCallable & Record<string, unknown>;
    hookFn[marker] = true;
    hookFn.__twe_is_hook_revision_v1 = HOOK_REVISION;
  } catch {
    // ignore
  }
}

function normalizeHookPayloadRequest(rawReq: unknown): InterceptedRequest {
  const req = rawReq as Partial<InterceptedRequest>;

  const normalized: InterceptedRequest = {
    method: typeof req.method === 'string' ? req.method : 'GET',
    url: typeof req.url === 'string' ? req.url : '',
  };

  if (typeof req.body === 'string') {
    normalized.body = req.body;
  }

  if (req.bookmarkContext !== undefined) {
    normalized.bookmarkContext = req.bookmarkContext;
  }

  if (
    normalized.bookmarkContext === undefined &&
    (req as { requestContext?: unknown }).requestContext !== undefined
  ) {
    normalized.bookmarkContext = (req as { requestContext?: unknown }).requestContext;
  }

  if (typeof req.requestId === 'string' && req.requestId) {
    normalized.requestId = req.requestId;
  }

  if (
    typeof req.__twe_hook_revision_v1 === 'number' &&
    Number.isFinite(req.__twe_hook_revision_v1)
  ) {
    normalized.hookRevision = req.__twe_hook_revision_v1;
  }

  return normalized;
}

function buildNormalizedHookMessageRequest(rawReq: unknown): InterceptedRequest {
  const req = normalizeHookPayloadRequest(rawReq);
  const requestId =
    typeof req.requestId === 'string' && req.requestId.trim().length > 0
      ? req.requestId
      : nextBookmarkRequestId();
  const method = typeof req.method === 'string' && req.method ? req.method : 'GET';
  const url = typeof req.url === 'string' ? req.url : '';
  const body = typeof req.body === 'string' ? req.body : '';
  const bookmarkContext = normalizeBookmarkContextValue(req.bookmarkContext, {
    method,
    url,
    body,
    requestId,
    hasBody: body.length > 0,
  });

  return {
    method,
    url,
    body,
    bookmarkContext,
    requestId,
    hookRevision: HOOK_REVISION,
    __twe_hook_revision_v1: HOOK_REVISION,
  };
}

function pickBridgeRequest(rawMessage: unknown): unknown {
  if (!rawMessage || typeof rawMessage !== 'object') return null;
  const envelope = rawMessage as Record<string, unknown>;
  if (envelope.req && typeof envelope.req === 'object') return envelope.req;
  return envelope;
}

function getBridgeMessageRevision(rawMessage: unknown): number | null {
  if (!rawMessage || typeof rawMessage !== 'object') return null;
  const envelope = rawMessage as Record<string, unknown>;
  const req = envelope.req;

  if (typeof envelope.__twe_msg_revision_v1 === 'number') {
    return Number(envelope.__twe_msg_revision_v1);
  }

  if (
    req &&
    typeof req === 'object' &&
    typeof (req as { __twe_hook_revision_v1?: unknown }).__twe_hook_revision_v1 === 'number'
  ) {
    return Number((req as { __twe_hook_revision_v1?: unknown }).__twe_hook_revision_v1);
  }

  return null;
}

function createEndpointMetrics(): EndpointHookMetrics {
  return {
    received: 0,
    processed: 0,
    skippedDuplicate: 0,
    newUniqueTweets: 0,
    legacyShape: 0,
    missingContext: 0,
    lastAt: 0,
    lastStatus: 0,
    lastUrl: '',
  };
}

function extractBookmarksEndpoint(url: string): string {
  try {
    const parsed = new URL(url, 'https://x.com');
    const path = parsed.pathname.toLowerCase();
    const graphqlMatch = path.match(/\/graphql\/[^/]+\/([^/?#]+)/);
    if (graphqlMatch?.[1]) {
      return `graphql:${graphqlMatch[1]}`;
    }
    const apiMatch = path.match(/\/i\/api\/1\.1\/([^/?#]+)/);
    if (apiMatch?.[1]) {
      return `api:${apiMatch[1]}`;
    }
  } catch {
    // ignore
  }
  return 'other';
}

function countUniqueTweetIds(responseText: string): number {
  const found = new Set<string>();
  const regex = /"rest_id"\s*:\s*"(\d{10,})"/g;
  for (;;) {
    const match = regex.exec(responseText);
    if (!match) break;
    if (match[1]) {
      found.add(match[1]);
    }
  }
  return found.size;
}

function serializeRequestBodyText(body: unknown): string | undefined {
  try {
    if (!body) return undefined;

    if (typeof body === 'string') {
      return body;
    }

    if (typeof URLSearchParams !== 'undefined' && body instanceof URLSearchParams) {
      return body.toString();
    }

    if (typeof FormData !== 'undefined' && body instanceof FormData) {
      try {
        const entries = [...body.entries()]
          .map(([name, value]) => `${name}=${String(value).slice(0, 200)}`)
          .join('&');
        return entries;
      } catch {
        return undefined;
      }
    }

    if (typeof Blob !== 'undefined' && body instanceof Blob) {
      return `blob:${body.type || 'application/octet-stream'}:${body.size}`;
    }

    return undefined;
  } catch {
    // Cross-compartment objects in Firefox can throw on instanceof checks.
    return undefined;
  }
}

function captureBookmarkRouteFromUrl(
  url: string,
): { folderId: string | null; pageUrl: string } | null {
  try {
    const u = new URL(url, 'https://x.com');
    const match = u.pathname.match(/\/bookmarks\/(\d+)/);
    return { folderId: match?.[1] ?? null, pageUrl: u.href };
  } catch {
    return null;
  }
}

function coerceBookmarkFolderId(value: unknown): string | null {
  if (typeof value === 'number' && Number.isFinite(value)) {
    const normalized = String(Math.trunc(value));
    return /^\d+$/.test(normalized) ? normalized : null;
  }
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  return /^\d+$/.test(trimmed) ? trimmed : null;
}

function isLikelyBookmarkFolderKey(key: string): boolean {
  const normalized = key.toLowerCase().replace(/[^a-z0-9]/g, '');
  return /^(bookmarkcollectionid|bookmarkfolderid|bookmarkcollection|folderid|collectionid|bookmarkfolder|bookmarkcollectionid)/.test(
    normalized,
  );
}

function findFolderIdInUnknownValue(
  value: unknown,
  depth = 0,
  seen = new Set<object>(),
): { folderId: string; pageUrl?: string } | null {
  if (!value || depth > BOOKMARK_CONTEXT_SCAN_DEPTH) return null;

  if (typeof value === 'string') {
    const fromUrl = captureBookmarkRouteFromUrl(value);
    if (fromUrl?.folderId) return { folderId: fromUrl.folderId, pageUrl: fromUrl.pageUrl };
    const fallback = value.match(
      /(?:bookmark|folder|collection)[^\w]{0,20}(?:id|_id|Id|_Id)\W*[:=]\W*["']?(\d{5,})["']?/i,
    );
    if (fallback?.[1]) return { folderId: fallback[1] };
    return null;
  }

  if (typeof value !== 'object') return null;

  if (Array.isArray(value)) {
    for (const item of value) {
      const found = findFolderIdInUnknownValue(item, depth + 1, seen);
      if (found?.folderId) return found;
    }
    return null;
  }

  const obj = value as Record<string, unknown>;
  if (seen.has(obj)) return null;
  seen.add(obj);

  for (const [key, nested] of Object.entries(obj)) {
    const fromKey = isLikelyBookmarkFolderKey(key) ? coerceBookmarkFolderId(nested) : null;
    if (fromKey) {
      const asText = typeof nested === 'string' ? nested : '';
      const fromText = asText ? captureBookmarkRouteFromUrl(asText) : null;
      if (fromText?.folderId) return { folderId: fromText.folderId, pageUrl: fromText.pageUrl };
      return { folderId: fromKey };
    }

    const found = findFolderIdInUnknownValue(nested, depth + 1, seen);
    if (found?.folderId) return found;
  }

  return null;
}

type BookmarkRouteCandidate = {
  folderId: string | null;
  pageUrl: string;
  source: string;
  confidence: number;
};

const BOOKMARK_QUERY_FOLDER_KEYS = [
  'bookmark_collection_id',
  'bookmarkcollectionid',
  'bookmarkCollectionId',
  'folder_id',
  'folderid',
  'folderId',
  'collection_id',
  'collectionid',
  'collectionId',
];

function extractFolderIdFromRequestVariables(raw: string | null): string | null {
  if (!raw) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(decodeURIComponent(raw));
  } catch {
    try {
      parsed = JSON.parse(raw);
    } catch {
      return null;
    }
  }

  const found = findFolderIdInUnknownValue(parsed);
  if (found?.folderId) return found.folderId;

  if (parsed && typeof parsed === 'object') {
    const obj = parsed as Record<string, unknown>;
    for (const key of BOOKMARK_QUERY_FOLDER_KEYS) {
      const value = obj[key];
      const folderId = coerceBookmarkFolderId(value);
      if (folderId) return folderId;
    }
  }

  return null;
}

function extractFolderIdFromBookmarkRequestUrl(url: string): string | null {
  try {
    const u = new URL(url, 'https://x.com');
    const directQueryId = BOOKMARK_QUERY_FOLDER_KEYS.map((key) => u.searchParams.get(key)).find(
      (value) => !!value && /^\d+$/.test(value),
    );
    if (directQueryId) {
      return directQueryId;
    }

    const fromVariables = extractFolderIdFromRequestVariables(u.searchParams.get('variables'));
    if (fromVariables) {
      return fromVariables;
    }

    return null;
  } catch {
    return null;
  }
}

function extractFolderIdFromBookmarkRequestBody(body: string | null | undefined): string | null {
  if (!body) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    try {
      const form = new URLSearchParams(body);
      const formVariables = form.get('variables');
      if (formVariables) {
        parsed = JSON.parse(formVariables);
      } else {
        return null;
      }
    } catch {
      return null;
    }
  }

  const found = findFolderIdInUnknownValue(parsed);
  if (found?.folderId) return found.folderId;
  return null;
}

function extractBookmarkFolderIdFromPath(pathOrUrl: string): string | null {
  try {
    const path = new URL(pathOrUrl, location.href).pathname;
    const match = path.match(/\/bookmarks\/(\d+)/);
    return match?.[1] ?? null;
  } catch {
    return null;
  }
}

function normalizeBookmarkTabCandidateSource(value: string | null): string {
  if (!value) return 'bookmark-tab';
  return value;
}

function captureBookmarkRouteFromNavigationArgs(args: unknown[]): BookmarkRouteCandidate | null {
  if (!args.length) return null;
  const stateArg = args[0];
  const urlArg = args[2];
  const urlSource =
    typeof urlArg === 'string'
      ? urlArg
      : typeof urlArg === 'object' && urlArg
        ? String(urlArg)
        : null;
  if (urlSource) {
    const fromUrl = captureBookmarkRouteFromUrl(urlSource);
    if (fromUrl?.folderId) {
      return {
        folderId: fromUrl.folderId,
        pageUrl: fromUrl.pageUrl,
        source: 'history-url',
        confidence: 95,
      };
    }
  }

  const fromState = findFolderIdInUnknownValue(stateArg);
  if (fromState?.folderId) {
    const pageUrl =
      fromState.pageUrl ||
      (urlSource && captureBookmarkRouteFromUrl(urlSource)?.pageUrl
        ? captureBookmarkRouteFromUrl(urlSource)!.pageUrl
        : '');
    return {
      folderId: fromState.folderId,
      pageUrl: pageUrl || (typeof location !== 'undefined' ? location.href : ''),
      source: 'history-state',
      confidence: 90,
    };
  }

  return null;
}

function captureBookmarkRouteFromPerformanceNavigation(): BookmarkRouteCandidate | null {
  try {
    if (typeof performance === 'undefined' || typeof performance.getEntriesByType !== 'function') {
      return null;
    }

    const entries = performance.getEntriesByType('navigation') as PerformanceEntry[];
    const candidateEntry = entries[entries.length - 1];
    if (!candidateEntry || !candidateEntry.name) return null;
    const parsed = captureBookmarkRouteFromUrl(candidateEntry.name);
    if (!parsed?.folderId) return null;
    return {
      folderId: parsed.folderId,
      pageUrl: parsed.pageUrl,
      source: 'performance',
      confidence: 88,
    };
  } catch {
    return null;
  }
}

function captureBookmarkRouteFromBookmarkTabs(): BookmarkRouteCandidate | null {
  if (typeof document === 'undefined') return null;

  const selectors = [
    '[role="tab"] a[href*="/i/bookmarks/"]',
    'a[role="tab"][href*="/i/bookmarks/"]',
    '[role="tablist"] a[href*="/i/bookmarks/"]',
    'a[href*="/i/bookmarks/"]',
    // Additional selectors for X's current DOM structure
    'nav a[href*="/bookmarks/"]',
    '[data-testid="primaryColumn"] a[href*="/bookmarks/"]',
    'a[href*="/bookmarks/"]',
  ];
  const activeTabs: Array<{ folderId: string; pageUrl: string; source: string; score: number }> =
    [];
  const allTabs: Array<{ folderId: string; pageUrl: string; source: string; score: number }> = [];
  const seen = new Set<string>();

  for (const selector of selectors) {
    const nodes = Array.from(document.querySelectorAll(selector)) as Element[];
    for (const node of nodes) {
      if (!(node instanceof HTMLAnchorElement)) continue;
      const rawHref = node.getAttribute('href');
      if (!rawHref) continue;

      const folderId = extractBookmarkFolderIdFromPath(rawHref);
      if (!folderId) {
        continue;
      }
      const safeFolderId = folderId;
      if (seen.has(safeFolderId)) continue;
      seen.add(safeFolderId);

      const hrefUrl = (() => {
        try {
          return new URL(rawHref, location.href).href;
        } catch {
          return rawHref;
        }
      })();
      const anchorOrFallback = node.closest('a[href]') || node;

      const attr = (el: Element | null, name: string) => el?.getAttribute(name);
      const attrBool = (el: Element | null, name: string) => attr(el, name) === 'true';

      const isActive =
        attrBool(anchorOrFallback, 'aria-selected') ||
        attrBool(anchorOrFallback, 'aria-current') ||
        attrBool(anchorOrFallback, 'data-selected') ||
        attrBool(anchorOrFallback, 'data-state');
      const isInsideTabList = !!anchorOrFallback.closest('[role="tablist"]');
      const isAnchorTab =
        anchorOrFallback instanceof HTMLAnchorElement &&
        (anchorOrFallback.role === 'tab' || !!anchorOrFallback.closest('[role="tab"]'));

      let style: CSSStyleDeclaration | null = null;
      try {
        style = window.getComputedStyle(anchorOrFallback);
      } catch {
        // ignore
      }
      const isVisible = style
        ? style.display !== 'none' && style.visibility !== 'hidden' && style.opacity !== '0'
        : true;

      let score = 0;
      if (isActive) score += 20;
      if (isInsideTabList) score += 5;
      if (isAnchorTab) score += 3;
      if (isVisible) score += 2;

      const tabInfo = {
        folderId: safeFolderId,
        pageUrl: hrefUrl,
        source: normalizeBookmarkTabCandidateSource(
          anchorOrFallback.getAttribute('data-testid') ?? (isAnchorTab ? 'bookmark-tab' : null),
        ),
        score,
      };

      allTabs.push(tabInfo);
      if (isActive) {
        activeTabs.push(tabInfo);
      }
    }
  }

  // Prefer explicitly active tabs, but fall back to all tabs with scoring
  const picked = activeTabs.length > 0 ? activeTabs : allTabs;
  if (!picked.length) return null;

  picked.sort((a, b) => {
    if (a.score !== b.score) return b.score - a.score;
    return a.folderId.localeCompare(b.folderId);
  });

  const top = picked[0];
  if (!top) return null;
  return {
    folderId: top.folderId,
    pageUrl: top.pageUrl,
    source: top.source,
    confidence: Math.max(
      activeTabs.length > 0 ? 45 : 25,
      top.score + (activeTabs.length > 0 ? 45 : 25),
    ),
  };
}

function captureBookmarkRouteFromHistoryState(): BookmarkRouteCandidate | null {
  try {
    const state = hookGlobalObject.history?.state;
    if (!state) return null;
    const found = findFolderIdInUnknownValue(state);
    if (!found?.folderId) return null;
    const pageUrl = found.pageUrl || (typeof location !== 'undefined' ? location.href : '');
    return { folderId: found.folderId, pageUrl, source: 'history-state', confidence: 86 };
  } catch {
    return null;
  }
}

function captureBookmarkRouteFromGlobalState(): BookmarkRouteCandidate | null {
  const stateSources = [
    '__INITIAL_STATE__',
    '__NEXT_DATA__',
    '__INITIAL_PROPS__',
    '__NEXT_REDUX_STATE__',
    '__META_DATA__',
  ];
  const candidates: Array<{ source: string; value: unknown }> = [];
  const globalObj = hookGlobalObject as Record<string, unknown>;

  for (const key of stateSources) {
    const value = globalObj[key];
    if (!value) continue;
    candidates.push({ source: key, value });
  }

  for (const { source, value } of candidates) {
    const found = findFolderIdInUnknownValue(value);
    if (!found?.folderId) continue;
    return {
      folderId: found.folderId,
      pageUrl: found.pageUrl || (typeof location !== 'undefined' ? location.href : ''),
      source,
      confidence: 82,
    };
  }

  return null;
}

function captureBookmarkRouteFromEventTarget(
  target: EventTarget | null,
): BookmarkRouteCandidate | null {
  if (!target || typeof target !== 'object') return null;

  const candidates: Element[] = [];
  const seen = new Set<Element>();
  const push = (candidate: unknown) => {
    if (candidate instanceof Element && !seen.has(candidate)) {
      seen.add(candidate);
      candidates.push(candidate);
    }
  };

  push(target);
  const targetWithPath = target as { composedPath?: () => EventTarget[] };
  if (typeof targetWithPath.composedPath === 'function') {
    const path = targetWithPath.composedPath() || [];
    path.forEach((item) => {
      push(item);
    });
  }
  const directTarget = target as { currentTarget?: unknown; target?: unknown };
  push(directTarget.target);
  push(directTarget.currentTarget);

  for (const node of candidates) {
    const anchor = node.closest?.('a[href]');
    const targetNode = (
      node instanceof HTMLAnchorElement ? node : anchor
    ) as HTMLAnchorElement | null;
    if (!targetNode) continue;

    const href = targetNode.getAttribute('href');
    if (!href || !href.includes('/i/bookmarks/')) {
      continue;
    }

    const parsed = captureBookmarkRouteFromUrl(href);
    if (!parsed?.folderId) continue;
    return {
      folderId: parsed.folderId,
      pageUrl: parsed.pageUrl,
      source: 'bookmark-click',
      confidence: 92,
    };
  }

  return null;
}

function isBookmarksApiRequest(url: string): boolean {
  try {
    const path = new URL(url, 'https://x.com').pathname.toLowerCase();
    return /(bookmarks|bookmarkfolderslice|bookmarkfoldertimeline|bookmarkcollectiontimeline|bookmarkcollectionstimeline)/.test(
      path,
    );
  } catch {
    return false;
  }
}

function resolveRequestBookmarkContext(
  url: string,
  bodyText?: string,
  request?: BookmarkRequestSource,
): BookmarkContextPayload {
  const now = Date.now();
  const pageUrl = typeof location !== 'undefined' ? location.href : '';
  const pageCandidate = resolveCanonicalRouteFromUrl(pageUrl);

  if (isBookmarksApiRequest(url)) {
    const fromRequest = extractFolderIdFromBookmarkRequestUrl(url);
    if (fromRequest) {
      return {
        folderId: fromRequest,
        pageUrl,
        source: 'request-url',
        capturedAt: now,
        requestId: request?.requestId,
        routeSource: 'request-url',
        pageRouteUrl: pageCandidate?.pageUrl,
      };
    }

    const fromBody = extractFolderIdFromBookmarkRequestBody(bodyText);
    if (fromBody) {
      return {
        folderId: fromBody,
        pageUrl,
        source: 'request-body',
        capturedAt: now,
        requestId: request?.requestId,
        routeSource: 'request-body',
        pageRouteUrl: pageCandidate?.pageUrl,
      };
    }

    if (pageCandidate?.folderId) {
      return {
        folderId: pageCandidate.folderId,
        pageUrl: pageCandidate.pageUrl || pageUrl,
        source: 'page-route',
        capturedAt: now,
        requestId: request?.requestId,
        routeSource: 'location',
        pageRouteUrl: pageCandidate.pageUrl || pageUrl,
      };
    }

    // Lock is intentionally lower priority than request-derived signals.
    // We only consult it after request URL/body and page route candidates are exhausted.
    const lock = getBookmarkContextLock(now);
    if (lock?.folderId && isBookmarksRoute(pageUrl)) {
      return {
        folderId: lock.folderId,
        pageUrl: lock.pageUrl || pageUrl,
        source: lock.source || 'active-context-lock',
        capturedAt: now,
        requestId: request?.requestId,
        routeSource: 'active-context-lock',
        pageRouteUrl: lock.pageUrl || pageUrl,
      };
    }

    const pageUrlFresh = typeof location !== 'undefined' ? location.href : pageUrl;
    if (
      activeBookmarkContext?.folderId &&
      now - activeBookmarkContext.capturedAt <= BOOKMARK_CONTEXT_BOOKMARKS_ONLY_STALE_MS
    ) {
      return {
        folderId: activeBookmarkContext.folderId,
        pageUrl: activeBookmarkContext.pageUrl || pageUrlFresh,
        source: activeBookmarkContext.source || 'active-context',
        capturedAt: now,
        requestId: request?.requestId,
        routeSource: 'active-context',
        pageRouteUrl: activeBookmarkContext.pageUrl || pageUrlFresh,
      };
    }

    const fromPage = captureBookmarkRouteFromPage();
    if (fromPage?.folderId) {
      return {
        folderId: fromPage.folderId,
        pageUrl: fromPage.pageUrl,
        source: fromPage.source,
        capturedAt: now,
        requestId: request?.requestId,
        routeSource: fromPage.source,
        pageRouteUrl: fromPage.pageUrl,
      };
    }
  }

  return normalizeBookmarkContextValue(activeBookmarkContext, {
    method: request?.method || 'GET',
    url: pageUrl,
    requestId: request?.requestId,
    hasBody: !!request?.body,
  });
}

function normalizeBookmarkContextValue(
  raw: unknown,
  request?: BookmarkRequestSource & { hasBody?: boolean },
): BookmarkContextPayload {
  const fallbackUrl =
    typeof location !== 'undefined'
      ? location.href
      : typeof document !== 'undefined'
        ? document.URL
        : '';
  const fallback = {
    folderId: null as string | null,
    pageUrl: fallbackUrl,
    source: 'fallback',
    capturedAt: Date.now(),
    requestId: request?.requestId,
  };
  if (!raw) {
    const found = captureBookmarkRouteFromPage();
    return {
      folderId: found?.folderId ?? null,
      pageUrl: found?.pageUrl ?? fallback.pageUrl,
      source: found?.source ?? fallback.source,
      requestId: request?.requestId,
      routeSource: found?.source,
      pageRouteUrl: found?.pageUrl,
      capturedAt: Date.now(),
    };
  }

  if (typeof raw === 'string') {
    const trimmed = raw.trim();
    if (!trimmed) return fallback;
    if (/^\d+$/.test(trimmed)) {
      return {
        folderId: trimmed,
        pageUrl: fallback.pageUrl,
        source: 'string-id',
        capturedAt: Date.now(),
        requestId: request?.requestId,
      };
    }
    const fromRaw = captureBookmarkRouteFromUrl(trimmed);
    return {
      folderId: fromRaw?.folderId ?? null,
      pageUrl: fromRaw?.pageUrl ?? fallback.pageUrl,
      source: 'raw-string',
      capturedAt: Date.now(),
      requestId: request?.requestId,
      routeSource: fromRaw?.folderId ? 'string-id' : 'fallback',
      pageRouteUrl: fromRaw?.pageUrl,
    };
  }

  if (typeof raw === 'object') {
    const asObj = raw as Record<string, unknown>;
    const candidates = [
      asObj.folderUrl,
      asObj.pageUrl,
      asObj.url,
      asObj.location,
      asObj.currentUrl,
      asObj.pageUrlBase64,
    ]
      .map((value): unknown => value)
      .filter((value): value is string => typeof value === 'string' && value.length > 0);
    const now = Date.now();
    const candidate = candidates
      .map((value) => captureBookmarkRouteFromUrl(value))
      .find((value): value is { folderId: string | null; pageUrl: string } => !!value?.folderId);
    const directFolderId =
      typeof asObj.folderId === 'string' || typeof asObj.folderId === 'number'
        ? String(asObj.folderId)
        : null;
    const pageUrl =
      typeof asObj.pageUrl === 'string' && asObj.pageUrl.length > 0
        ? asObj.pageUrl
        : typeof asObj.url === 'string' && asObj.url.length > 0
          ? asObj.url
          : fallback.pageUrl;

    if (candidate?.folderId) {
      return {
        folderId: candidate.folderId,
        pageUrl: candidate.pageUrl,
        source:
          typeof asObj.source === 'string' && asObj.source.length > 0 ? asObj.source : 'object',
        capturedAt: typeof asObj.capturedAt === 'number' ? asObj.capturedAt : now,
        requestId: request?.requestId,
        routeSource:
          typeof asObj.source === 'string' && asObj.source.length > 0 ? asObj.source : 'object',
        pageRouteUrl: pageUrl,
      };
    }

    if (directFolderId && /^\d+$/.test(directFolderId)) {
      return {
        folderId: directFolderId,
        pageUrl,
        source:
          typeof asObj.source === 'string' && asObj.source.length > 0 ? asObj.source : 'object',
        capturedAt: typeof asObj.capturedAt === 'number' ? asObj.capturedAt : now,
        requestId: request?.requestId,
        routeSource:
          typeof asObj.source === 'string' && asObj.source.length > 0 ? asObj.source : 'object',
        pageRouteUrl: pageUrl,
      };
    }
  }

  const fromLocation = captureBookmarkRouteFromPage();
  return {
    folderId: fromLocation?.folderId ?? null,
    pageUrl: fromLocation?.pageUrl ?? fallback.pageUrl,
    source: 'fallback',
    capturedAt: Date.now(),
    requestId: request?.requestId,
    routeSource: fromLocation?.source,
    pageRouteUrl: fromLocation?.pageUrl,
  };
}

function captureBookmarkRouteFromPage(): BookmarkRouteCandidate | null {
  const candidates: BookmarkRouteCandidate[] = [];

  const tabCandidate = captureBookmarkRouteFromBookmarkTabs();
  if (tabCandidate) candidates.push(tabCandidate);
  const historyCandidate = captureBookmarkRouteFromHistoryState();
  if (historyCandidate) candidates.push(historyCandidate);
  const navigationCandidate = captureBookmarkRouteFromPerformanceNavigation();
  if (navigationCandidate) candidates.push(navigationCandidate);
  const globalCandidate = captureBookmarkRouteFromGlobalState();
  if (globalCandidate) candidates.push(globalCandidate);

  const bestFolderCandidate = candidates
    .filter((candidate) => !!candidate.folderId)
    .sort((a, b) => b.confidence - a.confidence)[0];
  if (bestFolderCandidate) {
    return {
      folderId: bestFolderCandidate.folderId,
      pageUrl: bestFolderCandidate.pageUrl,
      source: bestFolderCandidate.source,
      confidence: bestFolderCandidate.confidence,
    };
  }

  const urlCandidates: Array<{ source: string; url: string }> = [];
  const locationUrl = typeof location !== 'undefined' ? location.href : '';
  const documentUrl = typeof document !== 'undefined' ? document.URL : '';
  const canonical =
    typeof document !== 'undefined'
      ? (document.querySelector('link[rel="canonical"]') as HTMLLinkElement | null)
      : null;
  const og =
    typeof document !== 'undefined'
      ? (document.querySelector('meta[property="og:url"]') as HTMLMetaElement | null)
      : null;

  if (locationUrl) urlCandidates.push({ source: 'location', url: locationUrl });
  if (documentUrl && documentUrl !== locationUrl)
    urlCandidates.push({ source: 'document', url: documentUrl });
  if (canonical?.href) urlCandidates.push({ source: 'canonical', url: canonical.href });
  if (og?.content) urlCandidates.push({ source: 'og', url: og.content });

  for (const candidate of urlCandidates) {
    const parsed = captureBookmarkRouteFromUrl(candidate.url);
    if (parsed?.folderId) {
      return { ...parsed, source: candidate.source, confidence: 30 };
    }
  }

  const firstCandidate = urlCandidates[0];
  if (!firstCandidate) return null;
  const firstUrl = firstCandidate.url;
  if (!firstUrl) return null;
  return {
    folderId: null,
    pageUrl: firstUrl,
    source: firstCandidate.source,
    confidence: 14,
  };
}

function isBookmarksRoute(url: string): boolean {
  try {
    return /\/i\/bookmarks(?:\/|$)/.test(new URL(url, 'https://x.com').pathname);
  } catch {
    return false;
  }
}

function setBookmarkContext(value: unknown): void {
  const payload = normalizeBookmarkContextValue(value);
  activeBookmarkContext = payload;
  if (payload.folderId) {
    setBookmarkContextLock(payload);
  } else if (!isBookmarksRoute(payload.pageUrl)) {
    clearBookmarkContextLock();
  }
  try {
    // The same value needs to be visible from both content and page realms.
    (hookGlobalObject as Record<string, unknown>)[BOOKMARK_CONTEXT_KEY] = payload;
  } catch {
    // ignore
  }
  try {
    (window as unknown as Record<string, unknown>)[BOOKMARK_CONTEXT_KEY] = payload;
  } catch {
    // ignore
  }
  try {
    (globalThis as Record<string, unknown>)[BOOKMARK_CONTEXT_KEY] = payload;
  } catch {
    // ignore
  }
}

function defineOn(target: object, name: string, fn: unknown): boolean {
  try {
    if (typeof fn !== 'function') {
      return false;
    }
    const hookFn = fn as HookCallable;
    const exportFunctionMaybe = getExportFunctionMaybe();
    if (exportFunctionMaybe) {
      exportFunctionMaybe(hookFn, target, { defineAs: name });
      return true;
    }
    (target as Record<string, unknown>)[name] = hookFn;
    return true;
  } catch (err) {
    logger.error(`Failed to define ${name} hook`, err);
    return false;
  }
}

function getFunctionFromHookState(
  candidate: unknown,
  marker: '__twe_is_hook_open_v1' | '__twe_is_hook_send_v1' | '__twe_is_hook_fetch_v1',
  originalKey: string,
): unknown {
  if (!candidate || typeof candidate !== 'function') {
    return undefined;
  }

  let current = candidate as unknown as Record<string, unknown>;
  if (!hasHookShape(candidate, marker)) {
    return candidate;
  }

  for (let i = 0; i < 10; i++) {
    const nested = current[originalKey];
    if (!nested || typeof nested !== 'function') {
      return undefined;
    }
    if (!hasHookShape(nested, marker)) {
      return nested;
    }
    current = nested as unknown as Record<string, unknown>;
  }
  return undefined;
}

function postHookMessage(payload: unknown): void {
  const messageTargetOrigin = '*';
  try {
    if (
      typeof payload !== 'object' ||
      payload === null ||
      (payload as { __twe_mcp_hook_v1?: unknown }).__twe_mcp_hook_v1 !== true
    ) {
      return;
    }

    const data = payload as {
      req?: {
        __twe_hook_revision_v1?: number;
      };
    };
    if (data.req && typeof data.req === 'object') {
      data.req = buildNormalizedHookMessageRequest(data.req);
    }
    payload = {
      ...(payload as Record<string, unknown>),
      __twe_msg_revision_v1: HOOK_REVISION,
    };

    const postMessageOnHookGlobal = (hookGlobalObject as Record<string, unknown>).postMessage as (
      message: unknown,
      targetOrigin: string,
    ) => void | undefined;
    postMessageOnHookGlobal?.(payload, messageTargetOrigin);
    return;
  } catch {
    // ignore
  }

  try {
    const postMessageOnGlobalThis = (globalThis as Record<string, unknown>).postMessage as (
      message: unknown,
      targetOrigin: string,
    ) => void | undefined;
    postMessageOnGlobalThis?.(payload, messageTargetOrigin);
  } catch {
    // ignore
  }
}

function addEventListenerSafe(
  target: unknown,
  type: string,
  listener: EventListenerOrEventListenerObject,
  options?: AddEventListenerOptions | boolean,
): boolean {
  if (!target || (typeof target !== 'object' && typeof target !== 'function')) {
    return false;
  }
  const addEventListener = (target as { addEventListener?: unknown }).addEventListener;
  if (typeof addEventListener !== 'function') {
    return false;
  }
  try {
    addEventListener.call(target, type, listener, options);
    return true;
  } catch {
    return false;
  }
}

/**
 * The registry for all extensions.
 */
export class ExtensionManager {
  private extensions: Map<string, Extension> = new Map();
  private disabledExtensions: Set<string> = new Set();
  private debugEnabled = false;
  private hookStats: HookStats | null = null;
  private hookRuntime: HookStats | null = null;
  private recentResponseSigs: Map<string, RecentSig> = new Map();
  private lastStickyBookmarkContext: BookmarkContextPayload | null = null;
  private hookRepairInterval: ReturnType<typeof setTimeout> | null = null;
  private hookRepairBackoffMs = HOOK_REPAIR_INTERVAL_MS;
  private hookRepairFailures = 0;
  private runtimeModes: RuntimeModes = resolveRuntimeModes();
  private runtimeModeReason = '';
  private pageMessageHandler: ((event: MessageEvent<unknown>) => void) | null = null;
  private instanceId = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
  public readonly __twe_extension_manager_signature_v1 = EXTENSION_MANAGER_SIGNATURE;
  public readonly __twe_extension_manager_revision_v1 = EXTENSION_MANAGER_REVISION;
  public readonly __twe_extension_manager_started_at_v1 = Date.now();
  private disposed = false;
  private endpointMetricLimit = 40;

  /**
   * Signal for subscribing to extension changes.
   */
  public signal = new Signal(1);

  private isHookModeEnabled(target: 'xhr' | 'fetch'): boolean {
    if (this.runtimeModes.safeMode) {
      return false;
    }
    if (this.runtimeModes.hookMode === 'off') {
      return false;
    }
    if (this.runtimeModes.hookMode === 'both') {
      return true;
    }
    return this.runtimeModes.hookMode === target;
  }

  private persistRuntimeModes() {
    writeLocalStorageValue(LOCAL_STORAGE_SAFE_MODE_KEY, this.runtimeModes.safeMode ? '1' : '0');
    writeLocalStorageValue(LOCAL_STORAGE_HOOK_MODE_KEY, this.runtimeModes.hookMode);
    writeLocalStorageValue(LOCAL_STORAGE_REPAIR_MODE_KEY, this.runtimeModes.repairMode);
    try {
      options.set('safeMode', this.runtimeModes.safeMode);
      options.set('hookMode', this.runtimeModes.hookMode);
      options.set('repairMode', this.runtimeModes.repairMode);
    } catch {
      // ignore
    }
  }

  private publishRuntimeModes(extra?: Record<string, unknown>) {
    const payload = {
      safeMode: this.runtimeModes.safeMode,
      hookMode: this.runtimeModes.hookMode,
      repairMode: this.runtimeModes.repairMode,
      reason: this.runtimeModeReason || undefined,
      ...extra,
    };
    try {
      (globalThis as Record<string, unknown>)[RUNTIME_MODES_KEY] = payload;
    } catch {
      // ignore
    }
    try {
      (hookGlobalObject as Record<string, unknown>)[RUNTIME_MODES_KEY] = payload;
    } catch {
      // ignore
    }
  }

  private runHookSelfTest(): { ok: boolean; error?: string } {
    try {
      if (this.runtimeModes.safeMode || this.runtimeModes.hookMode === 'off') {
        return { ok: true };
      }
      if (this.isHookModeEnabled('xhr')) {
        const xhrCtor = hookGlobalObject.XMLHttpRequest;
        if (!xhrCtor?.prototype) {
          return { ok: false, error: 'XMLHttpRequest.prototype unavailable' };
        }
        if (typeof xhrCtor.prototype.open !== 'function') {
          return { ok: false, error: 'XMLHttpRequest.prototype.open unavailable' };
        }
        if (typeof xhrCtor.prototype.send !== 'function') {
          return { ok: false, error: 'XMLHttpRequest.prototype.send unavailable' };
        }
      }
      if (this.isHookModeEnabled('fetch')) {
        const fetchCandidate = (hookGlobalObject as Record<string, unknown>).fetch;
        if (typeof fetchCandidate !== 'function') {
          return { ok: false, error: 'fetch unavailable' };
        }
      }
      return { ok: true };
    } catch (err) {
      return { ok: false, error: String(err) };
    }
  }

  private enableSafeMode(reason: string, error?: unknown) {
    this.runtimeModes.safeMode = true;
    this.runtimeModeReason = reason;
    this.persistRuntimeModes();
    this.publishRuntimeModes({
      enabledAt: Date.now(),
      error: error ? String(error) : undefined,
    });
    this.uninstallHooks();
    if (this.hookRepairInterval !== null) {
      clearTimeout(this.hookRepairInterval);
      this.hookRepairInterval = null;
    }
    logger.error(`Hook safe mode enabled (${reason})`, error ?? '');
  }

  private refreshRuntimeModes() {
    this.runtimeModes = resolveRuntimeModes();
    this.persistRuntimeModes();
    this.publishRuntimeModes();
  }

  public dispose(): void {
    if (this.disposed) return;
    this.disposed = true;

    try {
      this.getExtensions().forEach((ext) => {
        try {
          if (ext.enabled) {
            ext.enabled = false;
            ext.dispose();
          }
        } catch {
          // ignore
        }
      });
    } catch {
      // ignore
    }

    if (this.hookRepairInterval !== null) {
      clearTimeout(this.hookRepairInterval);
      this.hookRepairInterval = null;
    }

    if (this.pageMessageHandler) {
      try {
        window.removeEventListener('message', this.pageMessageHandler, false);
      } catch {
        // ignore
      }
      this.pageMessageHandler = null;
    }

    this.uninstallHooks();
  }

  private syncRuntimeStats() {
    if (!this.hookStats || !this.hookRuntime) return;
    this.hookRuntime.messagesTotal = this.hookStats.messagesTotal;
    this.hookRuntime.messagesLegacyShape = this.hookStats.messagesLegacyShape;
    this.hookRuntime.messagesMissingContext = this.hookStats.messagesMissingContext;
    this.hookRuntime.messagesRepairedAtBridge = this.hookStats.messagesRepairedAtBridge;
    this.hookRuntime.messagesMissingBody = this.hookStats.messagesMissingBody;
    this.hookRuntime.responsesProcessed = this.hookStats.responsesProcessed;
    this.hookRuntime.responsesSkippedDuplicate = this.hookStats.responsesSkippedDuplicate;
    this.hookRuntime.lastMessageAt = this.hookStats.lastMessageAt;
    this.hookRuntime.activeInstanceId = this.hookStats.activeInstanceId;
    this.hookRuntime.rev = this.hookStats.rev;
    this.hookRuntime.endpointStats = this.hookStats.endpointStats;
  }

  private getEndpointStats(key: string): EndpointHookMetrics {
    if (!this.hookStats) return createEndpointMetrics();

    const existing = this.hookStats.endpointStats[key];
    if (existing) return existing;

    if (Object.keys(this.hookStats.endpointStats).length >= this.endpointMetricLimit) {
      const keys = Object.keys(this.hookStats.endpointStats);
      const oldest = keys[0];
      if (oldest) {
        delete this.hookStats.endpointStats[oldest];
      }
    }

    const next = createEndpointMetrics();
    this.hookStats.endpointStats[key] = next;
    return next;
  }

  public uninstallHooks() {
    try {
      if (this.hookRepairInterval !== null) {
        clearTimeout(this.hookRepairInterval);
        this.hookRepairInterval = null;
      }

      const g = hookGlobalObject as Record<string, unknown>;
      const xhrCtor = g.XMLHttpRequest as { prototype?: Record<string, unknown> } | undefined;
      const proto = xhrCtor?.prototype;
      if (proto) {
        if (typeof proto[ORIG_XHR_OPEN_KEY] === 'function') {
          proto.open = proto[ORIG_XHR_OPEN_KEY];
        }
        if (typeof proto[ORIG_XHR_SEND_KEY] === 'function') {
          proto.send = proto[ORIG_XHR_SEND_KEY];
        }
        delete proto.__twe_is_hook_open_v1;
        delete proto.__twe_is_hook_send_v1;
        delete proto.__twe_is_hook_revision_v1;
      }

      if (typeof g[ORIG_FETCH_KEY] === 'function') {
        g.fetch = g[ORIG_FETCH_KEY];
      }
      if (typeof g.fetch === 'object' && g.fetch !== null) {
        const fetchFn = g.fetch as Record<string, unknown>;
        delete fetchFn.__twe_is_hook_fetch_v1;
        delete fetchFn.__twe_is_hook_revision_v1;
      }
      const runtime = g[HOOK_RUNTIME_KEY] as { uninstall?: () => void };
      if (runtime?.uninstall === this.uninstallHooks) {
        delete g[HOOK_RUNTIME_KEY];
        if (typeof window !== 'undefined') {
          delete (window as unknown as Record<string, unknown>)[HOOK_RUNTIME_KEY];
        }
        delete (globalThis as unknown as Record<string, unknown>)[HOOK_RUNTIME_KEY];
      }
    } catch {
      // ignore
    }
  }

  constructor() {
    this.refreshRuntimeModes();
    try {
      this.installPageMessageBridge();
    } catch (err) {
      this.enableSafeMode('install-page-message-bridge-failed', err);
    }
    try {
      this.installBookmarkContextTracking();
    } catch (err) {
      const details =
        err instanceof Error ? `${err.name}: ${err.message}` : `unknown error: ${String(err)}`;
      logger.warn(
        `Bookmark context tracking install failed; continuing without tracker (${details})`,
      );
    }
    try {
      if (this.isHookModeEnabled('xhr')) {
        this.installHttpHooks();
      }
      if (this.isHookModeEnabled('fetch')) {
        this.installFetchHooks();
      }
      const hookSelfTest = this.runHookSelfTest();
      if (!hookSelfTest.ok) {
        this.enableSafeMode('hook-self-test-failed', hookSelfTest.error);
      } else if (!this.runtimeModes.safeMode && this.runtimeModes.repairMode !== 'off') {
        this.startHookRepairLoop();
      }
    } catch (err) {
      this.enableSafeMode('hook-install-failed', err);
    }
    this.disabledExtensions = new Set(options.get('disabledExtensions', []));

    // Make manager singleton discoverable in page/context globals for preload cleanup.
    publishExtensionManagerGlobal(this);

    // Do some extra logging when debug mode is enabled.
    if (options.get('debug')) {
      this.debugEnabled = true;
      logger.info('Debug mode enabled');
    }

    clearBootstrapErrorMarker();

    // A small, safe diagnostics object to make hook health inspectable from the console.
    const g = hookGlobalObject as Record<string, unknown>;
    let constructorError: BootstrapErrorReport | null = null;
    try {
      if (!g[HOOK_STATS_KEY]) {
        g[HOOK_STATS_KEY] = createHookStats(this.instanceId) as HookStats;
      }
      const runtimeCandidate = g[HOOK_RUNTIME_KEY];
      const runtimeObject =
        runtimeCandidate && typeof runtimeCandidate === 'object'
          ? (runtimeCandidate as {
              uninstall?: () => void;
              rev?: number;
              __twe_runtime_signature_v1?: string;
            })
          : null;
      if (!runtimeObject?.uninstall || typeof runtimeObject.uninstall !== 'function') {
        g[HOOK_RUNTIME_KEY] = {
          __twe_runtime_signature_v1: HOOK_RUNTIME_SIGNATURE,
          uninstall: () => this.uninstallHooks(),
        };
      } else if (
        runtimeObject.rev !== HOOK_REVISION ||
        runtimeObject.__twe_runtime_signature_v1 !== HOOK_RUNTIME_SIGNATURE
      ) {
        runtimeObject.uninstall();
      }

      if (g[HOOK_STATS_KEY]) {
        const existing = g[HOOK_STATS_KEY] as Partial<HookStats>;
        if (typeof existing.xhrMessages !== 'number') existing.xhrMessages = 0;
        if (typeof existing.fetchMessages !== 'number') existing.fetchMessages = 0;
        if (typeof existing.lastUrl !== 'string') existing.lastUrl = '';
        if (typeof existing.lastAt !== 'number') existing.lastAt = 0;
        if (typeof existing.loggedUrls !== 'number') existing.loggedUrls = 0;
        if (typeof existing.messagesTotal !== 'number') existing.messagesTotal = 0;
        if (typeof existing.messagesLegacyShape !== 'number') existing.messagesLegacyShape = 0;
        if (typeof existing.messagesMissingContext !== 'number')
          existing.messagesMissingContext = 0;
        if (typeof existing.messagesRepairedAtBridge !== 'number')
          existing.messagesRepairedAtBridge = 0;
        if (typeof existing.messagesMissingBody !== 'number') existing.messagesMissingBody = 0;
        if (typeof existing.responsesProcessed !== 'number') existing.responsesProcessed = 0;
        if (typeof existing.responsesSkippedDuplicate !== 'number')
          existing.responsesSkippedDuplicate = 0;
        if (typeof existing.lastMessageAt !== 'number') existing.lastMessageAt = 0;
        existing.activeInstanceId = this.instanceId;
        existing.rev = HOOK_REVISION;
        if (typeof existing.repairCount !== 'number') {
          existing.repairCount = 0;
        }
        if (typeof existing.endpointStats !== 'object' || existing.endpointStats === null) {
          existing.endpointStats = Object.create(null);
        }
      } else {
        g[HOOK_STATS_KEY] = createHookStats(this.instanceId);
      }
      this.hookStats = g[HOOK_STATS_KEY] as HookStats;
      this.hookRuntime = this.hookStats;
      this.syncRuntimeStats();
      g[HOOK_RUNTIME_KEY] = {
        ...(g[HOOK_RUNTIME_KEY] ?? {}),
        instanceId: this.instanceId,
        __twe_runtime_signature_v1: HOOK_RUNTIME_SIGNATURE,
        revision: HOOK_REVISION,
        installedAt: Date.now(),
        modes: this.runtimeModes,
        runtimeModeReason: this.runtimeModeReason,
        hookStats: this.hookStats,
        messagesTotal: this.hookStats.messagesTotal,
        messagesLegacyShape: this.hookStats.messagesLegacyShape,
        messagesMissingContext: this.hookStats.messagesMissingContext,
        messagesRepairedAtBridge: this.hookStats.messagesRepairedAtBridge,
        messagesMissingBody: this.hookStats.messagesMissingBody,
        responsesProcessed: this.hookStats.responsesProcessed,
        responsesSkippedDuplicate: this.hookStats.responsesSkippedDuplicate,
        lastMessageAt: this.hookStats.lastMessageAt,
        activeInstanceId: this.hookStats.activeInstanceId,
        rev: this.hookStats.rev,
        endpointStats: this.hookStats.endpointStats,
        uninstall: () => this.uninstallHooks(),
      };
      publishHookGlobals(this.hookStats, g[HOOK_RUNTIME_KEY] as Record<string, unknown>);
    } catch (error) {
      constructorError = recordBootstrapError(
        'ExtensionManager.constructor',
        this.instanceId,
        error,
      );
      logger.error('ExtensionManager constructor bootstrap error', error);
    }

    try {
      const fallbackStats = normalizeRuntimeForRead(
        this.hookStats,
        createHookStats(this.instanceId),
      );
      if (!this.hookStats || this.hookStats.activeInstanceId !== this.instanceId) {
        this.hookStats = fallbackStats;
      }
      this.hookStats.activeInstanceId = this.instanceId;
      this.hookStats.rev = HOOK_REVISION;
      if (constructorError) {
        this.hookStats.repairCount = (this.hookStats.repairCount || 0) + 1;
      }
      this.hookRuntime = this.hookStats;
      this.syncRuntimeStats();
      g[HOOK_STATS_KEY] = this.hookStats;
      g[HOOK_RUNTIME_KEY] = {
        ...(g[HOOK_RUNTIME_KEY] ?? {}),
        __twe_runtime_signature_v1: HOOK_RUNTIME_SIGNATURE,
        ...(constructorError ? { bootstrapError: constructorError } : {}),
        instanceId: this.instanceId,
        revision: HOOK_REVISION,
        installedAt: Date.now(),
        modes: this.runtimeModes,
        runtimeModeReason: this.runtimeModeReason,
        hookStats: this.hookStats,
        messagesTotal: this.hookStats.messagesTotal,
        messagesLegacyShape: this.hookStats.messagesLegacyShape,
        messagesMissingContext: this.hookStats.messagesMissingContext,
        messagesRepairedAtBridge: this.hookStats.messagesRepairedAtBridge,
        messagesMissingBody: this.hookStats.messagesMissingBody,
        responsesProcessed: this.hookStats.responsesProcessed,
        responsesSkippedDuplicate: this.hookStats.responsesSkippedDuplicate,
        lastMessageAt: this.hookStats.lastMessageAt,
        activeInstanceId: this.hookStats.activeInstanceId,
        rev: this.hookStats.rev,
        endpointStats: this.hookStats.endpointStats,
        uninstall: () => this.uninstallHooks(),
      };
      publishHookGlobals(this.hookStats, g[HOOK_RUNTIME_KEY] as Record<string, unknown>);
    } catch {
      // ignore
    }

    if (!this.hookRuntime || !this.hookStats) {
      this.hookStats = createHookStats(this.instanceId);
      this.hookRuntime = this.hookStats;
      this.syncRuntimeStats();
      g[HOOK_STATS_KEY] = this.hookStats;
      g[HOOK_RUNTIME_KEY] = {
        ...(g[HOOK_RUNTIME_KEY] ?? {}),
        ...(constructorError ? { bootstrapError: constructorError } : {}),
        instanceId: this.instanceId,
        __twe_runtime_signature_v1: HOOK_RUNTIME_SIGNATURE,
        revision: HOOK_REVISION,
        installedAt: Date.now(),
        modes: this.runtimeModes,
        runtimeModeReason: this.runtimeModeReason,
        hookStats: this.hookStats,
        messagesTotal: this.hookStats.messagesTotal,
        messagesLegacyShape: this.hookStats.messagesLegacyShape,
        messagesMissingContext: this.hookStats.messagesMissingContext,
        messagesRepairedAtBridge: this.hookStats.messagesRepairedAtBridge,
        messagesMissingBody: this.hookStats.messagesMissingBody,
        responsesProcessed: this.hookStats.responsesProcessed,
        responsesSkippedDuplicate: this.hookStats.responsesSkippedDuplicate,
        lastMessageAt: this.hookStats.lastMessageAt,
        activeInstanceId: this.hookStats.activeInstanceId,
        rev: this.hookStats.rev,
        endpointStats: this.hookStats.endpointStats,
        uninstall: () => this.uninstallHooks(),
      };
      publishHookGlobals(this.hookStats, g[HOOK_RUNTIME_KEY] as Record<string, unknown>);
    }
  }

  private applyBookmarkRouteCandidate(candidate: BookmarkRouteCandidate | null) {
    const now = Date.now();
    const currentRoute = typeof location !== 'undefined' ? location.href : '';
    const routeIsBookmarks = isBookmarksRoute(currentRoute);
    const candidateIsBookmarks = candidate?.pageUrl
      ? isBookmarksRoute(candidate.pageUrl)
      : routeIsBookmarks;

    if (!candidate) {
      if (routeIsBookmarks && this.lastStickyBookmarkContext?.folderId) {
        const keep = this.lastStickyBookmarkContext;
        if (now - keep.capturedAt <= BOOKMARK_CONTEXT_BOOKMARKS_ONLY_STALE_MS) {
          setBookmarkContext({
            folderId: keep.folderId,
            pageUrl: keep.pageUrl || currentRoute,
            source: keep.source,
            capturedAt: now,
          });
          return;
        }
      }
      this.lastStickyBookmarkContext = null;
      setBookmarkContext({
        folderId: null,
        pageUrl: currentRoute,
        source: 'refresh',
        capturedAt: now,
      });
      return;
    }

    if (!candidateIsBookmarks) {
      this.lastStickyBookmarkContext = null;
      setBookmarkContext({
        folderId: null,
        pageUrl: candidate.pageUrl,
        source: candidate.source,
        capturedAt: now,
      });
      return;
    }

    if (candidate.folderId) {
      const sticky: BookmarkContextPayload = {
        folderId: candidate.folderId,
        pageUrl: candidate.pageUrl,
        source: candidate.source,
        capturedAt: now,
      };
      this.lastStickyBookmarkContext = sticky;
      setBookmarkContext(sticky);
      return;
    }

    const lastSticky = this.lastStickyBookmarkContext;
    const allowFallback =
      lastSticky &&
      !!lastSticky.folderId &&
      candidate.confidence <= BOOKMARK_CONTEXT_MIN_CONFIDENCE &&
      now - lastSticky.capturedAt <= BOOKMARK_CONTEXT_BOOKMARKS_ONLY_STALE_MS;

    if (!candidate.folderId && allowFallback) {
      setBookmarkContext({
        folderId: lastSticky.folderId,
        pageUrl: candidate.pageUrl,
        source: lastSticky.source,
        capturedAt: now,
      });
      return;
    }

    if (
      !candidate.folderId &&
      routeIsBookmarks &&
      lastSticky?.folderId &&
      now - lastSticky.capturedAt <= BOOKMARK_CONTEXT_BOOKMARKS_ONLY_STALE_MS
    ) {
      setBookmarkContext({
        folderId: lastSticky.folderId,
        pageUrl: candidate.pageUrl,
        source: lastSticky.source,
        capturedAt: now,
      });
      return;
    }

    this.lastStickyBookmarkContext = null;
    setBookmarkContext({
      folderId: null,
      pageUrl: candidate.pageUrl,
      source: candidate.source,
      capturedAt: now,
    });
  }

  private updateBookmarkRouteContext() {
    try {
      const pageContext = captureBookmarkRouteFromPage();
      this.applyBookmarkRouteCandidate(pageContext ?? null);
    } catch {
      // ignore
    }
  }

  private installBookmarkContextTracking() {
    this.updateBookmarkRouteContext();

    const historyObj = hookGlobalObject.history;
    if (!historyObj) return;
    const refreshContext = () => this.updateBookmarkRouteContext();
    const applyNavigationCandidate = (candidate: BookmarkRouteCandidate | null) => {
      this.applyBookmarkRouteCandidate(candidate);
    };
    const captureFromNavigation = (args: unknown[]) => {
      try {
        const candidate = captureBookmarkRouteFromNavigationArgs(args);
        if (candidate) {
          applyNavigationCandidate(candidate);
          return;
        }
      } catch {
        // ignore
      }
      refreshContext();
    };

    const wrap = (name: 'pushState' | 'replaceState') => {
      type PatchedNavigationHook = HookCallable & {
        __twe_is_bookmark_context_patched_v1?: boolean;
      };
      const mutableHistory = historyObj as unknown as Record<string, unknown>;
      const original = mutableHistory[name];
      if (
        typeof original !== 'function' ||
        (original as PatchedNavigationHook).__twe_is_bookmark_context_patched_v1
      )
        return;

      const wrapped = function (this: History, ...args: unknown[]) {
        const result = (original as (...a: unknown[]) => unknown).apply(this, args);
        captureFromNavigation(args);
        return result;
      };
      (wrapped as PatchedNavigationHook).__twe_is_bookmark_context_patched_v1 = true;
      try {
        mutableHistory[name] = wrapped;
      } catch {
        try {
          Object.defineProperty(mutableHistory, name, {
            value: wrapped,
            configurable: true,
            writable: true,
          });
        } catch {
          // ignore
        }
      }
    };

    wrap('pushState');
    wrap('replaceState');

    if (!hookGlobalObject.__twe_bookmark_context_listeners_v1) {
      hookGlobalObject.__twe_bookmark_context_listeners_v1 = true;
      const listenerTargets = [hookGlobalObject, window, globalThis];
      for (const target of listenerTargets) {
        addEventListenerSafe(target, 'popstate', refreshContext);
        addEventListenerSafe(target, 'hashchange', refreshContext);
      }
    }

    const locationObj = (hookGlobalObject as unknown as { location?: Location }).location;
    if (locationObj) {
      const locationAny = locationObj as unknown as {
        assign?: (url: string | URL) => void;
        replace?: (url: string | URL) => void;
      };

      const wrapLocation = (methodName: 'assign' | 'replace'): void => {
        const original = locationAny[methodName];
        if (typeof original !== 'function') return;
        if (
          (original as { __twe_is_bookmark_context_patched_v1?: boolean })
            .__twe_is_bookmark_context_patched_v1
        ) {
          return;
        }
        const wrapped = function (url: string | URL): void {
          const value = typeof url === 'string' ? url : url.toString();
          try {
            original.call(locationObj as Location, value);
          } catch {
            // Some pages lock location methods; avoid crashing the script.
          }
          refreshContext();
        };
        (
          wrapped as { __twe_is_bookmark_context_patched_v1?: boolean }
        ).__twe_is_bookmark_context_patched_v1 = true;
        try {
          locationAny[methodName] = wrapped;
        } catch {
          try {
            if (typeof Object.defineProperty === 'function') {
              Object.defineProperty(locationAny, methodName, {
                value: wrapped,
                configurable: true,
                writable: true,
              });
            }
          } catch {
            // ignore
          }
        }
      };
      wrapLocation('assign');
      wrapLocation('replace');
    }

    if (!hookGlobalObject.__twe_bookmark_context_bookmark_click_v1 && document?.body) {
      const onClick = (event: Event) => {
        try {
          const target = event.target as EventTarget | null;
          const candidate = captureBookmarkRouteFromEventTarget(target);
          if (candidate) {
            applyNavigationCandidate(candidate);
          }
        } catch {
          // ignore
        }
      };
      hookGlobalObject.__twe_bookmark_context_bookmark_click_v1 = true;
      const clickTargets = [hookGlobalObject, window, document, globalThis];
      let clickListenerInstalled = false;
      for (const target of clickTargets) {
        clickListenerInstalled =
          addEventListenerSafe(target, 'click', onClick, { capture: true }) ||
          clickListenerInstalled;
      }
      if (!clickListenerInstalled && document?.body) {
        addEventListenerSafe(document.body, 'click', onClick, { capture: true });
      }
      hookGlobalObject.__twe_bookmark_context_bookmark_click_handler_v1 = onClick;
    }

    if (!hookGlobalObject.__twe_bookmark_context_interval_v1) {
      // Some execution paths update bookmark page state outside push/replace state hooks.
      hookGlobalObject.__twe_bookmark_context_interval_v1 = setInterval(() => {
        try {
          this.updateBookmarkRouteContext();
        } catch {
          // ignore
        }
      }, 1200);
    }

    if (!hookGlobalObject.__twe_bookmark_context_mutation_v1) {
      const doc = document;
      const hasMutationObserver = typeof MutationObserver !== 'undefined';
      if (doc?.body && hasMutationObserver) {
        const observer = new MutationObserver(() => {
          this.updateBookmarkRouteContext();
        });
        observer.observe(doc.body, {
          subtree: true,
          childList: true,
          attributes: true,
          attributeFilter: ['href', 'aria-selected', 'data-state', 'class'],
        });
        hookGlobalObject.__twe_bookmark_context_mutation_v1 = observer;
      }
    }
  }

  /**
   * Register and instantiate a new extension.
   *
   * @param ctor Extension constructor.
   */
  public add(ctor: ExtensionConstructor) {
    try {
      logger.debug(`Register new extension: ${ctor.name}`);
      const instance = new ctor(this);
      const previous = this.extensions.get(instance.name);
      if (previous && previous !== instance) {
        try {
          if (previous.enabled) {
            previous.dispose();
          }
        } catch {
          // ignore
        }
      }
      this.extensions.set(instance.name, instance);
    } catch (err) {
      logger.error(`Failed to register extension: ${ctor.name}`, err);
    }
  }

  /**
   * Set up all enabled extensions.
   */
  public start() {
    for (const ext of this.extensions.values()) {
      if (this.disabledExtensions.has(ext.name)) {
        this.disable(ext.name);
      } else {
        this.enable(ext.name);
      }
    }
  }

  public enable(name: string) {
    try {
      this.disabledExtensions.delete(name);
      options.set('disabledExtensions', [...this.disabledExtensions]);

      const ext = this.extensions.get(name)!;
      if (ext.enabled) return;
      ext.enabled = true;
      ext.setup();

      logger.debug(`Enabled extension: ${name}`);
      this.signal.value++;
    } catch (err) {
      logger.error(`Failed to enable extension: ${name}`, err);
    }
  }

  public disable(name: string) {
    try {
      this.disabledExtensions.add(name);
      options.set('disabledExtensions', [...this.disabledExtensions]);

      const ext = this.extensions.get(name)!;
      if (!ext.enabled) return;
      ext.enabled = false;
      ext.dispose();

      logger.debug(`Disabled extension: ${name}`);
      this.signal.value++;
    } catch (err) {
      logger.error(`Failed to disable extension: ${name}`, err);
    }
  }

  public isDisposed(): boolean {
    return this.disposed;
  }

  public getExtensions() {
    return [...this.extensions.values()];
  }

  private installPageMessageBridge() {
    // Listen for page-realm hook events. We keep interceptors in content realm,
    // and only pass serializable data across via postMessage.
    if (this.pageMessageHandler) return;

    this.pageMessageHandler = (event: MessageEvent) => {
      try {
        const origin = event.origin || '';
        if (origin && !/^https:\/\/(x|twitter|mobile\.x)\.com$/.test(origin)) return;
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        const data = event.data as any;
        if (!data || data[HOOK_MESSAGE_FLAG] !== true) return;
        const messageRevision = getBridgeMessageRevision(data);
        const isLegacyMessage = messageRevision === null || messageRevision !== HOOK_REVISION;
        const requestCandidate = pickBridgeRequest(data);
        const bridgeReq = normalizeHookPayloadRequest(requestCandidate);
        const res = data.res as { status: number; responseText: string } | undefined;
        if (!res) return;
        const rawEnvelopeRequest =
          requestCandidate && typeof requestCandidate === 'object'
            ? (requestCandidate as Record<string, unknown>)
            : null;
        const requestId =
          typeof bridgeReq.requestId === 'string' && bridgeReq.requestId.trim().length
            ? bridgeReq.requestId
            : typeof (rawEnvelopeRequest as { requestId?: unknown })?.requestId === 'string' &&
                (rawEnvelopeRequest as { requestId?: string }).requestId?.trim().length
              ? (rawEnvelopeRequest as { requestId: string }).requestId
              : nextBookmarkRequestId();
        const incomingMethod =
          typeof bridgeReq.method === 'string' && bridgeReq.method
            ? bridgeReq.method
            : typeof (rawEnvelopeRequest as { method?: unknown })?.method === 'string'
              ? ((rawEnvelopeRequest as { method?: unknown }).method as string)
              : 'GET';
        const incomingUrl =
          typeof bridgeReq.url === 'string' && bridgeReq.url.length
            ? bridgeReq.url
            : typeof (rawEnvelopeRequest as { url?: unknown })?.url === 'string'
              ? ((rawEnvelopeRequest as { url?: unknown }).url as string)
              : '';
        if (!incomingUrl) return;
        const incomingBody =
          typeof bridgeReq.body === 'string'
            ? bridgeReq.body
            : typeof (rawEnvelopeRequest as { body?: unknown })?.body === 'string'
              ? ((rawEnvelopeRequest as { body?: unknown }).body as string)
              : undefined;

        const requestContextFromLegacyWrapper = rawEnvelopeRequest
          ? (
              rawEnvelopeRequest as Record<string, { bookmarkContext?: unknown }> as {
                bookmarkContext?: unknown;
              }
            ).bookmarkContext
          : undefined;
        const requestContextFromAlternative =
          rawEnvelopeRequest &&
          Object.prototype.hasOwnProperty.call(rawEnvelopeRequest, 'requestContext')
            ? (rawEnvelopeRequest as Record<string, unknown>).requestContext
            : undefined;
        const hasRequestContext =
          (requestContextFromLegacyWrapper !== undefined &&
            requestContextFromLegacyWrapper !== null) ||
          (requestContextFromAlternative !== undefined && requestContextFromAlternative !== null) ||
          bridgeReq.bookmarkContext !== undefined;
        const hasBodyField =
          typeof bridgeReq.body === 'string' ||
          typeof (rawEnvelopeRequest as { body?: unknown })?.body === 'string';
        const hasRequestIdField =
          (typeof bridgeReq.requestId === 'string' && bridgeReq.requestId.trim().length > 0) ||
          (typeof (rawEnvelopeRequest as { requestId?: unknown })?.requestId === 'string' &&
            ((rawEnvelopeRequest as { requestId?: string }).requestId?.trim().length || 0) > 0);
        const req = buildNormalizedHookMessageRequest({
          method: incomingMethod,
          url: incomingUrl,
          body: incomingBody,
          bookmarkContext:
            bridgeReq.bookmarkContext ??
            requestContextFromLegacyWrapper ??
            requestContextFromAlternative,
          requestId,
        });
        const bridgeRepaired =
          isLegacyMessage || !hasRequestContext || !hasBodyField || !hasRequestIdField;
        const isBookmarksMessage = isBookmarksApiRequest(req.url);
        const endpointKey = isBookmarksMessage ? extractBookmarksEndpoint(req.url) : null;
        const endpointStats = endpointKey ? this.getEndpointStats(endpointKey) : null;
        const now = Date.now();
        const responseText = typeof res.responseText === 'string' ? res.responseText : '';

        if (!this.hookStats) return;
        if (bridgeRepaired) {
          this.hookStats.messagesRepairedAtBridge++;
        }
        if (!hasBodyField) {
          this.hookStats.messagesMissingBody++;
        }
        if (isLegacyMessage) {
          this.hookStats.messagesLegacyShape++;
        }

        if (endpointStats) {
          endpointStats.received += 1;
          endpointStats.lastAt = now;
          endpointStats.lastStatus = res.status ?? 0;
          endpointStats.lastUrl = req.url;
        }

        if (!hasRequestContext) {
          this.hookStats.messagesLegacyShape++;
          if (endpointStats) endpointStats.legacyShape += 1;
        }

        if (!hasRequestContext || req.bookmarkContext === null) {
          this.hookStats.messagesMissingContext++;
          if (endpointStats) endpointStats.missingContext += 1;
          req.bookmarkContext = resolveRequestBookmarkContext(req.url, req.body, {
            method: req.method,
            url: req.url,
            body: req.body,
            requestId,
          });
        } else {
          req.bookmarkContext = normalizeBookmarkContextValue(req.bookmarkContext, {
            method: req.method,
            url: req.url,
            body: req.body,
            requestId,
            hasBody: !!req.body,
          });
        }
        setBookmarkContext(req.bookmarkContext);

        appendBookmarkContextDump({
          requestId,
          ts: Date.now(),
          method: req.method,
          url: req.url,
          hasBody: !!req.body,
          confidenceSource:
            typeof (req.bookmarkContext as BookmarkContextPayload | undefined)?.source === 'string'
              ? (req.bookmarkContext as BookmarkContextPayload).source
              : 'unknown',
          context: req.bookmarkContext as BookmarkContextPayload,
          normalizedRoute: resolveCanonicalRouteFromUrl(req.url).pageUrl,
        });

        // Payload-level dedupe/backpressure: Twitter will sometimes re-emit the same
        // timeline payload (and spliced timelines can lead to repeated updates).
        // JSON.parse is expensive; skip identical payloads briefly.
        const method = (req.method || 'GET').toUpperCase();
        const isBookmarkApiRequest = isBookmarksApiRequest(req.url);
        if (!isBookmarkApiRequest) {
          const dedupeKey = createRequestSignature(method, req.url, res.status ?? 0, responseText);
          const prev = this.recentResponseSigs.get(dedupeKey);
          if (prev && now - prev.at < RESPONSE_DEDUPE_WINDOW_MS) {
            this.hookStats.responsesSkippedDuplicate++;
            if (endpointStats) endpointStats.skippedDuplicate += 1;
            this.syncRuntimeStats();
            return;
          }

          this.recentResponseSigs.set(dedupeKey, { sig: dedupeKey, at: now });
          cleanupSignatureCache(this.recentResponseSigs);
        }

        this.hookStats.responsesProcessed++;
        if (endpointStats) {
          endpointStats.processed += 1;
          endpointStats.newUniqueTweets += countUniqueTweetIds(responseText);
        }
        this.hookStats.messagesTotal++;
        this.hookStats.lastMessageAt = now;

        if (this.hookStats) {
          this.hookStats.lastUrl = req.url;
          this.hookStats.lastAt = Date.now();
          // Heuristic: treat non-GET as XHR-ish (GraphQL fetch is often POST).
          if ((req.method || '').toUpperCase() === 'GET') this.hookStats.xhrMessages++;
          else this.hookStats.fetchMessages++;

          if (this.debugEnabled && this.hookStats.loggedUrls < 5) {
            this.hookStats.loggedUrls++;
            logger.debug('Hook saw request', {
              method: req.method,
              url: req.url,
              status: res.status,
            });
          }
          this.syncRuntimeStats();
        }

        const pseudoXhr = {
          status: res.status,
          responseText,
        } as XMLHttpRequest;

        this.runInterceptors(req, pseudoXhr);
      } catch (err) {
        logger.debug('Failed to process hook message', err);
      }
    };

    window.addEventListener('message', this.pageMessageHandler, false);
  }

  /**
   * Here we hooks the browser's XHR method to intercept Twitter's Web API calls.
   * This need to be done before any XHR request is made.
   */
  private installHttpHooks(force = false) {
    if (!this.isHookModeEnabled('xhr')) {
      return;
    }

    let hookInstalled = false;
    let originalOpen: unknown;
    try {
      if (
        !hookGlobalObject.XMLHttpRequest?.prototype ||
        !hookGlobalObject.XMLHttpRequest?.prototype?.open
      ) {
        throw new Error('XMLHttpRequest.prototype.open not available');
      }

      // Stash originals in page-realm so wrappers can call through safely.

      const proto = hookGlobalObject.XMLHttpRequest.prototype as unknown as Record<string, unknown>;
      const currentOpen = proto.open;
      const currentSend = proto.send;
      originalOpen =
        typeof proto[ORIG_XHR_OPEN_KEY] === 'function' ? proto[ORIG_XHR_OPEN_KEY] : currentOpen;
      if (!proto[ORIG_XHR_OPEN_KEY] && typeof currentOpen === 'function') {
        proto[ORIG_XHR_OPEN_KEY] = currentOpen;
      }
      if (!proto[ORIG_XHR_SEND_KEY] && typeof currentSend === 'function') {
        proto[ORIG_XHR_SEND_KEY] = currentSend;
      }

      const wrappedSendFromState = getFunctionFromHookState(
        currentSend,
        '__twe_is_hook_send_v1',
        ORIG_XHR_SEND_KEY,
      );
      const sendBase =
        typeof wrappedSendFromState === 'function'
          ? (wrappedSendFromState as XMLHttpRequest['send'])
          : typeof proto[ORIG_XHR_SEND_KEY] === 'function'
            ? (proto[ORIG_XHR_SEND_KEY] as XMLHttpRequest['send'])
            : (currentSend as XMLHttpRequest['send']);
      if (typeof sendBase !== 'function') {
        throw new Error('XMLHttpRequest.prototype.send not available');
      }

      const sendNeedsRepair = force || !hasHookVersion(currentSend, '__twe_is_hook_send_v1');
      if (sendNeedsRepair) {
        const sendWrapper = function (
          this: XMLHttpRequest,
          body?: Document | XMLHttpRequestBodyInit | null,
        ): void {
          try {
            const xhr = this as HookedXhr;
            const reqMethod = String(xhr.__twe_req_method_v1 || 'GET');
            const requestId =
              typeof xhr.__twe_req_id_v1 === 'string'
                ? xhr.__twe_req_id_v1
                : nextBookmarkRequestId();
            xhr.__twe_req_id_v1 = requestId;
            xhr.__twe_req_body_v1 = serializeRequestBodyText(body as unknown);
            if (typeof body === 'undefined') {
              xhr.__twe_req_body_v1 = '';
            }
            if (xhr.__twe_req_url_v1) {
              const requestMeta = {
                method: reqMethod,
                url: String(xhr.__twe_req_url_v1),
                body: xhr.__twe_req_body_v1,
                requestId,
              };
              if (isBookmarksApiRequest(String(xhr.__twe_req_url_v1))) {
                xhr.__twe_req_bookmark_context_v1 = resolveRequestBookmarkContext(
                  xhr.__twe_req_url_v1,
                  xhr.__twe_req_body_v1,
                  requestMeta,
                );
                setBookmarkContext(xhr.__twe_req_bookmark_context_v1);
              } else {
                xhr.__twe_req_bookmark_context_v1 = null;
              }
            }
          } catch {
            // ignore
          }
          (sendBase as typeof sendWrapper).apply(this, [body as never]);
        };
        markHookFunction(sendWrapper, '__twe_is_hook_send_v1');
        defineOn(proto, 'send', sendWrapper);
        markHookFunction((proto as { send?: unknown }).send, '__twe_is_hook_send_v1');
        hookInstalled = true;
      } else {
        hookInstalled = true;
      }

      if (
        !force &&
        hasHookVersion(currentOpen, '__twe_is_hook_open_v1') &&
        hasHookVersion(currentSend, '__twe_is_hook_send_v1')
      ) {
        hookInstalled = true;
      } else {
        const wrappedOpenFromState = getFunctionFromHookState(
          currentOpen,
          '__twe_is_hook_open_v1',
          ORIG_XHR_OPEN_KEY,
        );
        const openBase =
          typeof wrappedOpenFromState === 'function'
            ? (wrappedOpenFromState as XMLHttpRequest['open'])
            : typeof proto[ORIG_XHR_OPEN_KEY] === 'function'
              ? (proto[ORIG_XHR_OPEN_KEY] as XMLHttpRequest['open'])
              : (currentOpen as XMLHttpRequest['open']);

        if (typeof openBase !== 'function') {
          throw new Error('XMLHttpRequest.prototype.open base function unavailable');
        }

        const openWrapper = function (this: XMLHttpRequest, ...args: unknown[]): unknown {
          try {
            const method = typeof args[0] === 'string' ? args[0] : String(args[0] ?? '');
            const rawUrl = args[1];
            const nextUrl = typeof rawUrl === 'string' ? rawUrl : String(rawUrl ?? '');

            // Keep this wrapper completely page-realm safe:
            // do not touch content-realm objects (logger/manager/etc), and never throw.
            if (nextUrl && /\/graphql\/|\/api\/1\.1\//.test(nextUrl)) {
              const self = this as HookedXhr;
              self.__twe_req_method_v1 = method;
              self.__twe_req_url_v1 = nextUrl;
              self.__twe_req_body_v1 = '';
              self.__twe_req_id_v1 = nextBookmarkRequestId();
              if (isBookmarksApiRequest(nextUrl)) {
                self.__twe_req_bookmark_context_v1 = resolveRequestBookmarkContext(
                  nextUrl,
                  undefined,
                  {
                    method,
                    url: nextUrl,
                    requestId: self.__twe_req_id_v1,
                  },
                );
                setBookmarkContext(self.__twe_req_bookmark_context_v1);
              } else {
                self.__twe_req_bookmark_context_v1 = null;
              }
              if (!self.__twe_hooked_v1) {
                self.__twe_hooked_v1 = true;
                this.addEventListener('load', function (this: XMLHttpRequest) {
                  try {
                    const xhr = this as HookedXhr;
                    const reqMethod = xhr.__twe_req_method_v1 || method;
                    const reqUrl = xhr.__twe_req_url_v1 || nextUrl;
                    if (!reqUrl || !/\/graphql\/|\/api\/1\.1\//.test(reqUrl)) return;
                    const responseText = String((this as XMLHttpRequest).responseText ?? '');
                    const reqBody = xhr.__twe_req_body_v1;
                    const reqId =
                      typeof xhr.__twe_req_id_v1 === 'string'
                        ? xhr.__twe_req_id_v1
                        : nextBookmarkRequestId();
                    xhr.__twe_req_id_v1 = reqId;
                    const bookmarkRequest = isBookmarksApiRequest(reqUrl);
                    const reqContext =
                      xhr.__twe_req_bookmark_context_v1 ||
                      (bookmarkRequest
                        ? resolveRequestBookmarkContext(reqUrl, reqBody, {
                            method: reqMethod,
                            url: reqUrl,
                            body: reqBody,
                            requestId: reqId,
                          })
                        : null);
                    if (!xhr.__twe_req_bookmark_context_v1) {
                      xhr.__twe_req_bookmark_context_v1 = reqContext;
                    }
                    if (bookmarkRequest && reqContext) {
                      setBookmarkContext(reqContext);
                    }
                    const normalizedReq = buildNormalizedHookMessageRequest({
                      method: reqMethod,
                      url: reqUrl,
                      body: reqBody || '',
                      bookmarkContext: reqContext ?? null,
                      requestId: reqId,
                    });
                    postHookMessage({
                      __twe_mcp_hook_v1: true,
                      req: normalizedReq,
                      res: { status: (this as XMLHttpRequest).status ?? 0, responseText },
                    });
                  } catch {
                    // Never throw from XHR hooks; it can break the feed.
                  }
                });
              }
            }
          } catch {
            // Never throw from XHR hooks; it can break the feed.
          }

          return (openBase as typeof openWrapper).apply(this, args as never);
        };
        markHookFunction(openWrapper, '__twe_is_hook_open_v1');
        proto[ORIG_XHR_OPEN_KEY] = openBase;
        proto[ORIG_XHR_SEND_KEY] = sendBase;
        if (defineOn(proto, 'open', openWrapper)) {
          hookInstalled = true;
        }
        markHookFunction((proto as { open?: unknown }).open, '__twe_is_hook_open_v1');
        markHookFunction((proto as { send?: unknown }).send, '__twe_is_hook_send_v1');
      }
    } catch (err) {
      logger.error('Failed to hook into XMLHttpRequest', err);
    }

    if (this.debugEnabled) {
      logger.info(`Hooked into XMLHttpRequest (installed=${hookInstalled})`);
    }

    // Diagnostics: with @inject-into content, hooks must be installed into the page realm.
    // We log capabilities rather than insisting on page-context injection, since X.com CSP
    // can block some managers' page injection modes.
    setTimeout(() => {
      try {
        const capabilities = getRuntimeCapabilities();
        const openIsPatched =
          hookGlobalObject.XMLHttpRequest?.prototype?.open !==
          (typeof originalOpen === 'function' ? originalOpen : undefined);
        if (!openIsPatched) {
          logger.error(
            `XHR hook not active (unsafeWindow=${capabilities.hasUnsafeWindow}, wrappedJSObject=${capabilities.hasWrappedJSObject}, exportFunction=${capabilities.hasExportFunction}). ` +
              `Bookmark capture will not work.`,
          );
        } else if (this.debugEnabled) {
          logger.debug('XHR hook active', {
            unsafeWindow: capabilities.hasUnsafeWindow,
            wrappedJSObject: capabilities.hasWrappedJSObject,
            exportFunction: capabilities.hasExportFunction,
          });
        }
      } catch (err) {
        logger.debug('XHR hook diagnostics failed', err);
      }
    }, 1000);
  }

  private installFetchHooks(force = false) {
    if (!this.isHookModeEnabled('fetch')) {
      return;
    }

    const fetchNative = (hookGlobalObject as Record<string, unknown>).fetch;
    if (typeof fetchNative !== 'function') {
      logger.warn('Fetch API not found, skipping fetch hooks');
      return;
    }

    const existingFetch = (hookGlobalObject as Record<string, unknown>).fetch;
    if (!force && hasHookVersion(existingFetch, '__twe_is_hook_fetch_v1')) {
      logger.debug('Fetch hook already installed');
      return;
    }

    const fetchBaseFromState = getFunctionFromHookState(
      existingFetch,
      '__twe_is_hook_fetch_v1',
      ORIG_FETCH_KEY,
    );
    const fetchBase =
      typeof fetchBaseFromState === 'function'
        ? (fetchBaseFromState as typeof fetch)
        : (fetchNative as typeof fetch);

    if (typeof fetchBase !== 'function') {
      logger.warn('Fetch API base function unavailable');
      return;
    }

    const pageAny = hookGlobalObject as Record<string, unknown>;
    pageAny[ORIG_FETCH_KEY] = fetchBase;

    const fetchWrapper = async function (
      input: RequestInfo | URL,
      init?: RequestInit,
    ): Promise<Response> {
      let method = 'GET';
      let url = '';
      let serializedBody: string | undefined;
      let requestId = '';
      let requestContext: BookmarkContextPayload | undefined;

      try {
        method =
          init?.method ??
          (typeof Request !== 'undefined' && input instanceof Request ? input.method : 'GET');
      } catch {
        method = init?.method ?? 'GET';
      }

      try {
        url =
          typeof input === 'string'
            ? input
            : typeof URL !== 'undefined' && input instanceof URL
              ? input.toString()
              : typeof Request !== 'undefined' && input instanceof Request
                ? input.url
                : '';
      } catch {
        url = '';
      }

      try {
        serializedBody = serializeRequestBodyText(init?.body);
      } catch {
        serializedBody = undefined;
      }

      try {
        requestId = nextBookmarkRequestId();
      } catch {
        requestId = '';
      }

      try {
        if (isBookmarksApiRequest(url)) {
          requestContext = resolveRequestBookmarkContext(url, serializedBody, {
            method,
            url,
            body: serializedBody,
            requestId,
          });
          setBookmarkContext(requestContext);
        } else {
          requestContext = undefined;
        }
      } catch (err) {
        // Never let hook pre-processing break app fetches.
        logger.debug('fetch request context capture failed', { method, url, err });
      }

      const origFetch = pageAny[ORIG_FETCH_KEY] ?? fetchBase;
      // Use call() to avoid illegal invocation issues.
      if (typeof origFetch !== 'function') {
        throw new Error('fetch base function unavailable');
      }
      const response = await (origFetch as typeof fetch).call(globalThis, input, init);

      try {
        if (!url || !/\/graphql\/|\/api\/1\.1\//.test(url)) return response;

        const contentType = response.headers.get('content-type') ?? '';
        const isTextualResponse =
          !contentType || contentType.includes('json') || contentType.startsWith('text/');

        if (!isTextualResponse) {
          return response;
        }

        // Read response body from a clone to avoid consuming the original stream.
        void response
          .clone()
          .text()
          .then((responseText: string) => {
            if (!responseText) return;
            try {
              const normalizedReq = buildNormalizedHookMessageRequest({
                method,
                url,
                body: serializedBody || '',
                bookmarkContext: requestContext ?? null,
                requestId,
              });

              postHookMessage({
                __twe_mcp_hook_v1: true,
                req: normalizedReq,
                res: { status: response.status, responseText },
              });
            } catch {
              // ignore
            }
          })
          .catch((err: unknown) => {
            logger.debug('fetch clone.text() failed', { method, url, err });
          });
      } catch (err) {
        // Never throw after native fetch succeeds.
        logger.debug('fetch response hook processing failed', { method, url, err });
      }

      return response;
    };
    markHookFunction(fetchWrapper, '__twe_is_hook_fetch_v1');

    const ok = defineOn(pageAny as object, 'fetch', fetchWrapper);
    markHookFunction((pageAny as { fetch?: unknown }).fetch, '__twe_is_hook_fetch_v1');
    if (ok && this.debugEnabled) {
      logger.info('Hooked into fetch');
    }
  }

  private startHookRepairLoop() {
    if (this.runtimeModes.safeMode || this.runtimeModes.repairMode === 'off') {
      return;
    }

    if (this.hookRepairInterval !== null) {
      return;
    }

    const schedule = (delayMs: number) => {
      this.hookRepairInterval = setTimeout(() => {
        this.hookRepairInterval = null;
        repair();
      }, delayMs);
    };

    const repair = () => {
      if (this.runtimeModes.safeMode || this.runtimeModes.repairMode === 'off') {
        return;
      }

      try {
        if (this.hookStats) {
          this.hookStats.repairCount += 1;
          this.syncRuntimeStats();
        }

        if (this.isHookModeEnabled('xhr')) {
          this.installHttpHooks(true);
        }
        if (this.isHookModeEnabled('fetch')) {
          this.installFetchHooks(true);
        }

        const hookSelfTest = this.runHookSelfTest();
        if (!hookSelfTest.ok) {
          this.enableSafeMode('hook-repair-self-test-failed', hookSelfTest.error);
          return;
        }

        this.hookRepairFailures = 0;
        this.hookRepairBackoffMs = HOOK_REPAIR_INTERVAL_MS;
      } catch (err) {
        this.hookRepairFailures += 1;
        this.hookRepairBackoffMs = Math.min(
          this.hookRepairBackoffMs * 2,
          HOOK_REPAIR_BACKOFF_MAX_MS,
        );
        logger.warn(
          `Hook repair failed (${this.hookRepairFailures}/${HOOK_REPAIR_FAILURE_LIMIT})`,
          err,
        );
        if (this.hookRepairFailures >= HOOK_REPAIR_FAILURE_LIMIT) {
          this.enableSafeMode('hook-repair-failure-limit', err);
          return;
        }
      }

      schedule(this.hookRepairBackoffMs);
    };

    schedule(0);
  }

  private runInterceptors(req: InterceptedRequest, res: XMLHttpRequest) {
    // Run current enabled interceptors. Wrap each in try/catch so a single
    // interceptor error cannot break the page behavior.
    this.getExtensions()
      .filter((ext) => ext.enabled)
      .forEach((ext) => {
        try {
          const func = ext.intercept();
          if (func) {
            func(req, res, ext);
          }
        } catch (err) {
          logger.error(`Interceptor error (${ext.name}):`, err);
        }
      });
  }
}
