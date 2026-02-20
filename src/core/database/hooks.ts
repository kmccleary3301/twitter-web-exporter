import { ExtensionType } from '@/core/extensions';
import { db } from '@/core/database';
import { Tweet, User } from '@/types';
import logger from '@/utils/logger';
import { useLiveQuery } from '@/utils/observable';
import { useEffect, useState } from 'preact/hooks';
import { unsafeWindow } from '$';
import { useDatabaseMutationVersion } from './mutation';

const CAPTURE_COUNT_SNAPSHOT_KEY = '__twe_capture_counts_v1';
const CAPTURE_COUNT_SNAPSHOT_V2_KEY = '__twe_capture_counts_v2';
const ACTIVE_DB_NAME_KEY = '__twe_active_db_name_v1';
const DB_MUTATION_STORAGE_KEY = '__twe_db_mutation_v1';
const CAPTURE_COUNT_EVENT_NAME = 'twe:capture-count-updated-v1';

type SnapshotCandidate = {
  count: number;
  dbName: string;
  updatedAt: number;
};

async function readCaptureCountFromDb(dbName: string, extName: string): Promise<number> {
  return await new Promise((resolve) => {
    const openReq = indexedDB.open(dbName);
    openReq.onerror = () => resolve(0);
    openReq.onsuccess = () => {
      const opened = openReq.result;
      let tx: IDBTransaction;
      try {
        if (!opened.objectStoreNames.contains('captures')) {
          opened.close();
          resolve(0);
          return;
        }
        tx = opened.transaction(['captures'], 'readonly');
      } catch {
        opened.close();
        resolve(0);
        return;
      }

      let req: IDBRequest<number>;
      try {
        const store = tx.objectStore('captures');
        if (store.indexNames.contains('extension')) {
          req = store.index('extension').count(extName);
        } else {
          req = store.count();
        }
      } catch {
        opened.close();
        resolve(0);
        return;
      }

      req.onsuccess = () => {
        opened.close();
        resolve(Number(req.result) || 0);
      };
      req.onerror = () => {
        opened.close();
        resolve(0);
      };
    };
  });
}

async function getCaptureCountAcrossKnownDatabases(extName: string): Promise<number> {
  const getActiveDatabaseName = (): string | null => {
    try {
      const unsafeCandidate = unsafeWindow as unknown as Record<string, unknown>;
      const unsafeName = unsafeCandidate?.[ACTIVE_DB_NAME_KEY];
      if (typeof unsafeName === 'string' && unsafeName.trim().length > 0) {
        return unsafeName.trim();
      }
    } catch {
      // ignore
    }

    try {
      const directName = (globalThis as Record<string, unknown>)[ACTIVE_DB_NAME_KEY];
      if (typeof directName === 'string' && directName.trim().length > 0) {
        return directName.trim();
      }
    } catch {
      // ignore
    }

    try {
      if (typeof localStorage !== 'undefined') {
        const stored = localStorage.getItem(ACTIVE_DB_NAME_KEY);
        if (stored && stored.trim().length > 0) {
          return stored.trim();
        }
      }
    } catch {
      // ignore
    }

    return null;
  };

  const readSnapshot = (activeDbName: string | null): number => {
    const candidates: SnapshotCandidate[] = [];

    const collectFromV2Entry = (entry: unknown): void => {
      if (!entry || typeof entry !== 'object') return;
      const obj = entry as Record<string, unknown>;
      const count = Number(obj.count);
      if (!Number.isFinite(count)) return;
      const dbName = typeof obj.dbName === 'string' ? obj.dbName : '';
      const updatedAt = Number(obj.updatedAt);
      candidates.push({
        count,
        dbName,
        updatedAt: Number.isFinite(updatedAt) ? updatedAt : 0,
      });
    };

    const collectFromV1Entry = (entry: unknown): void => {
      const count = Number(entry);
      if (!Number.isFinite(count)) return;
      candidates.push({ count, dbName: '', updatedAt: 0 });
    };

    const collectFromSource = (source: unknown): void => {
      if (!source || typeof source !== 'object') return;
      const root = source as Record<string, unknown>;
      const v2 = root[CAPTURE_COUNT_SNAPSHOT_V2_KEY];
      if (v2 && typeof v2 === 'object') {
        collectFromV2Entry((v2 as Record<string, unknown>)[extName]);
      }
      const v1 = root[CAPTURE_COUNT_SNAPSHOT_KEY];
      if (v1 && typeof v1 === 'object') {
        collectFromV1Entry((v1 as Record<string, unknown>)[extName]);
      }
    };

    try {
      collectFromSource(unsafeWindow as unknown as Record<string, unknown>);
    } catch {
      // ignore
    }

    try {
      collectFromSource(globalThis as Record<string, unknown>);
    } catch {
      // ignore
    }

    try {
      if (typeof localStorage !== 'undefined') {
        const rawV2 = localStorage.getItem(CAPTURE_COUNT_SNAPSHOT_V2_KEY);
        if (rawV2) {
          const parsed = JSON.parse(rawV2) as Record<string, unknown>;
          collectFromV2Entry(parsed?.[extName]);
        }
        const rawV1 = localStorage.getItem(CAPTURE_COUNT_SNAPSHOT_KEY);
        if (rawV1) {
          const parsed = JSON.parse(rawV1) as Record<string, unknown>;
          collectFromV1Entry(parsed?.[extName]);
        }
      }
    } catch {
      // ignore
    }

    if (!candidates.length) {
      return 0;
    }

    if (activeDbName) {
      const scoped = candidates
        .filter((candidate) => candidate.dbName === activeDbName)
        .sort((a, b) => b.updatedAt - a.updatedAt);
      const firstScoped = scoped[0];
      if (firstScoped) {
        return firstScoped.count;
      }
    }

    candidates.sort((a, b) => {
      if (b.updatedAt !== a.updatedAt) return b.updatedAt - a.updatedAt;
      return b.count - a.count;
    });
    const first = candidates[0];
    return first ? first.count : 0;
  };

  const activeDbName = getActiveDatabaseName();

  if (typeof indexedDB === 'undefined') {
    return Math.max(
      readSnapshot(activeDbName),
      Number((await db.extGetCaptureCount(extName)) || 0),
    );
  }

  let names: string[] = [];
  try {
    const rows = typeof indexedDB.databases === 'function' ? await indexedDB.databases() : [];
    names = Array.from(
      new Set(
        (rows || [])
          .map((row) => row?.name)
          .filter((name): name is string => !!name && name.includes('twitter-web-exporter')),
      ),
    );
  } catch {
    names = [];
  }

  if (!names.length) {
    if (activeDbName) {
      const activeDbCount = await readCaptureCountFromDb(activeDbName, extName);
      return Math.max(
        activeDbCount,
        readSnapshot(activeDbName),
        Number((await db.extGetCaptureCount(extName)) || 0),
      );
    }
    return Math.max(
      readSnapshot(activeDbName),
      Number((await db.extGetCaptureCount(extName)) || 0),
    );
  }

  if (activeDbName && names.includes(activeDbName)) {
    const activeDbCount = await readCaptureCountFromDb(activeDbName, extName);
    return Math.max(
      activeDbCount,
      readSnapshot(activeDbName),
      Number((await db.extGetCaptureCount(extName)) || 0),
    );
  }

  let best = 0;
  for (const name of names) {
    const count = await readCaptureCountFromDb(name, extName);
    if (count > best) {
      best = count;
    }
  }

  return Math.max(
    best,
    readSnapshot(activeDbName),
    Number((await db.extGetCaptureCount(extName)) || 0),
  );
}

export function useCaptureCount(extName: string) {
  const mutationVersion = useDatabaseMutationVersion(extName);
  const [count, setCount] = useState(0);

  useEffect(() => {
    let disposed = false;
    let refreshInFlight = false;
    let refreshQueued = false;

    const refresh = async () => {
      try {
        const next = await getCaptureCountAcrossKnownDatabases(extName);
        if (!disposed) {
          setCount(next);
        }
      } catch {
        // ignore polling failures
      }
    };

    const scheduleRefresh = () => {
      if (disposed) {
        return;
      }
      if (refreshInFlight) {
        refreshQueued = true;
        return;
      }
      refreshInFlight = true;
      void (async () => {
        try {
          await refresh();
        } finally {
          refreshInFlight = false;
          if (refreshQueued) {
            refreshQueued = false;
            scheduleRefresh();
          }
        }
      })();
    };

    const onStorage = (event: StorageEvent) => {
      const key = event.key;
      if (!key) return;
      if (
        key !== CAPTURE_COUNT_SNAPSHOT_KEY &&
        key !== CAPTURE_COUNT_SNAPSHOT_V2_KEY &&
        key !== ACTIVE_DB_NAME_KEY &&
        key !== DB_MUTATION_STORAGE_KEY
      ) {
        return;
      }
      scheduleRefresh();
    };

    const onCaptureCountEvent = (event: Event) => {
      const detail = (event as CustomEvent<{ extension?: string }>).detail;
      const targetExtension = detail && typeof detail === 'object' ? detail.extension : undefined;
      if (targetExtension && targetExtension !== extName) {
        return;
      }
      scheduleRefresh();
    };

    scheduleRefresh();
    const id = setInterval(() => {
      scheduleRefresh();
    }, 1500);
    if (typeof window !== 'undefined') {
      window.addEventListener('storage', onStorage);
      window.addEventListener(CAPTURE_COUNT_EVENT_NAME, onCaptureCountEvent);
    }

    return () => {
      disposed = true;
      clearInterval(id);
      if (typeof window !== 'undefined') {
        window.removeEventListener('storage', onStorage);
        window.removeEventListener(CAPTURE_COUNT_EVENT_NAME, onCaptureCountEvent);
      }
    };
  }, [extName, mutationVersion]);

  return count;
}

export function useCapturedRecords(extName: string, type: ExtensionType) {
  const mutationVersion = useDatabaseMutationVersion(extName);
  return useLiveQuery<Tweet[] | User[] | void, Tweet[] | User[] | void>(
    () => {
      logger.debug('useCapturedRecords liveQuery re-run', extName);

      if (type === ExtensionType.USER) {
        return db.extGetCapturedUsers(extName);
      }

      if (type === ExtensionType.TWEET) {
        return db.extGetCapturedTweets(extName);
      }
    },
    [extName, type, mutationVersion],
    [],
  );
}

export function useClearCaptures(extName: string) {
  return async () => {
    logger.debug('Clearing captures for extension:', extName);
    return db.extClearCaptures(extName);
  };
}
