import { unsafeWindow } from '$';
import Dexie, { KeyPaths } from 'dexie';
import { exportDB, importInto } from 'dexie-export-import';

import packageJson from '@/../package.json';
import { Capture, Tweet, User } from '@/types';
import { extractTweetMedia } from '@/utils/api';
import { parseTwitterDateTime } from '@/utils/common';
import { migration_20250609 } from '@/utils/migration';
import logger from '@/utils/logger';
import { ExtensionType } from '../extensions';
import { options } from '../options';
import { emitDatabaseMutation } from './mutation';

const DB_NAME = packageJson.name;
const DB_VERSION = 2;
const CAPTURE_COUNT_SNAPSHOT_KEY = '__twe_capture_counts_v1';
const CAPTURE_COUNT_SNAPSHOT_V2_KEY = '__twe_capture_counts_v2';
const ACTIVE_DB_NAME_KEY = '__twe_active_db_name_v1';
const CAPTURE_COUNT_EVENT_NAME = 'twe:capture-count-updated-v1';

const BOOKMARK_CONTEXT_FIELDS = [
  '__bookmark_folder_id',
  '__bookmark_folder_name',
  '__bookmark_folder_name_source',
  '__bookmark_folder_url',
] as const;

interface BookmarkFolderNameBackfillOptions {
  candidateTweetIds?: string[];
  candidateLimit?: number;
  recentCaptureScanLimit?: number;
}

interface BookmarkFolderNameBackfillSummary {
  candidates: number;
  inspected: number;
  updated: number;
}

function mergeTweetMetadata(existing: unknown, incoming: Tweet): Tweet {
  if (!existing || typeof existing !== 'object') {
    return incoming;
  }

  const merged = { ...incoming } as unknown as Record<string, unknown>;
  const existingObj = existing as unknown as Record<string, unknown>;

  for (const field of BOOKMARK_CONTEXT_FIELDS) {
    const existingValue = existingObj[field];
    const incomingValue = (incoming as unknown as Record<string, unknown>)[field];

    if (incomingValue === undefined && existingValue !== undefined) {
      merged[field] = existingValue;
      continue;
    }

    if (incomingValue === null && existingValue !== undefined && existingValue !== null) {
      merged[field] = existingValue;
      continue;
    }

    if (field === '__bookmark_folder_name_source') {
      const incomingSource = String(incomingValue || '');
      const existingSource = String(existingValue || '');
      if (incomingSource === 'id-only' && existingSource === 'api') {
        merged[field] = existingSource;
      }
    }
  }

  return merged as unknown as Tweet;
}

declare global {
  interface Window {
    __META_DATA__: {
      userId: string;
      userHash: string;
    };
  }
}

export class DatabaseManager {
  private db: Dexie;

  constructor() {
    let userId = 'unknown';
    try {
      const globalObject = (unsafeWindow ??
        (typeof window !== 'undefined' ? window : undefined) ??
        globalThis) as typeof globalThis & {
        __META_DATA__?: { userId?: string };
      };
      userId = globalObject.__META_DATA__?.userId ?? 'unknown';
    } catch {
      userId = 'unknown';
    }
    const suffix = options.get('dedicatedDbForAccounts') ? `_${userId}` : '';
    logger.debug(`Using database: ${DB_NAME}${suffix} for userId: ${userId}`);

    this.db = new Dexie(`${DB_NAME}${suffix}`);
    this.publishActiveDatabaseName();
    this.init();
  }

  /*
  |--------------------------------------------------------------------------
  | Type-Safe Table Accessors
  |--------------------------------------------------------------------------
  */

  private tweets() {
    return this.db.table<Tweet>('tweets');
  }

  private users() {
    return this.db.table<User>('users');
  }

  private captures() {
    return this.db.table<Capture>('captures');
  }

  /*
  |--------------------------------------------------------------------------
  | Read Methods for Extensions
  |--------------------------------------------------------------------------
  */

  async extGetCaptures(extName: string) {
    return this.captures().where('extension').equals(extName).toArray().catch(this.logError);
  }

  async extGetCaptureCount(extName: string) {
    return this.captures().where('extension').equals(extName).count().catch(this.logError);
  }

  async extGetCapturedTweets(extName: string) {
    const captures = await this.extGetCaptures(extName);
    if (!captures) {
      return [];
    }
    const tweetIds = captures.map((capture) => capture.data_key);
    return this.tweets()
      .where('rest_id')
      .anyOf(tweetIds)
      .filter((t) => this.filterEmptyData(t))
      .toArray()
      .catch(this.logError);
  }

  async extGetCapturedUsers(extName: string) {
    const captures = await this.extGetCaptures(extName);
    if (!captures) {
      return [];
    }
    const userIds = captures.map((capture) => capture.data_key);
    return this.users()
      .where('rest_id')
      .anyOf(userIds)
      .filter((t) => this.filterEmptyData(t))
      .toArray()
      .catch(this.logError);
  }

  /*
  |--------------------------------------------------------------------------
  | Write Methods for Extensions
  |--------------------------------------------------------------------------
  */

  async extAddTweets(extName: string, tweets: Tweet[]) {
    if (!tweets.length) {
      return;
    }

    await this.upsertTweets(tweets);
    await this.upsertCaptures(
      tweets.map((tweet) => ({
        id: `${extName}-${tweet.rest_id}`,
        extension: extName,
        type: ExtensionType.TWEET,
        data_key: tweet.rest_id,
        created_at: Date.now(),
      })),
    );

    emitDatabaseMutation({
      extension: extName,
      operation: 'extAddTweets',
    });
    void this.publishCaptureCountSnapshot(extName);
  }

  async extAddUsers(extName: string, users: User[]) {
    if (!users.length) {
      return;
    }

    await this.upsertUsers(users);
    await this.upsertCaptures(
      users.map((user) => ({
        id: `${extName}-${user.rest_id}`,
        extension: extName,
        type: ExtensionType.USER,
        data_key: user.rest_id,
        created_at: Date.now(),
      })),
    );

    emitDatabaseMutation({
      extension: extName,
      operation: 'extAddUsers',
    });
    void this.publishCaptureCountSnapshot(extName);
  }

  /*
  |--------------------------------------------------------------------------
  | Delete Methods for Extensions
  |--------------------------------------------------------------------------
  */

  async extClearCaptures(extName: string) {
    const captures = await this.extGetCaptures(extName);
    if (!captures) {
      return;
    }
    const result = await this.captures().bulkDelete(captures.map((capture) => capture.id));
    emitDatabaseMutation({
      extension: extName,
      operation: 'extClearCaptures',
    });
    void this.publishCaptureCountSnapshot(extName);
    return result;
  }

  async extBackfillRecentBookmarkFolderName(
    extName: string,
    folderId: string,
    folderName: string,
    options: BookmarkFolderNameBackfillOptions = {},
  ): Promise<BookmarkFolderNameBackfillSummary> {
    if (!extName || !folderId || !folderName) {
      return { candidates: 0, inspected: 0, updated: 0 };
    }

    const candidateLimit = Math.max(1, Math.min(1000, Number(options.candidateLimit) || 250));
    const recentCaptureScanLimit = Math.max(
      100,
      Math.min(5000, Number(options.recentCaptureScanLimit) || 1800),
    );

    const candidateIds = new Set<string>();
    for (const id of options.candidateTweetIds || []) {
      if (typeof id !== 'string') continue;
      const normalized = id.trim();
      if (!normalized) continue;
      candidateIds.add(normalized);
      if (candidateIds.size >= candidateLimit) break;
    }

    if (candidateIds.size < candidateLimit) {
      const recent = await this.captures()
        .orderBy('created_at')
        .reverse()
        .limit(recentCaptureScanLimit)
        .toArray()
        .catch(this.logError);

      for (const row of recent || []) {
        if (row?.extension !== extName || row?.type !== ExtensionType.TWEET) {
          continue;
        }

        const normalized = String(row?.data_key || '').trim();
        if (!normalized || candidateIds.has(normalized)) {
          continue;
        }

        candidateIds.add(normalized);
        if (candidateIds.size >= candidateLimit) {
          break;
        }
      }
    }

    if (!candidateIds.size) {
      return { candidates: 0, inspected: 0, updated: 0 };
    }

    const candidateArray = [...candidateIds];

    return await this.db
      .transaction('rw', this.tweets(), async () => {
        const rows = await this.tweets().where('rest_id').anyOf(candidateArray).toArray();

        const updates: Tweet[] = [];
        for (const row of rows) {
          const current = row as unknown as Record<string, unknown>;
          if (String(current.__bookmark_folder_id || '') !== folderId) {
            continue;
          }

          const currentName = String(current.__bookmark_folder_name || '');
          const currentSource = String(current.__bookmark_folder_name_source || '');
          if (currentName === folderName && currentSource === 'api') {
            continue;
          }

          updates.push({
            ...row,
            ...({
              __bookmark_folder_name: folderName,
              __bookmark_folder_name_source: 'api',
            } as unknown as Partial<Tweet>),
          } as Tweet);
        }

        if (updates.length) {
          await this.tweets().bulkPut(updates);
          emitDatabaseMutation({
            extension: extName,
            operation: 'bookmarkFolderNameBackfill',
          });
        }

        return {
          candidates: candidateArray.length,
          inspected: rows.length,
          updated: updates.length,
        };
      })
      .catch((error) => {
        this.logError(error);
        return {
          candidates: candidateArray.length,
          inspected: 0,
          updated: 0,
        };
      });
  }

  /*
  |--------------------------------------------------------------------------
  | Export and Import Methods
  |--------------------------------------------------------------------------
  */

  async export() {
    return exportDB(this.db).catch(this.logError);
  }

  async import(data: Blob) {
    const result = await importInto(this.db, data).catch(this.logError);
    emitDatabaseMutation({
      operation: 'import',
    });
    this.publishCaptureCountSnapshotForAllKnownExtensions();
    return result;
  }

  async clear() {
    await this.deleteAllCaptures();
    await this.deleteAllTweets();
    await this.deleteAllUsers();
    emitDatabaseMutation({
      operation: 'clear',
    });
    this.publishCaptureCountSnapshotForAllKnownExtensions();
    logger.info('Database cleared');
  }

  async count() {
    try {
      return {
        tweets: await this.tweets().count(),
        users: await this.users().count(),
        captures: await this.captures().count(),
      };
    } catch (error) {
      this.logError(error);
      return null;
    }
  }

  private async publishCaptureCountSnapshot(extName: string): Promise<void> {
    try {
      const count = Number((await this.extGetCaptureCount(extName)) || 0);
      const dbName = this.db.name;
      const updatedAt = Date.now();
      const globalObject = globalThis as Record<string, unknown>;
      const current = globalObject[CAPTURE_COUNT_SNAPSHOT_KEY];
      const map =
        current && typeof current === 'object'
          ? ({ ...(current as Record<string, number>) } as Record<string, number>)
          : ({} as Record<string, number>);
      map[extName] = count;

      const currentV2 = globalObject[CAPTURE_COUNT_SNAPSHOT_V2_KEY];
      const mapV2 =
        currentV2 && typeof currentV2 === 'object'
          ? ({ ...(currentV2 as Record<string, unknown>) } as Record<string, unknown>)
          : ({} as Record<string, unknown>);
      mapV2[extName] = { count, dbName, updatedAt };

      globalObject[CAPTURE_COUNT_SNAPSHOT_KEY] = map;
      globalObject[CAPTURE_COUNT_SNAPSHOT_V2_KEY] = mapV2;
      if (typeof window !== 'undefined') {
        (window as unknown as Record<string, unknown>)[CAPTURE_COUNT_SNAPSHOT_KEY] = map;
        (window as unknown as Record<string, unknown>)[CAPTURE_COUNT_SNAPSHOT_V2_KEY] = mapV2;
      }

      try {
        if (typeof localStorage !== 'undefined') {
          localStorage.setItem(CAPTURE_COUNT_SNAPSHOT_KEY, JSON.stringify(map));
          localStorage.setItem(CAPTURE_COUNT_SNAPSHOT_V2_KEY, JSON.stringify(mapV2));
        }
      } catch {
        // ignore localStorage failures
      }

      try {
        if (typeof window !== 'undefined' && typeof window.dispatchEvent === 'function') {
          const detail = {
            extension: extName,
            count,
            dbName,
            updatedAt,
          };
          try {
            window.dispatchEvent(
              new CustomEvent(CAPTURE_COUNT_EVENT_NAME, {
                detail,
              }),
            );
          } catch {
            window.dispatchEvent(new Event(CAPTURE_COUNT_EVENT_NAME));
          }
        }
      } catch {
        // ignore event dispatch failures
      }
    } catch {
      // ignore snapshot failures
    }
  }

  private publishCaptureCountSnapshotForAllKnownExtensions(): void {
    void this.captures()
      .toArray()
      .then((rows) => {
        const set = new Set<string>();
        for (const row of rows) {
          if (row?.extension) {
            set.add(String(row.extension));
          }
        }
        return Promise.all([...set].map((extName) => this.publishCaptureCountSnapshot(extName)));
      })
      .catch(() => {
        // ignore
      });
  }

  private publishActiveDatabaseName(): void {
    try {
      const dbName = this.db.name;
      const globalObject = globalThis as Record<string, unknown>;
      globalObject[ACTIVE_DB_NAME_KEY] = dbName;
      if (typeof window !== 'undefined') {
        (window as unknown as Record<string, unknown>)[ACTIVE_DB_NAME_KEY] = dbName;
      }
      if (typeof localStorage !== 'undefined') {
        localStorage.setItem(ACTIVE_DB_NAME_KEY, dbName);
      }
    } catch {
      // ignore
    }
  }

  /*
  |--------------------------------------------------------------------------
  | Common Methods
  |--------------------------------------------------------------------------
  */

  async upsertTweets(tweets: Tweet[]) {
    if (!tweets.length) {
      return;
    }

    return this.db
      .transaction('rw', this.tweets(), async () => {
        const ids = tweets.map((tweet) => tweet.rest_id);
        const existingRows = await this.tweets().where('rest_id').anyOf(ids).toArray();
        const existingById = new Map(existingRows.map((row) => [String(row.rest_id), row]));

        const data: Tweet[] = tweets.map((tweet) => {
          const normalized = {
            ...tweet,
            twe_private_fields: {
              created_at: +parseTwitterDateTime(tweet.legacy.created_at),
              updated_at: Date.now(),
              media_count: extractTweetMedia(tweet).length,
            },
          };

          return mergeTweetMetadata(existingById.get(tweet.rest_id) ?? null, normalized);
        });

        return this.tweets().bulkPut(data);
      })
      .catch(this.logError);
  }

  async upsertUsers(users: User[]) {
    return this.db
      .transaction('rw', this.users(), () => {
        const data: User[] = users.map((user) => ({
          ...user,
          twe_private_fields: {
            created_at: +parseTwitterDateTime(user.core.created_at),
            updated_at: Date.now(),
          },
        }));

        return this.users().bulkPut(data);
      })
      .catch(this.logError);
  }

  async upsertCaptures(captures: Capture[]) {
    return this.db
      .transaction('rw', this.captures(), () => {
        return this.captures().bulkPut(captures).catch(this.logError);
      })
      .catch(this.logError);
  }

  async deleteAllTweets() {
    return this.tweets().clear().catch(this.logError);
  }

  async deleteAllUsers() {
    return this.users().clear().catch(this.logError);
  }

  async deleteAllCaptures() {
    return this.captures().clear().catch(this.logError);
  }

  private filterEmptyData(data: Tweet | User) {
    if (!data?.legacy) {
      logger.warn('Empty data found in DB', data);
      return false;
    }
    return true;
  }

  /*
  |--------------------------------------------------------------------------
  | Migrations
  |--------------------------------------------------------------------------
  */

  async init() {
    // Indexes for the "tweets" table.
    const tweetIndexPaths: KeyPaths<Tweet>[] = [
      'rest_id',
      'twe_private_fields.created_at',
      'twe_private_fields.updated_at',
      'twe_private_fields.media_count',
      'core.user_results.result.core.screen_name',
      'legacy.favorite_count',
      'legacy.retweet_count',
      'legacy.bookmark_count',
      'legacy.quote_count',
      'legacy.reply_count',
      'views.count',
      'legacy.favorited',
      'legacy.retweeted',
      'legacy.bookmarked',
    ];

    // Indexes for the "users" table.
    const userIndexPaths: KeyPaths<User>[] = [
      'rest_id',
      'twe_private_fields.created_at',
      'twe_private_fields.updated_at',
      'core.screen_name',
      'legacy.followers_count',
      'legacy.friends_count',
      'legacy.statuses_count',
      'legacy.favourites_count',
      'legacy.listed_count',
      'verification.verified_type',
      'is_blue_verified',
      'relationship_perspectives.following',
      'relationship_perspectives.followed_by',
    ];

    // Indexes for the "captures" table.
    const captureIndexPaths: KeyPaths<Capture>[] = ['id', 'extension', 'type', 'created_at'];

    // Take care of database schemas and versioning.
    // See: https://dexie.org/docs/Tutorial/Design#database-versioning
    try {
      this.db
        .version(DB_VERSION)
        .stores({
          tweets: tweetIndexPaths.join(','),
          users: userIndexPaths.join(','),
          captures: captureIndexPaths.join(','),
        })
        .upgrade(async (tx) => {
          logger.info('Upgrading database schema...');
          await migration_20250609(tx);
          logger.info('Database upgraded');
        });

      await this.db.open();
      logger.info(`Database connected: ${this.db.name}`);
    } catch (error) {
      this.logError(error);
    }
  }

  /*
  |--------------------------------------------------------------------------
  | Loggers
  |--------------------------------------------------------------------------
  */

  logError(error: unknown) {
    logger.error(`Database Error: ${(error as Error).message}`, error);
  }
}
