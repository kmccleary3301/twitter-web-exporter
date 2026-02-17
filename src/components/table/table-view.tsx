import { ExportMediaModal } from '@/components/modals/export-media';
import { useCapturedRecords, useClearCaptures } from '@/core/database/hooks';
import { Extension, ExtensionType } from '@/core/extensions';
import { useTranslation } from '@/i18n';
import { Tweet, User } from '@/types';
import { useToggle } from '@/utils/common';
import { ColumnDef } from '@tanstack/table-core';
import { useMemo } from 'preact/hooks';

import { BaseTableView } from './base';
import { columns as columnsTweet } from './columns-tweet';
import { columns as columnsUser } from './columns-user';

type TableViewProps = {
  title: string;
  extension: Extension;
};

type InferDataType<T> = T extends ExtensionType.TWEET ? Tweet : User;

type BookmarkFolderStatus = 'api-name' | 'id-only' | 'none';

function getBookmarkFolderStatus(record: unknown): BookmarkFolderStatus {
  const obj = record as Record<string, unknown>;
  const folderName = obj?.__bookmark_folder_name;
  const folderNameSource = obj?.__bookmark_folder_name_source;
  const folderId = obj?.__bookmark_folder_id;

  if (
    folderNameSource === 'api' &&
    typeof folderName === 'string' &&
    folderName.trim().length > 0
  ) {
    return 'api-name';
  }
  if (typeof folderId === 'string' && folderId.trim().length > 0) {
    return 'id-only';
  }
  return 'none';
}

/**
 * Common table view.
 */
export function TableView({ title, extension }: TableViewProps) {
  const { t } = useTranslation();

  // Infer data type (Tweet or User) from extension type.
  type DataType = InferDataType<typeof extension.type>;

  // Query records from the database.
  const { name, type } = extension;
  const records = useCapturedRecords(name, type);
  const clearCapturedData = useClearCaptures(name);
  const isBookmarksModule = name === 'BookmarksModule' && type === ExtensionType.TWEET;

  const bookmarkStatus = useMemo(() => {
    const items = (records ?? []) as unknown[];
    const counts: Record<BookmarkFolderStatus, number> = {
      'api-name': 0,
      'id-only': 0,
      none: 0,
    };

    for (const item of items) {
      counts[getBookmarkFolderStatus(item)]++;
    }

    const latestStatus =
      items.length > 0 ? getBookmarkFolderStatus(items[items.length - 1]) : ('none' as const);

    return {
      latestStatus,
      counts,
    };
  }, [records]);

  // Control modal visibility for exporting media.
  const [showExportMediaModal, toggleShowExportMediaModal] = useToggle();

  const columns = (
    type === ExtensionType.TWEET ? columnsTweet : columnsUser
  ) as ColumnDef<DataType>[];

  return (
    <BaseTableView
      title={title}
      records={records ?? []}
      columns={columns}
      clear={clearCapturedData}
      renderActions={() => (
        <div class="flex items-center gap-2">
          {isBookmarksModule && (
            <span
              class="badge badge-outline tooltip before:whitespace-pre-line before:max-w-40"
              data-tip={`latest: ${bookmarkStatus.latestStatus}
api-name: ${bookmarkStatus.counts['api-name']}
id-only: ${bookmarkStatus.counts['id-only']}
none: ${bookmarkStatus.counts.none}`}
            >
              folder metadata: {bookmarkStatus.latestStatus}
            </span>
          )}
          <button class="btn btn-secondary" onClick={toggleShowExportMediaModal}>
            {t('Export Media')}
          </button>
        </div>
      )}
      renderExtra={(table) => (
        <ExportMediaModal
          title={title}
          table={table}
          isTweet={type === ExtensionType.TWEET}
          show={showExportMediaModal}
          onClose={toggleShowExportMediaModal}
        />
      )}
    />
  );
}
