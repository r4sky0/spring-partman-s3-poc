package com.example.archive.archive;

/**
 * Exports a single partition's contents to remote storage. Implementations are
 * responsible only for the export step; transactional bookkeeping (archive_log
 * insert, DETACH, DROP) is handled by {@link PartitionArchiveJob}.
 */
public interface PartitionArchiver {
    ArchiveResult archive(PartitionInfo partition);
}
