package com.example.archive.archive;

public record ArchiveResult(
        String s3Bucket,
        String s3Key,
        long rowCount,
        long bytesUploaded
) {}
