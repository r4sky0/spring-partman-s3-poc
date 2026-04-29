package com.example.archive.archive;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/archive")
public class ArchiveController {

    private final PartitionArchiveJob job;

    public ArchiveController(PartitionArchiveJob job) {
        this.job = job;
    }

    @PostMapping("/run")
    public PartitionArchiveJob.ArchiveSummary run() {
        return job.runOnce();
    }
}
