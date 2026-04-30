package com.example.archive.archive;

import com.example.archive.config.RetryConfig;
import org.junit.jupiter.api.Test;
import org.springframework.retry.support.RetryTemplate;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that the {@link RetryTemplate} configured in {@link RetryConfig} actually
 * retries the documented exception types and gives up at the configured attempt limit.
 * No Spring context — the bean is constructed directly so the test stays fast.
 */
class PartitionArchiveJobRetryTest {

    private final RetryTemplate retry = new RetryConfig().archiveRetryTemplate();

    @Test
    void retriesS3ExceptionUpToThreeAttemptsThenSucceeds() {
        AtomicInteger calls = new AtomicInteger();
        String result = retry.execute(ctx -> {
            int attempt = calls.incrementAndGet();
            if (attempt < 3) {
                throw S3Exception.builder()
                        .message("transient")
                        .awsErrorDetails(AwsErrorDetails.builder().errorCode("InternalError").build())
                        .statusCode(500)
                        .build();
            }
            return "ok";
        });

        assertThat(calls.get()).isEqualTo(3);
        assertThat(result).isEqualTo("ok");
    }

    @Test
    void surfacesExceptionAfterAttemptsExhausted() {
        AtomicInteger calls = new AtomicInteger();
        assertThatThrownBy(() -> retry.execute(ctx -> {
            calls.incrementAndGet();
            throw S3Exception.builder()
                    .message("always fails")
                    .awsErrorDetails(AwsErrorDetails.builder().errorCode("InternalError").build())
                    .statusCode(500)
                    .build();
        })).isInstanceOf(S3Exception.class);

        assertThat(calls.get()).isEqualTo(3);
    }

    @Test
    void doesNotRetryNonTransientExceptions() {
        AtomicInteger calls = new AtomicInteger();
        assertThatThrownBy(() -> retry.execute(ctx -> {
            calls.incrementAndGet();
            throw new IllegalArgumentException("config error");
        })).isInstanceOf(IllegalArgumentException.class);

        // Configuration errors should fail fast — there's nothing transient to recover from,
        // so wasting two more attempts and ~3.5 s of backoff would only delay the alert.
        assertThat(calls.get()).isOne();
    }
}
