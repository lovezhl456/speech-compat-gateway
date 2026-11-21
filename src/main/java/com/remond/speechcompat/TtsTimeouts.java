package com.remond.speechcompat;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("compat.tts-timeouts")
record TtsTimeouts(long readyMs, long firstAudioMs, long audioIdleMs, long totalMs) {
    TtsTimeouts {
        if (readyMs <= 0 || firstAudioMs <= 0 || audioIdleMs <= 0 || totalMs <= 0) {
            throw new IllegalArgumentException("TTS timeouts must be positive");
        }
    }
}
