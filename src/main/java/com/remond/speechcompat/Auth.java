package com.remond.speechcompat;

import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

final class Auth {
    private final String appId = setting("TENCENT_ASR_APP_ID");
    private final String tencentId = setting("TENCENT_ASR_SECRET_ID");
    private final String tencentSecret = setting("TENCENT_ASR_SECRET_KEY");
    private final String aliyunId = setting("ALIYUN_AK_ID");
    private final String aliyunSecret = setting("ALIYUN_AK_SECRET");
    private final String appKey = setting("ALIYUN_NLS_APP_KEY");

    static String setting(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing configuration: " + name);
        }
        return value;
    }

    static Map<String, String> query(String raw) {
        var fields = new TreeMap<String, String>();
        if (raw != null) {
            for (String entry : raw.split("&")) {
                int separator = entry.indexOf('=');
                if (separator <= 0 || fields.putIfAbsent(decode(entry.substring(0, separator)),
                        decode(entry.substring(separator + 1))) != null) {
                    throw new IllegalArgumentException("Invalid query");
                }
            }
        }
        return fields;
    }

    boolean tencent(String path, Map<String, String> fields, String header) {
        long now = Instant.now().getEpochSecond();
        long timestamp = Long.parseLong(fields.getOrDefault("timestamp", "0"));
        long expires = Long.parseLong(fields.getOrDefault("expired", "0"));
        if (!path.equals("/asr/v2/" + appId) || !tencentId.equals(fields.get("secretid"))
                || !fields.getOrDefault("voice_id", "").matches("[A-Za-z0-9_-]{1,128}")
                || !matches(fields, Map.of("engine_model_type", "8k_zh", "voice_format", "1",
                        "needvad", "1", "vad_silence_time", "700"))
                || timestamp > now + 300 || expires < now || expires < timestamp) {
            return false;
        }
        var signed = new TreeMap<>(fields);
        String provided = signed.remove("signature");
        String canonical = "asr.cloud.tencent.com/asr/v2/" + appId + "?" + join(signed, false);
        return equal(provided, sign("HmacSHA1", tencentSecret, canonical))
                && (header == null || equal(provided, header));
    }

    boolean aliyun(Map<String, String> fields) {
        if (!matches(fields, Map.of("Action", "CreateToken", "RegionId", "cn-shanghai",
                "Version", "2019-02-28", "SignatureMethod", "HMAC-SHA1", "AccessKeyId", aliyunId))
                || fields.getOrDefault("SignatureNonce", "").isBlank()) {
            return false;
        }
        long timestamp = Instant.parse(fields.getOrDefault("Timestamp", "")).getEpochSecond();
        if (Math.abs(Instant.now().getEpochSecond() - timestamp) > 300) {
            return false;
        }
        var signed = new TreeMap<>(fields);
        String provided = signed.remove("Signature");
        return equal(provided, sign("HmacSHA1", aliyunSecret + "&", "GET&%2F&" + encode(join(signed, true))));
    }

    String token(long expires) {
        String value = expires + "." + UUID.randomUUID();
        return value + "." + sign("HmacSHA256", aliyunSecret, value);
    }

    boolean validToken(String token) {
        if (token == null || token.length() > 256) {
            return false;
        }
        int end = token.lastIndexOf('.');
        int start = token.indexOf('.');
        return start > 0 && start < end && token.substring(0, start).matches("[0-9]{1,12}")
                && Long.parseLong(token.substring(0, start)) >= Instant.now().getEpochSecond()
                && equal(token.substring(end + 1), sign("HmacSHA256", aliyunSecret, token.substring(0, end)));
    }

    boolean appKey(String key) {
        return equal(appKey, key);
    }

    private static boolean matches(Map<String, String> actual, Map<String, String> expected) {
        return actual.entrySet().containsAll(expected.entrySet());
    }

    private static String join(Map<String, String> fields, boolean encoded) {
        var values = new java.util.StringJoiner("&");
        fields.forEach((name, value) -> values.add(encoded ? encode(name) + "=" + encode(value) : name + "=" + value));
        return values.toString();
    }

    private static String decode(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8)
                .replace("+", "%20").replace("*", "%2A").replace("%7E", "~");
    }

    private static String sign(String algorithm, String key, String message) {
        try {
            var mac = Mac.getInstance(algorithm);
            mac.init(new SecretKeySpec(key.getBytes(StandardCharsets.UTF_8), algorithm));
            return Base64.getEncoder().encodeToString(mac.doFinal(message.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.GeneralSecurityException error) {
            throw new IllegalStateException("Signing unavailable", error);
        }
    }

    private static boolean equal(String a, String b) {
        return a != null && b != null && MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8),
                b.getBytes(StandardCharsets.UTF_8));
    }
}
