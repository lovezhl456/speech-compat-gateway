package com.remond.speechcompat;

import java.io.ByteArrayOutputStream;

final class Pcm {
    static final int FRAME_BYTES = 320;

    static boolean speech(byte[] frame, int length) {
        long energy = 0;
        for (int i = 0; i < length; i += 2) {
            int sample = sample(frame[i], frame[i + 1]);
            energy += (long) sample * sample;
        }
        return energy >= 400L * 400 * (length / 2);
    }

    static byte[] up(byte[] frame, int length) {
        byte[] result = new byte[length * 3];
        for (int i = 0; i < length; i += 2) {
            int a = sample(frame[i], frame[i + 1]);
            int b = i + 2 < length ? sample(frame[i + 2], frame[i + 3]) : a;
            for (int j = 0; j < 3; j++) {
                write(result, i * 3 + j * 2, (a * (3 - j) + b * j) / 3);
            }
        }
        return result;
    }

    static final class Down {
        private int low = -1;
        private int sum;
        private int count;

        byte[] append(byte[] frame) {
            var output = new ByteArrayOutputStream(frame.length / 3 + 2);
            for (byte value : frame) {
                if (low < 0) {
                    low = value & 255;
                } else {
                    sum += sample(low, value);
                    low = -1;
                    if (++count == 3) {
                        write(output, sum / count);
                        count = sum = 0;
                    }
                }
            }
            return output.toByteArray();
        }

        byte[] finish() {
            if (low != -1) {
                throw new IllegalArgumentException("Incomplete model PCM sample");
            }
            var output = new ByteArrayOutputStream(2);
            if (count > 0) {
                write(output, sum / count);
            }
            count = sum = 0;
            return output.toByteArray();
        }

        private static void write(ByteArrayOutputStream output, int sample) {
            output.write(sample & 255);
            output.write((sample >> 8) & 255);
        }
    }

    private static int sample(int low, int high) {
        return (short) ((low & 255) | (high << 8));
    }

    private static void write(byte[] output, int offset, int sample) {
        output[offset] = (byte) sample;
        output[offset + 1] = (byte) (sample >> 8);
    }
}
