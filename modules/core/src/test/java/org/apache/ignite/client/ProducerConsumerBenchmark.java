package org.apache.ignite.client;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public class ProducerConsumerBenchmark {
    /** Default seed. */
    private static final int DFLT_SEED = 12345678;

    /** Chars. */
    private static final char[] CHARS = new char[40];

    static {
        "ABCDEFGHI JKLMNOPQR STUVWXYZ0 123456789 ".getChars(0, CHARS.length, CHARS, 0);
    }

    /** Cache name. */
    private static final String CACHE_NAME = "test_cache";

    /*private final long seed;

    private final*/

    /**
     * @param cnt Key count.
     */
    private String[] generateKeys(int cnt, int minKeySize, int maxKeySize, long seed) {
        Random rnd = ThreadLocalRandom.current();

        String[] keys = new String[cnt];

        for (int i = 0; i < keys.length; i++) {
            int keySize = rnd.nextInt(maxKeySize - minKeySize) + minKeySize + 1;

            char[] keyChars = new char[keySize];

            for (int j = 0; j < keySize; j++)
                keyChars[j] = CHARS[rnd.nextInt(CHARS.length)];

            keys[i] = new String(keyChars);
        }

        return keys;
    }

    /**
     *
     */
    public static class Builder {

    }
}
