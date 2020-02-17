/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.yardstick.thin.cache;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 */
public class RandomStringData {
    /** Chars. */
    private static final char[] CHARS = new char[40];

    static {
        "ABCDEFGHI JKLMNOPQR STUVWXYZ0 123456789 ".getChars(0, CHARS.length, CHARS, 0);
    }

    /** Keys. */
    private final String[] keys;

    /** Messages. */
    private final String[] messages;

    /**
     * Default constructor.
     */
    public RandomStringData() {
        this(new Options());
    }

    /**
     * @param options Options.
     */
    public RandomStringData(Options options) {
        Random rnd = new Random(options.seed());

        keys = randomStrings(
            options.keyCount(),
            options.minKeySize(),
            options.maxKeySize(),
            rnd);

        messages = randomStrings(
            options.messageCount(),
            options.minMessageSize(),
            options.maxMessageSize(),
            rnd);
    }

    /**
     *
     */
    public int keyCount() {
        return keys.length;
    }

    /**
     * @param idx Index.
     */
    public String key(int idx) {
        return keys[idx];
    }

    /**
     *
     */
    public int messageCount() {
        return messages.length;
    }

    /**
     * @param idx Index.
     */
    public String message(int idx) {
        return messages[idx];
    }

    /**
     *
     */
    public String randomMessage() {
        return messages[ThreadLocalRandom.current().nextInt(messages.length)];
    }

    /**
     * @param cnt Count.
     * @param minStrSize Min string size.
     * @param maxStrSize Max string size.
     * @param rnd Random.
     */
    private static String[] randomStrings(int cnt, int minStrSize, int maxStrSize, Random rnd) {
        String[] strings = new String[cnt];

        for (int i = 0; i < strings.length; i++) {
            int keySize = rnd.nextInt(maxStrSize - minStrSize) + minStrSize + 1;

            char[] keyChars = new char[keySize];

            for (int j = 0; j < keySize; j++)
                keyChars[j] = CHARS[rnd.nextInt(CHARS.length)];

            strings[i] = new String(keyChars);
        }

        return strings;
    }

    /**
     *
     */
    public static class Options {
        /** Default key count. */
        private static final int DFLT_KEY_CNT = 1_000_000;
        /** Default min key size. */
        private static final int DFLT_MIN_KEY_SIZE = 40;
        /** Default max key size. */
        private static final int DFLT_MAX_KEY_SIZE = 50;

        /** Default message count. */
        private static final int DFLT_MSG_CNT = 100_000;
        /** Default min message size. */
        private static final int DFLT_MIN_MSG_SIZE = 500;
        /** Default max message size. */
        private static final int DFLT_MAX_MSG_SIZE = 1500;

        /** Default seed. */
        private static final int DFLT_SEED = 12345678;

        /** Key count. */
        private int keyCnt = DFLT_KEY_CNT;
        /** Min key size. */
        private int minKeySize = DFLT_MIN_KEY_SIZE;
        /** Max key size. */
        private int maxKeySize = DFLT_MAX_KEY_SIZE;

        /** Message count. */
        private int msgCnt = DFLT_MSG_CNT;
        /** Min message size. */
        private int minMsgSize = DFLT_MIN_MSG_SIZE;
        /** Max message size. */
        private int maxMsgSize = DFLT_MAX_MSG_SIZE;

        /** Seed. */
        private int seed = DFLT_SEED;

        /**
         * @return Key count.
         */
        public int keyCount() {
            return keyCnt;
        }

        /**
         * @param keyCnt New key count.
         */
        public void keyCount(int keyCnt) {
            this.keyCnt = keyCnt;
        }

        /**
         * @return Min key size.
         */
        public int minKeySize() {
            return minKeySize;
        }

        /**
         * @param minKeySize New min key size.
         */
        public void minKeySize(int minKeySize) {
            this.minKeySize = minKeySize;
        }

        /**
         * @return Max key size.
         */
        public int maxKeySize() {
            return maxKeySize;
        }

        /**
         * @param maxKeySize New max key size.
         */
        public void maxKeySize(int maxKeySize) {
            this.maxKeySize = maxKeySize;
        }

        /**
         * @return Message count.
         */
        public int messageCount() {
            return msgCnt;
        }

        /**
         * @param msgCnt New message count.
         */
        public void messageCount(int msgCnt) {
            this.msgCnt = msgCnt;
        }

        /**
         * @return Min message size.
         */
        public int minMessageSize() {
            return minMsgSize;
        }

        /**
         * @param minMsgSize New min message size.
         */
        public void minMessageSize(int minMsgSize) {
            this.minMsgSize = minMsgSize;
        }

        /**
         * @return Max message size.
         */
        public int maxMessageSize() {
            return maxMsgSize;
        }

        /**
         * @param maxMsgSize New max message size.
         */
        public void maxMessageSize(int maxMsgSize) {
            this.maxMsgSize = maxMsgSize;
        }

        /**
         * @return Seed.
         */
        public int seed() {
            return seed;
        }

        /**
         * @param seed New seed.
         */
        public void seed(int seed) {
            this.seed = seed;
        }
    }
}
