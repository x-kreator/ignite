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
package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.function.Supplier;

/**
 *
 */
public interface PageIdsDumpStore {
    /**
     * @param part Partition.
     * @param zone Zone.
     */
    public void savePartition(Partition part, int zone);

    /**
     *
     */
    public Iterable<Zone> zones();

    /**
     *
     */
    public void clear();

    /**
     *
     */
    public static class Partition {
        /** */
        private final int id;

        /** */
        private final int cacheId;

        /** */
        private final Supplier<int[]> pageIdxSupplier;

        /**
         * @param id Id.
         * @param cacheId Cache id.
         * @param pageIdxSupplier Page indexes supplier.
         */
        public Partition(int id, int cacheId, Supplier<int[]> pageIdxSupplier) {
            this.id = id;
            this.cacheId = cacheId;
            this.pageIdxSupplier = pageIdxSupplier;
        }

        /**
         *
         */
        public int id() {
            return id;
        }

        /**
         *
         */
        public int cacheId() {
            return cacheId;
        }

        /**
         *
         */
        public Supplier<int[]> pageIdxSupplier() {
            return pageIdxSupplier;
        }
    }

    /**
     *
     */
    public static class Pages {
        /** */
        private final int cacheId;

        /** */
        private final Supplier<long[]> pageIdsSupplier;

        /**
         * @param cacheId Cache id.
         * @param pageIdsSupplier Page IDs supplier.
         */
        public Pages(int cacheId, Supplier<long[]> pageIdsSupplier) {
            this.cacheId = cacheId;
            this.pageIdsSupplier = pageIdsSupplier;
        }

        /**
         *
         */
        public int cacheId() {
            return cacheId;
        }

        /**
         *
         */
        public Supplier<long[]> pageIdsSupplier() {
            return pageIdsSupplier;
        }
    }

    /**
     *
     */
    public static class Zone {
        /** */
        private final Iterable<Partition> partitions;

        /** */
        private final Iterable<Pages> pages;

        /**
         * @param partitions Partitions.
         * @param pages Pages.
         */
        public Zone(
            Iterable<Partition> partitions,
            Iterable<Pages> pages) {
            this.partitions = partitions;
            this.pages = pages;
        }

        /**
         *
         */
        public Iterable<Partition> partitions() {
            return partitions;
        }

        /**
         *
         */
        public Iterable<Pages> pages() {
            return pages;
        }
    }
}
