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

package org.apache.ignite.marshaller.estimate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObjectBuilder;

/**
 *
 */
public class BinarySample implements Sample {
    /** */
    private final BinaryObjectBuilder builder;
    /** */
    private final String className;
    /** */
    private final List<Field> fields;

    /**
     * @param binary
     * @param className
     * @param fieldTypes
     * @throws SamplingException
     */
    public BinarySample(
        IgniteBinary binary,
        String className,
        Map<String, String> fieldTypes) throws SamplingException {

        try {
            this.builder = binary.builder(className);
        }
        catch (IgniteException e) {
            throw new SamplingException(e);
        }

        this.className = className;

        final List<Field> fields = new ArrayList<>();

        for (Map.Entry<String, String> fieldType : fieldTypes.entrySet()) {
            try {
                final Class<?> fieldClass = Class.forName(fieldType.getValue());

                fields.add(new BinaryField<>(
                    fieldType.getKey(),
                    fieldClass)
                );
            }
            catch (ClassNotFoundException e) {
                throw new SamplingException(e);
            }
        }

        this.fields = Collections.unmodifiableList(fields);
    }

    @Override public Object sample() {
        return builder.build();
    }

    @Override public String className() {
        return className;
    }

    @Override public Iterable<Field> fields() {
        return fields;
    }

    /**
     * @param <T>
     */
    public class BinaryField<T> implements Field {
        /** */
        private final String name;
        /** */
        private final Class<T> type;

        /**
         * @param name
         * @param type
         */
        public BinaryField(String name, Class<T> type) {
            this.name = name;
            this.type = type;
        }

        @Override public String name() {
            return name;
        }

        @Override public Class<T> type() {
            return type;
        }

        @Override public Object value() throws SamplingException {
            return builder.getField(name);
        }

        @Override public void value(Object value) throws SamplingException {
            builder.setField(name, type.cast(value), type);
        }
    }
}
