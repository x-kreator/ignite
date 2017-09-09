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

import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Abstract {@code Sampler} implementation
 */
public abstract class AbstractSampler implements Sampler {
    /** */
    private final SampleFactory sampleFactory;

    protected AbstractSampler(SampleFactory sampleFactory) {
        this.sampleFactory = sampleFactory;
    }

    /** {@inheritDoc} */
    @Override public Object[] sample(DataModel... dataModels) throws SamplingException {
        if (dataModels == null || dataModels.length == 0)
            return null;

        final Object[] samples = new Object[dataModels.length];

        for (int i = 0; i < samples.length; i++) {
            if (dataModels[i] == null)
                throw new SamplingException("Bad dataModels item found [dataModel#" + i + " = null]");

            samples[i] = sample(dataModels[i]);
        }

        return samples;
    }

    /**
     * Samples specified single data model.
     *
     * @param dataModel Data model to sample.
     * @return Sampled object(s) of specified data model.
     * @throws SamplingException when error occurs during sampling process.
     */
    protected abstract Object sample(DataModel dataModel) throws SamplingException;

    /**
     * @param sample
     * @param fieldStatsMap
     * @param index
     * @param parentFieldName
     * @return Sample object with sampled fields.
     * @throws SamplingException
     */
    protected Object sampleFields(
        Object sample,
        Map<String, DataModel.FieldStats> fieldStatsMap,
        int index,
        String parentFieldName) throws SamplingException {

        if (sample.getClass().getName().startsWith("java."))
            return sample;

        for (Class cls = sample.getClass(); cls != Object.class; cls = cls.getSuperclass()) {
            for (java.lang.reflect.Field field : cls.getDeclaredFields()) {
                if (field.getType().isPrimitive())
                    continue;

                final String statsFieldName = parentFieldName != null ?
                    parentFieldName + "." + field.getName() :
                    field.getName();

                DataModel.FieldStats stats = null;

                if (fieldStatsMap != null)
                    stats = fieldStatsMap.get(statsFieldName);

                if (stats != null && stats.nullsPercent() != null && stats.nullsPercent() < index) {
                    setValue(field, sample, null);

                    continue;
                }

                if (field.getType().getName().startsWith("java.lang."))
                    continue;

                if (field.getType().isArray()) {
                    if (stats == null || stats.averageSize() == null) {
                        throw new SamplingException(
                            "No fieldStat or averageSize for array field '" + statsFieldName
                                + "' of class " + sample.getClass()
                                + " in dataModel[" + index + "]");
                    }

                    final Class<?> elementType = field.getType().getComponentType();

                    final Object arrayObj = Array.newInstance(elementType, stats.averageSize());

                    if (!elementType.isPrimitive()) {
                        final Object[] array = (Object[])arrayObj;

                        for (int i = 0; i < array.length; i++) {
                            array[i] = newInstance(elementType);

                            sampleFields(array[i], fieldStatsMap, index, statsFieldName);
                        }
                    }

                    setValue(field, sample, arrayObj);

                    continue;
                }

                if (getValue(field, sample) == null) {
                    setValue(
                        field,
                        sample,
                        sampleFields(
                            newInstance(field.getType()),
                            fieldStatsMap,
                            index,
                            statsFieldName));
                }
            }
        }

        return sample;
    }

    /**
     * @param sample
     * @param dataModel
     * @param parentFieldName
     * @param nullPredicate
     * @return
     * @throws SamplingException
     */
    protected Sample sampleFields(
        Sample sample,
        DataModel dataModel,
        String parentFieldName,
        IgnitePredicate<DataModel.FieldStats> nullPredicate) throws SamplingException {

        if (sample.className().startsWith("java."))
            return sample;

        for (Sample.Field field : sample.fields()) {
            if (field.type().isPrimitive())
                continue;

            final String statsFieldName = parentFieldName != null ?
                parentFieldName + "." + field.name() :
                field.name();

            DataModel.FieldStats stats = null;

            if (dataModel.fieldStatsMap() != null)
                stats = dataModel.fieldStatsMap().get(statsFieldName);

            if (nullPredicate.apply(stats)) {
                field.value(null);

                continue;
            }

            if (String.class == field.type()) {
                if (stats != null && stats.averageSize() != null)
                    field.value(new String(new char[stats.averageSize()]));
                else if (field.value() == null)
                    throw new SamplingException(
                        "No fieldStat or averageSize for string field '" + statsFieldName
                            + "' in dataModel[className = " + dataModel.className() + "]");
            }

            if (field.type().getName().startsWith("java.lang."))
                continue;

            if (field.type().isArray()) {
                if (stats == null || stats.averageSize() == null) {
                    throw new SamplingException(
                        "No fieldStat or averageSize for array field '" + statsFieldName
                            + "' in dataModel[className = " + dataModel.className() + "]");
                }

                final Class<?> elementType = field.type().getComponentType();

                final Object arrayObj = Array.newInstance(elementType, stats.averageSize());

                if (!elementType.isPrimitive()) {
                    final Object[] array = (Object[])arrayObj;

                    for (int i = 0; i < array.length; i++) {
                        array[i] = newInstance(elementType);

                        sampleFields(new ReflectionSample(array[i]), dataModel, statsFieldName, nullPredicate);
                    }
                }

                field.value(arrayObj);

                continue;
            }

            if (field.value() == null) {
                final Object value = newInstance(field.type());

                sampleFields(new ReflectionSample(value), dataModel, statsFieldName, nullPredicate);

                field.value(value);
            }
        }
        return sample;
    }

    /**
     * @param builder
     * @param fieldStatsMap
     * @param index
     * @param parentFieldName
     * @return
     * @throws SamplingException
     */
    protected BinaryObjectBuilder sampleFields(
        BinaryObjectBuilder builder,
        Map<String, DataModel.FieldStats> fieldStatsMap,
        int index,
        String parentFieldName) throws SamplingException {

        return null; // FIXME
    }

    protected Sample createSample(DataModel dataModel) throws SamplingException {
        // FIXME
        //return new ReflectionSample(newInstance(dataModel.className()));
        return sampleFactory.createSample(dataModel);
    }

    /**
     * @param cls
     * @return
     * @throws SamplingException
     */
    private Object newInstance(Class<?> cls) throws SamplingException {
        try {
            if (Number.class.isAssignableFrom(cls)) {
                Constructor<?> constructor = cls.getConstructor(String.class);

                return constructor.newInstance("1");
            }

            return U.newInstance(cls);
        }
        catch (IgniteCheckedException | ReflectiveOperationException e) {
            throw new SamplingException(e);
        }
    }

    /**
     * @param field
     * @param obj
     * @return
     * @throws SamplingException
     */
    private Object getValue(java.lang.reflect.Field field, Object obj) throws SamplingException {
        boolean accessible = field.isAccessible();

        field.setAccessible(true);

        try {
            return field.get(obj);
        }
        catch (IllegalAccessException e) {
            throw new SamplingException(e);
        }
        finally {
            if (!accessible)
                field.setAccessible(false);
        }
    }

    /**
     * @param field
     * @param obj
     * @param value
     * @throws SamplingException
     */
    private void setValue(java.lang.reflect.Field field, Object obj, Object value) throws SamplingException {
        boolean accessible = field.isAccessible();

        field.setAccessible(true);

        try {
            field.set(obj, value);
        }
        catch (IllegalAccessException e) {
            throw new SamplingException(e);
        }
        finally {
            if (!accessible)
                field.setAccessible(false);
        }
    }
}
