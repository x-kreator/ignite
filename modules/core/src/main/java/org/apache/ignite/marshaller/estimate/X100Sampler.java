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

/**
 * {@code X100Sampler} is a {@code Sampler} implementation which makes arrays of 100 sampled instances for each data
 * model considering their field stats.
 */
public class X100Sampler extends AbstractSampler {
    /** {@inheritDoc} */
    @Override protected Object sample(DataModel dataModel) throws SamplingException {
        final Object[] samples = new Object[100];

        for (int i = 0; i < samples.length; i++) {
            samples[i] = sampleFields(
                newInstance(dataModel.className()),
                dataModel.fieldStatsMap(),
                i,
                null);
        }

        return samples;
    }
}
