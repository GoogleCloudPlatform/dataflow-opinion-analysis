/*******************************************************************************
 * Copyright 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/

package com.google.cloud.dataflow.examples.opinionanalysis.transforms;

import java.util.concurrent.ThreadLocalRandom;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;


/**
 * Uses a {@link GroupByKey} to split the pipeline in half, so that the
 * stage that includes partitioning (which contains all tuples) is isolated
 * from each individual partition.
 *
 * <p>
 * Works by adding a random key to each incoming element, grouping on that
 * random key, and then removing the keys from all elements after the
 * {@link GroupByKey}.
 */
public class Reshuffle<T> extends PTransform<PCollection<T>, PCollection<T>> {
	private static class AddArbitraryKey<T> extends DoFn<T, KV<Integer, T>> {
		@ProcessElement
		public void processElement(ProcessContext c) throws Exception {
			c.output(KV.of(ThreadLocalRandom.current().nextInt(), c.element()));
		}
	}

	private static class RemoveArbitraryKey<T> extends DoFn<KV<Integer, Iterable<T>>, T> {
		@ProcessElement
		public void processElement(ProcessContext c) throws Exception {
			for (T s : c.element().getValue()) {
				c.output(s);
			}
		}
	}
	@Override
	public PCollection<T> expand(PCollection<T> input) {
		return input.apply(ParDo.of(new AddArbitraryKey<T>())).apply(GroupByKey.<Integer, T>create())
				.apply(ParDo.of(new RemoveArbitraryKey<T>()));
	}
}
