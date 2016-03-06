/*
 * Copyright 2015 Otto (GmbH & Co KG)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.flinkspector.datastream.input;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.flinkspector.datastream.DataStreamTestEnvironment;
import org.flinkspector.datastream.input.time.TimeSpan;

/**
 * This builder is used to define input in a fluent way.
 * The functionality of {@link EventTimeInputBuilder} is used to build a list
 * of input records. And converted to a {@link DataStreamSource}.
 *
 * @param <T>
 */
public class ProcessingTimeSourceBuilder<T> {

	private final ProcessingTimeInputBuilder<T> builder;
	private final DataStreamTestEnvironment env;

	public ProcessingTimeSourceBuilder(DataStreamTestEnvironment env, T record) {
		this(env, record, 0);
	}

	public ProcessingTimeSourceBuilder(DataStreamTestEnvironment env, T record, long timestamp) {
		this.env = env;
		this.builder = ProcessingTimeInputBuilder.startWith(record,0);
	}


	/**
	 * Factory method used to dynamically type the {@link ProcessingTimeSourceBuilder}
	 * using the type of the provided input object.
	 *
	 * @param record first record to emit.
	 * @param env    to work on.
	 * @param <T>
	 * @return created {@link SourceBuilder}
	 */
	public static <T> ProcessingTimeSourceBuilder<T> createBuilder(T record,
	                                                          DataStreamTestEnvironment env) {
		return new ProcessingTimeSourceBuilder<>(env, record);
	}

	/**
	 * Produces a {@link DataStreamSource} with the predefined input.
	 *
	 * @return {@link DataStreamSource}
	 */
	public DataStreamSource<T> close() {
		return env.fromInput(builder);
	}

	/**
	 * Repeat the current input list, after the defined span.
	 * The time span between records in your already defined list will
	 * be kept.
	 *
	 */
	public ProcessingTimeSourceBuilder<T> emit(T elem, long... timestamps) {
		builder.emit(elem, timestamps);
		return this;
	}

	public ProcessingTimeSourceBuilder<T> repeat(T elem, long start, long interval, int times) {
		builder.repeat(elem, start, interval, times);
		return this;
	}


}
