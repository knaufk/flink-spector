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

import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder to define the input for a test
 * Offers multiple methods to generate input with the EventTime attached.
 *
 * @param <T> value type
 */
public class ProcessingTimeInputBuilder<T> implements ProcessingTimeInput<T> {

	/**
	 * List of input containing StreamRecords
	 */
	private ArrayList<StreamRecord<T>> input = new ArrayList<>();

	private ProcessingTimeInputBuilder(StreamRecord<T> record) {
		input.add(record);
	}

	/**
	 *  Create an {@link ProcessingTimeInputBuilder} with the first record as input.
	 *
	 * @param record value
	 * @param <T>
	 * @return {@link ProcessingTimeInputBuilder}
	 */
	public static <T> ProcessingTimeInputBuilder<T> startWith(T record) {
		if (record == null) {
			throw new IllegalArgumentException("Record has to be not null!");
		}
		return new ProcessingTimeInputBuilder<T>(new StreamRecord<T>(record, 0));
	}

	/**
	 *  Create an {@link ProcessingTimeInputBuilder} with the first record as input.
	 *
	 * @param record value
	 * @param <T>
	 * @return {@link ProcessingTimeInputBuilder}
	 */
	public static <T> ProcessingTimeInputBuilder<T> startWith(T record, long timestamp) {
		if (record == null) {
			throw new IllegalArgumentException("Record has to be not null!");
		}
		return new ProcessingTimeInputBuilder<T>(new StreamRecord<T>(record, timestamp));
	}

	/**
	 * Add an element with timestamp to the input.
	 *
	 * @param record
	 * @param timestamps
	 * @return
	 */
	public ProcessingTimeInputBuilder<T> emit(T record, long... timestamps) {
		if (timestamps.length == 0) {
			throw new IllegalArgumentException("List of timestamps has to be of length > 0");
		}
		if (record == null) {
			throw new IllegalArgumentException("Elem has to be not null!");
		}
		for (long timestamp : timestamps) {
			if (timestamp < 0) {
				throw new IllegalArgumentException("Timestamp can not be negative");
			}
			input.add(new StreamRecord<T>(record, timestamp));
		}
		return this;
	}

	public ProcessingTimeInputBuilder<T> repeat(T record, long start, long interval, int times) {
		long[] timestamps = createEquiDistantTimestamps(start, interval, times);
		return emit(record, timestamps);
	}

	private long[] createEquiDistantTimestamps(long start, long interval, int times) {
		long[] timestamps = new long[times+1];

		long timestamp = start;
		for (int count = 0; count <= times;count++) {
			timestamps[count] = timestamp;
			timestamp=timestamp + interval;
		}
		return timestamps;
	}

	/**
	 * Print the input list.
	 *
	 * @return
	 */
	public String toString() {
		StringBuilder builder = new StringBuilder();
		for (StreamRecord<T> r : input) {
			builder.append("value: " + r.getValue() + " timestamp: " + r.getTimestamp() + "\n");
		}
		return builder.toString();
	}

	@Override
	public List<StreamRecord<T>> getInput() {
		return input;
	}

}
