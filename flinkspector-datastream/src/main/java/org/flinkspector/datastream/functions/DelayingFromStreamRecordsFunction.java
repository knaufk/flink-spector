package org.flinkspector.datastream.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.*;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by kknauf on 18.02.16.
 */
public class DelayingFromStreamRecordsFunction<T> extends RichSourceFunction<T> {

    private static final long serialVersionUID = 1L;

    /** The (de)serializer to be used for the data elements */
    private final TypeSerializer<StreamRecord<T>> serializer;

    /** The actual data elements, in serialized form */
    private final byte[] elementsSerialized;

    /** The number of serialized elements */
    private final int numElements;

    /** The number of elements emitted already */
    private volatile int numElementsEmitted;


    /** Flag to make the source cancelable */
    private volatile boolean isRunning = true;


    public DelayingFromStreamRecordsFunction(TypeSerializer<StreamRecord<T>> serializer, Iterable<StreamRecord<T>> elements) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(new DataOutputStream(baos));

        List<StreamRecord<T>> elementList = Lists.newArrayList(elements);

        Collections.sort(elementList, new Comparator<StreamRecord<T>>() {

            @Override
            public int compare(StreamRecord<T> o1, StreamRecord<T> o2) {
                if (o2.getTimestamp() == o1.getTimestamp()) {
                    return 0;
                } else {
                    return o2.getTimestamp() > o1.getTimestamp() ? -1 : 1;
                }
            }
        });

        int count = 0;
        try {
            for (StreamRecord<T> element : elementList) {
                serializer.serialize(element, wrapper);
                count++;
            }
        }
        catch (Exception e) {
            throw new IOException("Serializing the source elements failed: " + e.getMessage(), e);
        }

        this.serializer = serializer;
        this.elementsSerialized = baos.toByteArray();
        this.numElements = count;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(elementsSerialized);
        final DataInputView input = new DataInputViewStreamWrapper(new DataInputStream(bais));

        long lastTimestamp = 0;

        final Object lock = ctx.getCheckpointLock();

        while (isRunning && numElementsEmitted < numElements) {
            StreamRecord<T> next;
            try {
                next = serializer.deserialize(input);
            }
            catch (Exception e) {
                throw new IOException("Failed to deserialize an element from the source. " +
                        "If you are using user-defined serialization (Value and Writable types), check the " +
                        "serialization functions.\nSerializer is " + serializer);
            }


            Thread.sleep(next.getTimestamp() - lastTimestamp);


            lastTimestamp = next.getTimestamp();
            synchronized (lock) {
                ctx.collect(next.getValue());
                numElementsEmitted++;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }


    /**
     * Gets the number of elements produced in total by this function.
     *
     * @return The number of elements produced in total.
     */
    public int getNumElements() {
        return numElements;
    }

    /**
     * Gets the number of elements emitted so far.
     *
     * @return The number of elements emitted so far.
     */
    public int getNumElementsEmitted() {
        return numElementsEmitted;
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Verifies that all elements in the collection are non-null, and are of the given class, or
     * a subclass thereof.
     *
     * @param elements The collection to check.
     * @param viewedAs The class to which the elements must be assignable to.
     *
     * @param <OUT> The generic type of the collection to be checked.
     */
    public static <OUT> void checkCollection(Collection<OUT> elements, Class<OUT> viewedAs) {
        for (OUT elem : elements) {
            if (elem == null) {
                throw new IllegalArgumentException("The collection contains a null element");
            }

            if (!viewedAs.isAssignableFrom(elem.getClass())) {
                throw new IllegalArgumentException("The elements in the collection are not all subclasses of " +
                        viewedAs.getCanonicalName());
            }
        }
    }
}
