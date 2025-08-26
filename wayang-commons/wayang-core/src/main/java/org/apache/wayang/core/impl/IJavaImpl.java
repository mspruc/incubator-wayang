package org.apache.wayang.core.impl;

import java.util.function.Function;

import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;

/**
 * Wraps the implementation of a Java function.
 * <p>
 * is used to define custom a descriptor via inheritance.
 * <p>
 * See {@link IDescriptor}, {@link ISqlImpl}
 */
@FunctionalInterface
public interface IJavaImpl<T> extends IDescriptor {

    /**
     * Converts a given {@link Function} to a {@link SerializableFunction} and wraps it with {@link IJavaImpl}
     * @param <I> input type
     * @param <O> output type
     * @param func function you want wrapped
     */
    public static <I, O> IJavaImpl<SerializableFunction<I, O>> of(final Function<I, O> func) {
        return () -> func::apply;
    }

    public T getImpl();
}
