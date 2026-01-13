package org.apache.wayang.core.function;

import java.io.Serializable;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.function.LongUnaryOperator;
import java.util.function.Predicate;
import java.util.function.ToDoubleBiFunction;
import java.util.function.ToLongBiFunction;
import java.util.function.ToLongFunction;

public interface SerializableFunctionInterface {

    /**
     * Decorates the default {@link Function} with {@link Serializable}, which is required by some distributed frameworks.
     */
    @FunctionalInterface
    public interface SerializableFunction<Input, Output> extends Function<Input, Output>, Serializable {
    }

    /**
     * Decorates the default {@link Function} with {@link Serializable}, which is required by some distributed frameworks.
     */
    @FunctionalInterface
    public interface SerializableBiFunction<Input0, Input1, Output> extends BiFunction<Input0, Input1, Output>, Serializable {
    }


    /**
     * Extends a {@link SerializableFunction} to an {@link ExtendedFunction}.
     */
    public interface ExtendedSerializableFunction<Input, Output> extends SerializableFunction<Input, Output>, ExtendedFunction {
    }

    /**
     * Decorates the default {@link Function} with {@link Serializable}, which is required by some distributed frameworks.
     */
    @FunctionalInterface
    public interface SerializableBinaryOperator<Type> extends BinaryOperator<Type>, Serializable {
    }

    /**
     * Extends a {@link SerializableBinaryOperator} to an {@link ExtendedFunction}.
     */
    public interface ExtendedSerializableBinaryOperator<Type> extends SerializableBinaryOperator<Type>, ExtendedFunction {
    }

    @FunctionalInterface
    public interface SerializablePredicate<T> extends Predicate<T>, Serializable {

    }

    public interface ExtendedSerializablePredicate<T> extends SerializablePredicate<T>, ExtendedFunction {

    }

    /**
     * Decorates the default {@link Consumer} with {@link Serializable}, which is required by some distributed frameworks.
     */
    @FunctionalInterface
    public interface SerializableConsumer<T> extends Consumer<T>, Serializable {

    }
    /**
     * Extends a {@link SerializableConsumer} to an {@link ExtendedFunction}.
     */
    public interface ExtendedSerializableConsumer<T> extends SerializableConsumer<T>, ExtendedFunction{

    }

    @FunctionalInterface
    public interface SerializableIntUnaryOperator extends IntUnaryOperator, Serializable {

    }

    @FunctionalInterface
    public interface SerializableLongUnaryOperator extends LongUnaryOperator, Serializable {

    }

    @FunctionalInterface
    public interface SerializableToLongBiFunction<T, U> extends ToLongBiFunction<T, U>, Serializable {

    }

    @FunctionalInterface
    public interface SerializableToDoubleBiFunction<T, U> extends ToDoubleBiFunction<T, U>, Serializable {

    }

    @FunctionalInterface
    public interface SerializableToLongFunction<T> extends ToLongFunction<T>, Serializable {

    }
}