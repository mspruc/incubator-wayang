/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.core.function;

import org.apache.wayang.core.optimizer.ProbabilisticDoubleInterval;
import org.apache.wayang.core.optimizer.costs.LoadEstimator;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimator;
import org.apache.wayang.core.optimizer.costs.NestableLoadProfileEstimator;

import java.io.Serializable;
import java.util.Optional;
import java.util.function.*;

/**
 * A function operates on single data units or collections of those.
 */
public abstract class FunctionDescriptor implements Serializable {

    public FunctionDescriptor() {
    }

    private LoadProfileEstimator loadProfileEstimator;

    public FunctionDescriptor(LoadProfileEstimator loadProfileEstimator) {
        this.setLoadProfileEstimator(loadProfileEstimator);
    }

    public void setLoadProfileEstimator(LoadProfileEstimator loadProfileEstimator) {
        this.loadProfileEstimator = loadProfileEstimator;
    }

    public Optional<LoadProfileEstimator> getLoadProfileEstimator() {
        return Optional.ofNullable(this.loadProfileEstimator);
    }

    /**
     * Utility method to retrieve the selectivity of a {@link FunctionDescriptor}
     *
     * @param functionDescriptor either a {@link PredicateDescriptor}, a
     *                           {@link FlatMapDescriptor}, or a
     *                           {@link MapPartitionsDescriptor}
     * @return the selectivity
     */
    public static Optional<ProbabilisticDoubleInterval> getSelectivity(FunctionDescriptor functionDescriptor) {
        if (functionDescriptor == null)
            throw new NullPointerException();
        if (functionDescriptor instanceof PredicateDescriptor) {
            return ((PredicateDescriptor<?>) functionDescriptor).getSelectivity();
        }
        if (functionDescriptor instanceof FlatMapDescriptor) {
            return ((FlatMapDescriptor<?, ?>) functionDescriptor).getSelectivity();
        }
        if (functionDescriptor instanceof MapPartitionsDescriptor) {
            return ((MapPartitionsDescriptor<?, ?>) functionDescriptor).getSelectivity();
        }
        throw new IllegalArgumentException(String.format("Cannot retrieve selectivity of %s.", functionDescriptor));
    }

    /**
     * Updates the {@link LoadProfileEstimator} of this instance.
     *
     * @param cpuEstimator the {@link LoadEstimator} for the CPU load
     * @param ramEstimator the {@link LoadEstimator} for the RAM load
     * @deprecated Use {@link #setLoadProfileEstimator(LoadProfileEstimator)}
     *             instead.
     */
    public void setLoadEstimators(LoadEstimator cpuEstimator, LoadEstimator ramEstimator) {
        this.setLoadProfileEstimator(new NestableLoadProfileEstimator(
                cpuEstimator,
                ramEstimator));
    }

    /**
     * Decorates the default {@link Function} with {@link Serializable}, which is
     * required by some distributed frameworks.
     * 
     * @deprecated Use
     *             {@link org.apache.wayang.core.function.SerializableFunctionInterface.SerializableFunction}
     *             instead.
     */
    @FunctionalInterface
    @Deprecated(forRemoval = true)
    public interface SerializableFunction<Input, Output> extends Function<Input, Output>, Serializable {
    }

    /**
     * Decorates the default {@link BiFunction} with {@link Serializable}, which is
     * required by some distributed frameworks.
     * 
     * @deprecated Use
     *             {@link org.apache.wayang.core.function.SerializableFunctionInterface.SerializableBiFunction}
     *             instead.
     */
    @FunctionalInterface
    @Deprecated(forRemoval = true)
    public interface SerializableBiFunction<Input0, Input1, Output>
            extends BiFunction<Input0, Input1, Output>, Serializable {
    }

    /**
     * Extends a {@link SerializableFunction} to an {@link ExtendedFunction}.
     * 
     * @deprecated Use
     *             {@link org.apache.wayang.core.function.SerializableFunctionInterface.ExtendedSerializableFunction}
     *             instead.
     */
    @Deprecated(forRemoval = true)
    public interface ExtendedSerializableFunction<Input, Output>
            extends SerializableFunction<Input, Output>, ExtendedFunction {
    }

    /**
     * Decorates the default {@link BinaryOperator} with {@link Serializable}, which
     * is required by some distributed frameworks.
     * 
     * @deprecated Use
     *             {@link org.apache.wayang.core.function.SerializableFunctionInterface.SerializableBinaryOperator}
     *             instead.
     */
    @FunctionalInterface
    @Deprecated(forRemoval = true)
    public interface SerializableBinaryOperator<Type> extends BinaryOperator<Type>, Serializable {
    }

    /**
     * Extends a {@link SerializableBinaryOperator} to an {@link ExtendedFunction}.
     * 
     * @deprecated Use
     *             {@link org.apache.wayang.core.function.SerializableFunctionInterface.ExtendedSerializableBinaryOperator}
     *             instead.
     */
    @Deprecated(forRemoval = true)
    public interface ExtendedSerializableBinaryOperator<Type>
            extends SerializableBinaryOperator<Type>, ExtendedFunction {
    }

    /**
     * @deprecated Use
     *             {@link org.apache.wayang.core.function.SerializableFunctionInterface.SerializablePredicate}
     *             instead.
     */
    @FunctionalInterface
    @Deprecated(forRemoval = true)
    public interface SerializablePredicate<T> extends Predicate<T>, Serializable {
    }

    /**
     * @deprecated Use
     *             {@link org.apache.wayang.core.function.SerializableFunctionInterface.ExtendedSerializablePredicate}
     *             instead.
     */
    @Deprecated(forRemoval = true)
    public interface ExtendedSerializablePredicate<T> extends SerializablePredicate<T>, ExtendedFunction {
    }

    /**
     * Decorates the default {@link Consumer} with {@link Serializable}, which is
     * required by some distributed frameworks.
     * 
     * @deprecated Use
     *             {@link org.apache.wayang.core.function.SerializableFunctionInterface.SerializableConsumer}
     *             instead.
     */
    @FunctionalInterface
    @Deprecated(forRemoval = true)
    public interface SerializableConsumer<T> extends Consumer<T>, Serializable {
    }

    /**
     * Extends a {@link SerializableConsumer} to an {@link ExtendedFunction}.
     * 
     * @deprecated Use
     *             {@link org.apache.wayang.core.function.SerializableFunctionInterface.ExtendedSerializableConsumer}
     *             instead.
     */
    @Deprecated(forRemoval = true)
    public interface ExtendedSerializableConsumer<T> extends SerializableConsumer<T>, ExtendedFunction {
    }

    /**
     * @deprecated Use
     *             {@link org.apache.wayang.core.function.SerializableFunctionInterface.SerializableIntUnaryOperator}
     *             instead.
     */
    @FunctionalInterface
    @Deprecated(forRemoval = true)
    public interface SerializableIntUnaryOperator extends IntUnaryOperator, Serializable {
    }

    /**
     * @deprecated Use
     *             {@link org.apache.wayang.core.function.SerializableFunctionInterface.SerializableLongUnaryOperator}
     *             instead.
     */
    @FunctionalInterface
    @Deprecated(forRemoval = true)
    public interface SerializableLongUnaryOperator extends LongUnaryOperator, Serializable {
    }

    /**
     * @deprecated Use
     *             {@link org.apache.wayang.core.function.SerializableFunctionInterface.SerializableToLongBiFunction}
     *             instead.
     */
    @FunctionalInterface
    @Deprecated(forRemoval = true)
    public interface SerializableToLongBiFunction<T, U> extends ToLongBiFunction<T, U>, Serializable {
    }

    /**
     * @deprecated Use
     *             {@link org.apache.wayang.core.function.SerializableFunctionInterface.SerializableToDoubleBiFunction}
     *             instead.
     */
    @FunctionalInterface
    @Deprecated(forRemoval = true)
    public interface SerializableToDoubleBiFunction<T, U> extends ToDoubleBiFunction<T, U>, Serializable {
    }

    /**
     * @deprecated Use
     *             {@link org.apache.wayang.core.function.SerializableFunctionInterface.SerializableToLongFunction}
     *             instead.
     */
    @FunctionalInterface
    @Deprecated(forRemoval = true)
    public interface SerializableToLongFunction<T> extends ToLongFunction<T>, Serializable {
    }
}
