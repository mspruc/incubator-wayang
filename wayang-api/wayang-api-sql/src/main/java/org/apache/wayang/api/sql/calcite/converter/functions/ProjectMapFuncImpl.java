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

package org.apache.wayang.api.sql.calcite.converter.functions;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import org.apache.wayang.core.function.FunctionDescriptor;
import org.apache.wayang.core.function.FunctionDescriptor.SerializableFunction;
import org.apache.wayang.basic.data.Record;

public class ProjectMapFuncImpl implements
        FunctionDescriptor.SerializableFunction<Record, Record> {
    final List<SerializableFunction<Record, Object>> projections;

    public ProjectMapFuncImpl(final List<RexNode> projects) {
        this.projections = projects.stream().map(exp -> {
            if (exp instanceof RexInputRef) {
                final int key = ((RexInputRef) exp).getIndex();
                return (SerializableFunction<Record, Object>) record -> record.getField(key);
            } else if (exp instanceof RexLiteral) {
                final Object literalValue = ((RexLiteral) exp).getValue();
                return (SerializableFunction<Record, Object>) record -> literalValue;
            } else if (exp instanceof RexCall) {
                return (SerializableFunction<Record, Object>) record -> evaluateRexCall(record, (RexCall) exp);
            } else {
                throw new UnsupportedOperationException("Could not resolve record for exp: " + exp);
            }
        }).collect(Collectors.toList());
    }

    public Object evaluateRexCall(final Record record, final RexCall rexCall) {
        if (rexCall == null) {
            return null;
        }

        // Get the operator and operands
        final SqlOperator operator = rexCall.getOperator();
        final List<RexNode> operands = rexCall.getOperands();

        if (operator == SqlStdOperatorTable.PLUS) {
            // Handle addition
            return evaluateNaryOperation(record, operands, (a, b) -> a + b);
        } else if (operator == SqlStdOperatorTable.MINUS) {
            // Handle subtraction
            return evaluateNaryOperation(record, operands, (a, b) -> a - b);
        } else if (operator == SqlStdOperatorTable.MULTIPLY) {
            // Handle multiplication
            return evaluateNaryOperation(record, operands, (a, b) -> a * b);
        } else if (operator == SqlStdOperatorTable.DIVIDE) {
            // Handle division
            return evaluateNaryOperation(record, operands, (a, b) -> a / b);
        } else {
            return null;
        }
    }

    public Object evaluateNaryOperation(final Record record, final List<RexNode> operands,
            final BinaryOperator<Double> operation) {
        if (operands.isEmpty()) {
            return null;
        }

        final List<Double> values = new ArrayList<>();

        for (int i = 0; i < operands.size(); i++) {
            final Number val = (Number) evaluateRexNode(record, operands.get(i));
            if (val == null) {
                return null;
            }
            values.add(val.doubleValue());
        }

        Object result = values.get(0);
        // Perform the operation with the remaining operands
        for (int i = 1; i < operands.size(); i++) {
            result = operation.apply((double) result, values.get(i));
        }

        return result;
    }

    public Object evaluateRexNode(final Record record, final RexNode rexNode) {
        if (rexNode instanceof RexCall) {
            // Recursively evaluate a RexCall
            return evaluateRexCall(record, (RexCall) rexNode);
        } else if (rexNode instanceof RexLiteral) {
            // Handle literals (e.g., numbers)
            final RexLiteral literal = (RexLiteral) rexNode;
            return literal.getValue();
        } else if (rexNode instanceof RexInputRef) {
            return record.getField(((RexInputRef) rexNode).getIndex());
        } else {
            return null; // Unsupported or unknown expression
        }
    }

    @Override
    public Record apply(final Record record) {
        return new Record(projections.stream().map(func -> func.apply(record)).toArray());
    }
}
