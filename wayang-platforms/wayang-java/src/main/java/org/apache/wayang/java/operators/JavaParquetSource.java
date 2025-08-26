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

package org.apache.wayang.java.operators;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.ParquetSource;
import org.apache.wayang.core.api.exception.WayangException;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.optimizer.costs.LoadProfileEstimators;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;

/**
 * This is execution operator implements the {@link ParquetSource}.
 */
public class JavaParquetSource extends ParquetSource implements JavaExecutionOperator {

    public JavaParquetSource(final String inputUrl, final String[] projection) {
        super(ParquetSource.create(inputUrl, projection));
    }

    /**
     * Copies an instance (exclusive of broadcasts).
     *
     * @param that that should be copied
     */
    public JavaParquetSource(final ParquetSource that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            final ChannelInstance[] inputs,
            final ChannelInstance[] outputs,
            final JavaExecutor javaExecutor,
            final OptimizationContext.OperatorContext operatorContext) {

        assert inputs.length == this.getNumInputs();
        assert outputs.length == this.getNumOutputs();

        final String urlStr = this.getInputUrl();

        try {
            final Configuration conf = new Configuration();

            // Define a projection schema, if any (uses default schema if no projection
            // defined)
            final Schema schema = getSchemaToRead();
            AvroReadSupport.setAvroReadSchema(conf, schema);
            AvroReadSupport.setRequestedProjection(conf, schema);

            final InputFile file = HadoopInputFile.fromPath(new Path(urlStr), conf);

            // Parse dates as logical types
            final GenericData model = new GenericData();
            model.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
            model.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
            model.addLogicalTypeConversion(new TimeConversions.DateConversion());
            model.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
            model.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());

            final ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(file)
                    .withDataModel(model)
                    .build();

            final List<Record> records = new ArrayList<>();
            GenericRecord rec;

            while ((rec = reader.read()) != null) {
                records.add(convertGenericRecordToRecord(rec));
            }

            ((StreamChannel.Instance) outputs[0]).accept(records);

        } catch (final Exception e) {
            throw new WayangException(String.format("Reading from Parquet file %s failed.", urlStr), e);
        }

        final ExecutionLineageNode prepareLineageNode = new ExecutionLineageNode(operatorContext);
        prepareLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.java.parquetsource.load.prepare", javaExecutor.getConfiguration()));
        final ExecutionLineageNode mainLineageNode = new ExecutionLineageNode(operatorContext);
        mainLineageNode.add(LoadProfileEstimators.createFromSpecification(
                "wayang.java.parquetsource.load.main", javaExecutor.getConfiguration()));

        outputs[0].getLineage().addPredecessor(mainLineageNode);

        return prepareLineageNode.collectAndMark();
    }

    @Override
    public Collection<String> getLoadProfileEstimatorConfigurationKeys() {
        return Arrays.asList("wayang.java.parquetsource.load.prepare", "wayang.java.parquetsource.load.main");
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(final int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(final int index) {
        assert index <= this.getNumOutputs() || (index == 0 && this.getNumOutputs() == 0);
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

    private Schema getSchemaToRead() {
        final String[] projection = this.getProjection();
        final Schema avroSchema = new AvroSchemaConverter().convert(this.getSchema());

        if (projection == null || projection.length == 0) {
            return avroSchema;
        }

        final Set<String> projectionSet = Set.of(projection);

        final List<Schema.Field> filteredFields = avroSchema.getFields().stream()
                .filter(field -> projectionSet.contains(field.name()))
                .toList();

        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(avroSchema.getName())
                .namespace(avroSchema.getNamespace())
                .fields();

        for (final Field field : filteredFields) {
            fieldAssembler = fieldAssembler.name(field.name()).type(field.schema()).noDefault();
        }

        return fieldAssembler.endRecord();
    }

    private Record convertGenericRecordToRecord(final GenericRecord rec) {
        final List<Serializable> values = rec.getSchema().getFields().stream()
                .map(field -> rec.get(field.name()))
                .map(Serializable.class::cast)
                .toList();

        return new Record(values);
    }
}
