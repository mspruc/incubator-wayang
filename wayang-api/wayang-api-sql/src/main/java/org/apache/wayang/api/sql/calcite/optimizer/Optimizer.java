/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.api.sql.calcite.optimizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCostImpl;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.sql2rel.StandardConvertletTable;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.wayang.api.sql.calcite.converter.WayangRelConverter;
import org.apache.wayang.api.sql.calcite.schema.WayangSchema;
import org.apache.wayang.basic.data.Record;
import org.apache.wayang.basic.operators.LocalCallbackSink;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.plan.wayangplan.Operator;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;

import com.google.common.collect.ImmutableList;

public class Optimizer {

    private final CalciteConnectionConfig config;
    private final SqlValidator sqlValidator;
    private final SqlToRelConverter sqlToRelConverter;
    private final VolcanoPlanner volcanoPlanner;

    public Optimizer(
            final CalciteConnectionConfig config,
            final SqlValidator sqlValidator,
            final SqlToRelConverter sqlToRelConverter,
            final VolcanoPlanner volcanoPlanner) {
        this.config = config;
        this.sqlValidator = sqlValidator;
        this.sqlToRelConverter = sqlToRelConverter;
        this.volcanoPlanner = volcanoPlanner;
    }

    public static Optimizer create(
            final CalciteSchema calciteSchema,
            final Properties configProperties,
            final RelDataTypeFactory typeFactory) {

        final CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);

        final CalciteCatalogReader catalogReader = new CalciteCatalogReader(
                calciteSchema.root(),
                ImmutableList.of(calciteSchema.name),
                typeFactory,
                config);

        final SqlOperatorTable operatorTable = SqlOperatorTables.chain(
                ImmutableList.of(SqlStdOperatorTable.instance()));

        final SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(config.lenientOperatorLookup())
                .withConformance(config.conformance())
                .withDefaultNullCollation(config.defaultNullCollation())
                .withIdentifierExpansion(true);

        final SqlValidator validator = SqlValidatorUtil.newValidator(operatorTable, catalogReader, typeFactory,
                validatorConfig);

        final VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(config));
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        final RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        final SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(false);

        final SqlToRelConverter converter = new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig);

        return new Optimizer(config, validator, converter, planner);
    }

    // To remove
    public static Optimizer create(final WayangSchema wayangSchema) {
        final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

        // Configuration
        final Properties configProperties = new Properties();
        configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
        configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
        configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());

        final CalciteConnectionConfig config = new CalciteConnectionConfigImpl(configProperties);

        final CalciteSchema rootSchema = CalciteSchema.createRootSchema(false, false);
        rootSchema.add(wayangSchema.getSchemaName(), wayangSchema);
        final Prepare.CatalogReader catalogReader = new CalciteCatalogReader(
                rootSchema,
                Collections.singletonList(wayangSchema.getSchemaName()),
                typeFactory,
                config);

        final SqlOperatorTable operatorTable = SqlOperatorTables.chain(
                ImmutableList.of(SqlStdOperatorTable.instance()));

        final SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withLenientOperatorLookup(config.lenientOperatorLookup())
                .withConformance(config.conformance())
                .withDefaultNullCollation(config.defaultNullCollation())
                .withIdentifierExpansion(true);

        final SqlValidator validator = SqlValidatorUtil.newValidator(operatorTable, catalogReader, typeFactory,
                validatorConfig);

        final VolcanoPlanner planner = new VolcanoPlanner(RelOptCostImpl.FACTORY, Contexts.of(config));
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        final RelOptCluster cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory));

        final SqlToRelConverter.Config converterConfig = SqlToRelConverter.config()
                .withTrimUnusedFields(true)
                .withExpand(false);

        final SqlToRelConverter converter = new SqlToRelConverter(
                null,
                validator,
                catalogReader,
                cluster,
                StandardConvertletTable.INSTANCE,
                converterConfig);

        return new Optimizer(config, validator, converter, planner);
    }

    public SqlNode parseSql(final String sql) throws SqlParseException {
        final SqlParser.Config parserConfig = SqlParser.config()
                .withCaseSensitive(config.caseSensitive())
                .withQuotedCasing(config.quotedCasing())
                .withUnquotedCasing(config.unquotedCasing())
                .withConformance(config.conformance());

        final SqlParser parser = SqlParser.create(sql, parserConfig);

        return parser.parseStmt();
    }

    public SqlNode validate(final SqlNode sqlNode) {
        return sqlValidator.validate(sqlNode);
    }

    public RelNode convert(final SqlNode sqlNode) {
        final RelRoot root = sqlToRelConverter.convertQuery(sqlNode, false, true);
        return root.rel;
    }

    public RelNode optimize(final RelNode node, final RelTraitSet requiredTraitSet, final RuleSet rules) {
        final Program program = Programs.of(RuleSets.ofList(rules));

        return program.run(
                volcanoPlanner,
                node,
                requiredTraitSet,
                Collections.emptyList(),
                Collections.emptyList());
    }

    public static WayangPlan convert(final RelNode relNode) {
        return convert(relNode, new ArrayList<>());
    }

    public static WayangPlan convert(final RelNode relNode, final Collection<Record> collector) {

        final LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(collector, Record.class);

        final Operator op = new WayangRelConverter().convert(relNode);

        op.connectTo(0, sink, 0);
        return new WayangPlan(sink);
    }

    public static WayangPlan convertWithConfig(final RelNode relNode, final Configuration configuration,
            final Collection<Record> collector) {
        final LocalCallbackSink<Record> sink = LocalCallbackSink.createCollectingSink(collector, Record.class);

        final Operator op = new WayangRelConverter(configuration).convert(relNode);

        op.connectTo(0, sink, 0);
        return new WayangPlan(sink);
    }

    public static class ConfigProperties {

        public static Properties getDefaults() {
            final Properties configProperties = new Properties();
            configProperties.put(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), Boolean.TRUE.toString());
            configProperties.put(CalciteConnectionProperty.UNQUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
            configProperties.put(CalciteConnectionProperty.QUOTED_CASING.camelName(), Casing.UNCHANGED.toString());
            return configProperties;
        }

    }

}
