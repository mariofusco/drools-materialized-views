package org.drools.materialized;

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

import static org.apache.calcite.sql.parser.SqlParser.configBuilder;

public class SqlUtil {
    public static SqlNode parseQuery(String query) {
        try {
            return SqlParser.create(query).parseQuery();
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
    }

    public static SqlSelect toSqlSelect(SqlNode sqlNode) {
        if (sqlNode instanceof SqlSelect) {
            return (SqlSelect) sqlNode;
        }
        if (sqlNode instanceof SqlOrderBy) {
            return toSqlSelect( ((SqlOrderBy) sqlNode).query );
        }
        throw new UnsupportedOperationException("Unknown node: " + sqlNode);
    }

    public static RelRoot toAlgebra(String query) {
        Map<String, Map<String, Class<?>>> schemaDefinition = new HashMap<>();

        Map<String, Class<?>> customers = new HashMap<>();
        customers.put("id", String.class);
        customers.put("first_name", String.class);
        customers.put("last_name", String.class);
        schemaDefinition.put("customers", customers);

        Map<String, Class<?>> addresses = new HashMap<>();
        addresses.put("id", String.class);
        addresses.put("customer_id", String.class);
        addresses.put("street", String.class);
        schemaDefinition.put("addresses", addresses);

        Schema schema = new ProgrammaticSchema(schemaDefinition);

        try {
            SchemaPlus schemaPlus = CalciteSchema.createRootSchema(false, true, "", schema).plus();
            FrameworkConfig config = Frameworks.newConfigBuilder()
                    .defaultSchema(schemaPlus)
                    .parserConfig(configBuilder().setCaseSensitive(false).build())
                    .build();
            config.getParserConfig().caseSensitive();
            Planner planner = Frameworks.getPlanner(config);
            SqlNode parsed = planner.parse(query);
            planner.validate(parsed);
            return planner.rel(parsed);
        } catch (SqlParseException | ValidationException | RelConversionException e) {
            throw new RuntimeException(e);
        }
    }
}
