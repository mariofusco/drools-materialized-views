package org.drools.materialized;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;

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
        SqlNode parsed = parseQuery(query);
        try {
            Planner planner = Frameworks.getPlanner(Frameworks.newConfigBuilder().defaultSchema(Frameworks.createRootSchema(false)).build());
            planner.validate(parsed);
            return planner.rel(parsed);
        } catch (ValidationException | RelConversionException e) {
            throw new RuntimeException(e);
        }
    }
}
