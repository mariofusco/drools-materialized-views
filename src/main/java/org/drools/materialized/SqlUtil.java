package org.drools.materialized;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

public class SqlUtil {
    public static SqlNode parseQuery(String query) {
        try {
            return SqlParser.create(query).parseQuery();
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
    }
}
