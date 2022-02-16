package org.drools.materialized;

import org.junit.Test;
import org.junit.runners.Parameterized;
import org.kie.api.runtime.rule.Row;
import org.kie.api.runtime.rule.ViewChangedEventListener;

import static org.junit.Assert.assertEquals;

public class MaterializedViewsTest {

    private static final String[] ADDRESSES = new String[]{
        "{\"before\":null,\"after\":{\"id\":100001,\"customer_id\":1001,\"street\":\"42 Main Street\",\"city\":\"Hamburg\",\"zipcode\":\"90210\",\"country\":\"Canada\"},\"source\":{\"version\":\"1.8.0.Alpha2\",\"connector\":\"postgresql\",\"name\":\"dbserver1\",\"ts_ms\":1643708392710,\"snapshot\":\"true\",\"db\":\"postgres\",\"sequence\":\"[null,\\\"36183464\\\"]\",\"schema\":\"inventory\",\"table\":\"addresses\",\"txId\":765,\"lsn\":36183464,\"xmin\":null},\"op\":\"r\",\"ts_ms\":1643708392715,\"transaction\":null}",
        "{\"before\":null,\"after\":{\"id\":100002,\"customer_id\":1001,\"street\":\"11 Post Dr.\",\"city\":\"Berlin\",\"zipcode\":\"90211\",\"country\":\"Canada\"},\"source\":{\"version\":\"1.8.0.Alpha2\",\"connector\":\"postgresql\",\"name\":\"dbserver1\",\"ts_ms\":1643708392745,\"snapshot\":\"true\",\"db\":\"postgres\",\"sequence\":\"[null,\\\"36183464\\\"]\",\"schema\":\"inventory\",\"table\":\"addresses\",\"txId\":765,\"lsn\":36183464,\"xmin\":null},\"op\":\"r\",\"ts_ms\":1643708392745,\"transaction\":null}",
        "{\"before\":null,\"after\":{\"id\":100003,\"customer_id\":1002,\"street\":\"12 Rodeo Dr.\",\"city\":\"Los Angeles\",\"zipcode\":\"90212\",\"country\":\"US\"},\"source\":{\"version\":\"1.8.0.Alpha2\",\"connector\":\"postgresql\",\"name\":\"dbserver1\",\"ts_ms\":1643708392747,\"snapshot\":\"true\",\"db\":\"postgres\",\"sequence\":\"[null,\\\"36183464\\\"]\",\"schema\":\"inventory\",\"table\":\"addresses\",\"txId\":765,\"lsn\":36183464,\"xmin\":null},\"op\":\"r\",\"ts_ms\":1643708392747,\"transaction\":null}",
        "{\"before\":null,\"after\":{\"id\":100004,\"customer_id\":1002,\"street\":\"1 Debezium Plaza\",\"city\":\"Monterey\",\"zipcode\":\"90213\",\"country\":\"US\"},\"source\":{\"version\":\"1.8.0.Alpha2\",\"connector\":\"postgresql\",\"name\":\"dbserver1\",\"ts_ms\":1643708392747,\"snapshot\":\"true\",\"db\":\"postgres\",\"sequence\":\"[null,\\\"36183464\\\"]\",\"schema\":\"inventory\",\"table\":\"addresses\",\"txId\":765,\"lsn\":36183464,\"xmin\":null},\"op\":\"r\",\"ts_ms\":1643708392747,\"transaction\":null}",
        "{\"before\":null,\"after\":{\"id\":100005,\"customer_id\":1002,\"street\":\"2 Debezium Plaza\",\"city\":\"Monterey\",\"zipcode\":\"90213\",\"country\":\"US\"},\"source\":{\"version\":\"1.8.0.Alpha2\",\"connector\":\"postgresql\",\"name\":\"dbserver1\",\"ts_ms\":1643708392747,\"snapshot\":\"true\",\"db\":\"postgres\",\"sequence\":\"[null,\\\"36183464\\\"]\",\"schema\":\"inventory\",\"table\":\"addresses\",\"txId\":765,\"lsn\":36183464,\"xmin\":null},\"op\":\"r\",\"ts_ms\":1643708392747,\"transaction\":null}",
        "{\"before\":{\"id\":100003,\"customer_id\":1002,\"street\":\"12 Rodeo Dr.\",\"city\":\"Los Angeles\",\"zipcode\":\"90212\",\"country\":\"US\"},\"after\":null,\"source\":{\"version\":\"1.8.0.Alpha2\",\"connector\":\"postgresql\",\"name\":\"dbserver1\",\"ts_ms\":1643709078615,\"snapshot\":\"false\",\"db\":\"postgres\",\"sequence\":\"[\\\"36559760\\\",\\\"36560472\\\"]\",\"schema\":\"inventory\",\"table\":\"addresses\",\"txId\":773,\"lsn\":36560472,\"xmin\":null},\"op\":\"d\",\"ts_ms\":1643709078691,\"transaction\":null}",
        "{\"before\":{\"id\":100004,\"customer_id\":1002,\"street\":\"1 Debezium Plaza\",\"city\":\"Monterey\",\"zipcode\":\"90213\",\"country\":\"US\"},\"after\":null,\"source\":{\"version\":\"1.8.0.Alpha2\",\"connector\":\"postgresql\",\"name\":\"dbserver1\",\"ts_ms\":1643709078615,\"snapshot\":\"false\",\"db\":\"postgres\",\"sequence\":\"[\\\"36559760\\\",\\\"36560576\\\"]\",\"schema\":\"inventory\",\"table\":\"addresses\",\"txId\":773,\"lsn\":36560576,\"xmin\":null},\"op\":\"d\",\"ts_ms\":1643709078691,\"transaction\":null}",
        "{\"before\":{\"id\":100005,\"customer_id\":1002,\"street\":\"2 Debezium Plaza\",\"city\":\"Monterey\",\"zipcode\":\"90213\",\"country\":\"US\"},\"after\":null,\"source\":{\"version\":\"1.8.0.Alpha2\",\"connector\":\"postgresql\",\"name\":\"dbserver1\",\"ts_ms\":1643709078615,\"snapshot\":\"false\",\"db\":\"postgres\",\"sequence\":\"[\\\"36559760\\\",\\\"36560680\\\"]\",\"schema\":\"inventory\",\"table\":\"addresses\",\"txId\":773,\"lsn\":36560680,\"xmin\":null},\"op\":\"d\",\"ts_ms\":1643709078692,\"transaction\":null}"
    };

    private static final String[] CUSTOMERS = new String[]{
        "{\"before\":null,\"after\":{\"id\":1001,\"first_name\":\"Sally\",\"last_name\":\"Thomas\",\"email\":\"sally.thomas@acme.com\"},\"source\":{\"version\":\"1.8.0.Alpha2\",\"connector\":\"postgresql\",\"name\":\"dbserver1\",\"ts_ms\":1643708392757,\"snapshot\":\"true\",\"db\":\"postgres\",\"sequence\":\"[null,\\\"36183464\\\"]\",\"schema\":\"inventory\",\"table\":\"customers\",\"txId\":765,\"lsn\":36183464,\"xmin\":null},\"op\":\"r\",\"ts_ms\":1643708392757,\"transaction\":null}",
        "{\"before\":null,\"after\":{\"id\":1002,\"first_name\":\"George\",\"last_name\":\"Bailey\",\"email\":\"gbailey@foobar.com\"},\"source\":{\"version\":\"1.8.0.Alpha2\",\"connector\":\"postgresql\",\"name\":\"dbserver1\",\"ts_ms\":1643708392757,\"snapshot\":\"true\",\"db\":\"postgres\",\"sequence\":\"[null,\\\"36183464\\\"]\",\"schema\":\"inventory\",\"table\":\"customers\",\"txId\":765,\"lsn\":36183464,\"xmin\":null},\"op\":\"r\",\"ts_ms\":1643708392757,\"transaction\":null}",
        "{\"before\":null,\"after\":{\"id\":1003,\"first_name\":\"Edward\",\"last_name\":\"Walker\",\"email\":\"ed@walker.com\"},\"source\":{\"version\":\"1.8.0.Alpha2\",\"connector\":\"postgresql\",\"name\":\"dbserver1\",\"ts_ms\":1643708392757,\"snapshot\":\"true\",\"db\":\"postgres\",\"sequence\":\"[null,\\\"36183464\\\"]\",\"schema\":\"inventory\",\"table\":\"customers\",\"txId\":765,\"lsn\":36183464,\"xmin\":null},\"op\":\"r\",\"ts_ms\":1643708392757,\"transaction\":null}",
        "{\"before\":null,\"after\":{\"id\":1004,\"first_name\":\"Anne\",\"last_name\":\"Kretchmar\",\"email\":\"annek@noanswer.org\"},\"source\":{\"version\":\"1.8.0.Alpha2\",\"connector\":\"postgresql\",\"name\":\"dbserver1\",\"ts_ms\":1643708392758,\"snapshot\":\"true\",\"db\":\"postgres\",\"sequence\":\"[null,\\\"36183464\\\"]\",\"schema\":\"inventory\",\"table\":\"customers\",\"txId\":765,\"lsn\":36183464,\"xmin\":null},\"op\":\"r\",\"ts_ms\":1643708392758,\"transaction\":null}",
        "{\"before\":{\"id\":1001,\"first_name\":\"Sally\",\"last_name\":\"Thomas\",\"email\":\"sally.thomas@acme.com\"},\"after\":{\"id\":1001,\"first_name\":\"Sarah\",\"last_name\":\"Thomas\",\"email\":\"sally.thomas@acme.com\"},\"source\":{\"version\":\"1.8.0.Alpha2\",\"connector\":\"postgresql\",\"name\":\"dbserver1\",\"ts_ms\":1643708483291,\"snapshot\":\"false\",\"db\":\"postgres\",\"sequence\":\"[\\\"36557736\\\",\\\"36557792\\\"]\",\"schema\":\"inventory\",\"table\":\"customers\",\"txId\":769,\"lsn\":36557792,\"xmin\":null},\"op\":\"u\",\"ts_ms\":1643708483355,\"transaction\":null}",
        "{\"before\":{\"id\":1002,\"first_name\":\"George\",\"last_name\":\"Bailey\",\"email\":\"gbailey@foobar.com\"},\"after\":null,\"source\":{\"version\":\"1.8.0.Alpha2\",\"connector\":\"postgresql\",\"name\":\"dbserver1\",\"ts_ms\":1643709082371,\"snapshot\":\"false\",\"db\":\"postgres\",\"sequence\":\"[\\\"36560832\\\",\\\"36560832\\\"]\",\"schema\":\"inventory\",\"table\":\"customers\",\"txId\":774,\"lsn\":36560832,\"xmin\":null},\"op\":\"d\",\"ts_ms\":1643709082457,\"transaction\":null}"
    };

    private static final String QUERY_1 =
            "SELECT\n" +
            "  customer_id, count(*)\n" +
            "FROM\n" +
            "  addresses\n" +
            "GROUP BY\n" +
            "  customer_id\n" +
            "ORDER BY\n" +
            "  count(*) DESC";

    private static final String QUERY_2 =
            "SELECT\n" +
            "  *\n" +
            "FROM\n" +
            "  customers c LEFT JOIN addresses a on c.id = a.customer_id";

    @Test
    public void testJoinWithPrototype() {
        testJoin(true);
    }

    @Test
    public void testJoinWithoutPrototype() {
        testJoin(false);
    }

    public void testJoin(boolean usePrototype) {
        QueryExecutor queryExecutor = QueryExecutor.create(QUERY_2, usePrototype);

        MaterializedViewChangedEventListener listener = new MaterializedViewChangedEventListener();
        queryExecutor.listen(listener);

        for (int i = 0; i < Math.max(ADDRESSES.length, CUSTOMERS.length); i++) {
            if (i < ADDRESSES.length) {
                queryExecutor.process(ADDRESSES[i]);
            }
            if (i < CUSTOMERS.length) {
                queryExecutor.process(CUSTOMERS[i]);
            }
            System.out.println("*** " + i);
        }

        assertEquals(5, listener.inserts);
        assertEquals(2, listener.updates);
        assertEquals(3, listener.deletes);
    }

    private static class MaterializedViewChangedEventListener implements ViewChangedEventListener {

        int inserts = 0;
        int updates = 0;
        int deletes = 0;

        @Override
        public void rowInserted(Row row) {
            inserts++;
            System.out.println("rowInserted: " + row.get("c") + "; " + row.get("a"));
        }

        @Override
        public void rowDeleted(Row row) {
            deletes++;
            System.out.println("rowDeleted: " + row.get("c") + "; " + row.get("a"));
        }

        @Override
        public void rowUpdated(Row row) {
            updates++;
            System.out.println("rowUpdated: " + row.get("c") + "; " + row.get("a"));
        }
    }
}