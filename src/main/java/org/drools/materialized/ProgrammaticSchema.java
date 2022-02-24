package org.drools.materialized;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.JavaToSqlTypeConversionRules;
import org.apache.calcite.sql.type.SqlTypeName;

public class ProgrammaticSchema extends AbstractSchema {
    private static final JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    private final Map<String, Map<String, Class<?>>> schemaDefinition;

    private Map<String, Table> tableMap;

    public ProgrammaticSchema(Map<String, Map<String, Class<?>>> schemaDefinition) {
        this.schemaDefinition = schemaDefinition;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        if (tableMap == null) {
            tableMap = createTableMap();
        }
        return tableMap;
    }

    private Map<String, Table> createTableMap() {
        ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
        for (Map.Entry<String, Map<String, Class<?>>> entry : schemaDefinition.entrySet()) {
            Table table = new ProgrammaticTable(entry.getValue());
            builder.put(entry.getKey(), table);
        }
        return builder.build();
    }

    private static class ProgrammaticTable extends AbstractTable {
        private final RelDataType rowType;

        ProgrammaticTable(Map<String, Class<?>> tableDefinition) {
            this.rowType = createRelDataType(tableDefinition);
        }

        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return rowType;
        }

        private RelDataType createRelDataType(Map<String, Class<?>> tableDefinition) {
            final List<RelDataTypeField> list = new ArrayList<>();
            for (Map.Entry<String, Class<?>> entry : tableDefinition.entrySet()) {
                list.add( new RelDataTypeFieldImpl(entry.getKey(), list.size(), createType(entry.getValue())) );
            }
            return new RelRecordType(list);
        }

        private RelDataType createType(Class clazz) {
            switch (Primitive.flavor(clazz)) {
                case PRIMITIVE:
                    return typeFactory.createJavaType(clazz);
                case BOX:
                    return typeFactory.createJavaType(Primitive.ofBox(clazz).boxClass);
            }
            if (JavaToSqlTypeConversionRules.instance().lookup(clazz) != null) {
                return typeFactory.createJavaType(clazz);
            } else if (clazz.isArray()) {
                return typeFactory.createMultisetType( createType(clazz.getComponentType()), -1 );
            } else if (List.class.isAssignableFrom(clazz)) {
                return typeFactory.createArrayType(
                        typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true), -1);
            } else if (Map.class.isAssignableFrom(clazz)) {
                return typeFactory.createMapType(
                        typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true),
                        typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.ANY), true));
            } else {
                return typeFactory.createStructType(clazz);
            }
        }
    }
}