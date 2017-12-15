package com.septima.sqldrivers;

import com.septima.jdbc.NamedJdbcValue;
import com.septima.metadata.*;
import com.septima.sqldrivers.resolvers.PostgreTypesResolver;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.sql.Wrapper;
import java.util.ArrayList;
import java.util.List;

import com.septima.sqldrivers.resolvers.TypesResolver;
import org.postgis.PGgeometry;

/**
 * @author mg
 */
public class PostgreSqlDriver extends SqlDriver {

    private static final String POSTGRES_DIALECT = "Postgres";
    private static final Character ESCAPE = '"';

    private static final PostgreTypesResolver resolver = new PostgreTypesResolver();
    private static final String SET_SCHEMA_CLAUSE = "set search_path = %s,public";
    private static final String GET_SCHEMA_CLAUSE = "select current_schema()";
    private static final String CREATE_SCHEMA_CLAUSE = "CREATE SCHEMA %s";
    private static final String DEF_OTHER_TYPE_NAME = "point";
    private static final String RENAME_FIELD_SQL_PREFIX = "alter table %s rename column %s to %s";
    private static final String MODIFY_FIELD_SQL_PREFIX = "alter table %s alter ";

    @Override
    public String getDialect() {
        return POSTGRES_DIALECT;
    }

    @Override
    public boolean is(String aJdbcUrl) {
        return aJdbcUrl.contains("jdbc:postgre");
    }

    @Override
    public TypesResolver getTypesResolver() {
        return resolver;
    }

    @Override
    public String getSql4GetSchema() {
        return GET_SCHEMA_CLAUSE;
    }

    @Override
    public String[] getSql4CreateColumnComment(String aOwnerName, String aTableName, String aFieldName, String aDescription) {
        String ownerName = escapeNameIfNeeded(aOwnerName);
        String tableName = escapeNameIfNeeded(aTableName);
        String fieldName = escapeNameIfNeeded(aFieldName);
        String sqlText = aOwnerName == null ? String.join(".", tableName, fieldName) : String.join(".", ownerName, tableName, fieldName);
        if (aDescription == null) {
            aDescription = "";
        }
        return new String[]{String.format("comment on column %s is '%s'", sqlText, aDescription.replaceAll("'", "''"))};
    }

    @Override
    public String getSql4CreateTableComment(String aOwnerName, String aTableName, String aDescription) {
        String sqlText = String.join(".", escapeNameIfNeeded(aOwnerName), escapeNameIfNeeded(aTableName));
        if (aDescription == null) {
            aDescription = "";
        }
        return String.format("comment on table %s is '%s'", sqlText, aDescription.replaceAll("'", "''"));
    }

    @Override
    public String getSql4DropTable(String aSchemaName, String aTableName) {
        return "drop table " + makeFullName(aSchemaName, aTableName) + " cascade";
    }

    @Override
    public String getSql4DropIndex(String aSchemaName, String aTableName, String aIndexName) {
        return "drop index " + makeFullName(aSchemaName, aIndexName);
    }

    @Override
    public String getSql4CreateIndex(String aSchemaName, String aTableName, TableIndex aIndex) {
        assert aIndex.getColumns().size() > 0 : "index definition must consist of at least 1 column";
        String indexName = escapeNameIfNeeded(aIndex.getName());
        String tableName = makeFullName(aSchemaName, aTableName);
        String modifier = "";
        if (aIndex.isUnique()) {
            modifier = "unique";
        }
        String methodClause = "";
        if (aIndex.isHashed()) {
            methodClause = " using hash ";
        }
        String fieldsList = aIndex.getColumns().stream()
                .map(column -> new StringBuilder(
                        escapeNameIfNeeded(column.getColumnName())
                ))
                .reduce((s1, s2) -> new StringBuilder()
                        .append(s1)
                        .append(", ")
                        .append(s2))
                .map(StringBuilder::toString)
                .orElse("");
        return "create " + modifier + " index " + indexName + " on " + tableName + " " + methodClause + " ( " + fieldsList + " )";
    }

    @Override
    public String getSql4DropFkConstraint(String aSchemaName, ForeignKey aFk) {
        String constraintName = escapeNameIfNeeded(aFk.getCName());
        String tableName = makeFullName(aSchemaName, aFk.getTable());
        return "alter table " + tableName + " drop constraint " + constraintName + " cascade";
    }

    @Override
    public String getSql4CreateFkConstraint(String aSchemaName, ForeignKey aFk) {
        List<ForeignKey> fkList = new ArrayList<>();
        fkList.add(aFk);
        return getSql4CreateFkConstraint(aSchemaName, fkList);
    }

    @Override
    public String getSql4EmptyTableCreation(String aSchemaName, String aTableName, String aPkFieldName) {
        String fullName = makeFullName(aSchemaName, aTableName);
        String pkFieldName = escapeNameIfNeeded(aPkFieldName);
        return "CREATE TABLE " + fullName + " ("
                + pkFieldName + " NUMERIC NOT NULL,"
                + "CONSTRAINT " + escapeNameIfNeeded(generatePkName(aTableName, PKEY_NAME_SUFFIX)) + " PRIMARY KEY (" + pkFieldName + "))";
    }

    private String getFieldTypeDefinition(JdbcColumn aField) {
        String typeDefine = "";
        String sqlTypeName = aField.getType().toLowerCase();
        // field length
        int size = aField.getSize();
        int scale = aField.getScale();

        typeDefine += sqlTypeName;
        if (resolver.isScaled(sqlTypeName) && resolver.isSized(sqlTypeName) && size > 0) {
            typeDefine += "(" + String.valueOf(size) + "," + String.valueOf(scale) + ")";
        } else {
            if (resolver.isSized(sqlTypeName) && size > 0) {
                typeDefine += "(" + String.valueOf(size) + ")";
            }
        }
        return typeDefine;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSql4FieldDefinition(JdbcColumn aField) {
        String fieldName = escapeNameIfNeeded(aField.getName());
        String fieldDefinition = fieldName + " " + getFieldTypeDefinition(aField);

        if (!aField.isNullable()) {
            fieldDefinition += " not null";
        } else {
            fieldDefinition += " null";
        }
        return fieldDefinition;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getSqls4ModifyingField(String aSchemaName, String aTableName, JdbcColumn aOldFieldMd, JdbcColumn aNewFieldMd) {
        List<String> sqls = new ArrayList<>();
        String fullTableName = makeFullName(aSchemaName, aTableName);
        String fieldName = escapeNameIfNeeded(aOldFieldMd.getName());
        String updateDefinition = String.format(MODIFY_FIELD_SQL_PREFIX, fullTableName) + fieldName + " ";
        String fieldDefination = getFieldTypeDefinition(aNewFieldMd);

        String newSqlTypeName = aNewFieldMd.getType();
        if (newSqlTypeName == null) {
            newSqlTypeName = "";
        }
        int newScale = aNewFieldMd.getScale();
        int newSize = aNewFieldMd.getSize();
        boolean newNullable = aNewFieldMd.isNullable();

        String oldSqlTypeName = aOldFieldMd.getType();
        if (oldSqlTypeName == null) {
            oldSqlTypeName = "";
        }
        int oldScale = aOldFieldMd.getScale();
        int oldSize = aOldFieldMd.getSize();
        boolean oldNullable = aOldFieldMd.isNullable();

        if (!newSqlTypeName.equalsIgnoreCase(oldSqlTypeName)
                || (resolver.isSized(newSqlTypeName) && newSize != oldSize)
                || (resolver.isScaled(newSqlTypeName) && newScale != oldScale)) {
            sqls.add(updateDefinition + " type " + fieldDefination + " using " + fieldName + "::" + newSqlTypeName);
        }
        if (oldNullable != newNullable) {
            sqls.add(updateDefinition + (newNullable ? " drop not null" : " set not null"));
        }
        return sqls.toArray(new String[sqls.size()]);
    }

    @Override
    public String[] getSqls4RenamingField(String aSchemaName, String aTableName, String aOldFieldName, JdbcColumn aNewFieldMd) {
        return new String[]{
                String.format(RENAME_FIELD_SQL_PREFIX,
                        makeFullName(aSchemaName, aTableName),
                        escapeNameIfNeeded(aOldFieldName),
                        escapeNameIfNeeded(aNewFieldMd.getName()))
        };
    }

    @Override
    public String parseException(Exception ex) {
        return ex.getMessage();
    }

    @Override
    public String getSql4CreateSchema(String aSchemaName, String aPassword) {
        if (aSchemaName != null && !aSchemaName.isEmpty()) {
            return String.format(CREATE_SCHEMA_CLAUSE, aSchemaName);
        }
        throw new IllegalArgumentException("Schema name is null or empty.");
    }

    @Override
    public String getSql4DropPkConstraint(String aSchemaName, PrimaryKey aPk) {
        String constraintName = escapeNameIfNeeded(aPk.getCName());
        String tableFullName = makeFullName(aSchemaName, aPk.getTable());
        return "alter table " + tableFullName + " drop constraint " + constraintName;
    }

    @Override
    public String getSql4CreateFkConstraint(String aSchemaName, List<ForeignKey> listFk) {
        if (listFk != null && listFk.size() > 0) {
            ForeignKey fk = listFk.get(0);
            String fkTableName = makeFullName(aSchemaName, fk.getTable());
            String fkName = fk.getCName();
            String fkColumnName = escapeNameIfNeeded(fk.getField());

            PrimaryKey pk = fk.getReferee();
            String pkSchemaName = pk.getSchema();
            String pkTableName = makeFullName(aSchemaName, pk.getTable());
            String pkColumnName = escapeNameIfNeeded(pk.getField());

            for (int i = 1; i < listFk.size(); i++) {
                fk = listFk.get(i);
                pk = fk.getReferee();
                fkColumnName += ", " + escapeNameIfNeeded(fk.getField());
                pkColumnName += ", " + escapeNameIfNeeded(pk.getField());
            }

            String fkRule = "";
            switch (fk.getUpdateRule()) {
                case CASCADE:
                    fkRule += " ON UPDATE CASCADE ";
                    break;
                case NOACTION:
                    fkRule += " ON UPDATE no action";
                    break;
                case SETDEFAULT:
                    fkRule += " ON UPDATE set default";
                    break;
                case SETNULL:
                    fkRule += " ON UPDATE set null";
                    break;
            }
            switch (fk.getDeleteRule()) {
                case CASCADE:
                    fkRule += " ON DELETE CASCADE ";
                    break;
                case NOACTION:
                    fkRule += " ON DELETE no action ";
                    break;
                case SETDEFAULT:
                    fkRule += " ON DELETE set default ";
                    break;
                case SETNULL:
                    fkRule += " ON DELETE set null ";
                    break;
            }
            if (fk.getDeferrable()) {
                fkRule += " DEFERRABLE INITIALLY DEFERRED";
            }
            return String.format("ALTER TABLE %s ADD CONSTRAINT %s"
                    + " FOREIGN KEY (%s) REFERENCES %s (%s) %s", fkTableName, fkName.isEmpty() ? "" : escapeNameIfNeeded(fkName), fkColumnName, pkTableName, pkColumnName, fkRule);
        }
        return null;
    }

    @Override
    public String[] getSql4CreatePkConstraint(String aSchemaName, List<PrimaryKey> listPk) {
        if (listPk != null && listPk.size() > 0) {
            PrimaryKey pk = listPk.get(0);
            String tableName = pk.getTable();
            String pkTableName = makeFullName(aSchemaName, tableName);
            String pkName = escapeNameIfNeeded(generatePkName(tableName, PKEY_NAME_SUFFIX));
            String pkColumnName = escapeNameIfNeeded(pk.getField());
            for (int i = 1; i < listPk.size(); i++) {
                pk = listPk.get(i);
                pkColumnName += ", " + escapeNameIfNeeded(pk.getField());
            }
            return new String[]{
                    String.format("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s)", pkTableName, pkName, pkColumnName)
            };
        }
        return null;
    }

    @Override
    public boolean isConstraintsDeferrable() {
        return true;
    }

    @Override
    public String[] getSqls4AddingField(String aSchemaName, String aTableName, JdbcColumn aField) {
        String fullTableName = makeFullName(aSchemaName, aTableName);
        return new String[]{
                String.format(SqlDriver.ADD_FIELD_SQL_PREFIX, fullTableName) + getSql4FieldDefinition(aField)
        };
    }

    @Override
    public String[] getSql4DroppingField(String aSchemaName, String aTableName, String aFieldName) {
        String fullTableName = escapeNameIfNeeded(aTableName);
        if (aSchemaName != null && !aSchemaName.isEmpty()) {
            fullTableName = escapeNameIfNeeded(aSchemaName) + "." + fullTableName;
        }
        return new String[]{
                String.format(DROP_FIELD_SQL_PREFIX, fullTableName) + escapeNameIfNeeded(aFieldName) + " cascade"
        };
    }

    @Override
    public Character getEscape() {
        return ESCAPE;
    }

    @Override
    public NamedJdbcValue convertGeometry(String aValue, Connection aConnection) throws SQLException {
        return new NamedJdbcValue(
                null,
                aValue != null ? new PGgeometry(aValue) : null,
                Types.OTHER,
                "geometry");
    }

    @Override
    public String readGeometry(Wrapper aRs, int aColumnIndex, Connection aConnection) throws SQLException {
        Object read = aRs instanceof ResultSet ? ((ResultSet) aRs).getObject(aColumnIndex) : ((CallableStatement) aRs).getObject(aColumnIndex);
        boolean wasNull = aRs instanceof ResultSet ? ((ResultSet) aRs).wasNull() : ((CallableStatement) aRs).wasNull();
        if (wasNull) {
            return null;
        } else {
            if (read instanceof PGgeometry) {
                PGgeometry pgg = (PGgeometry) read;
                read = pgg.getGeometry();
            } else if (read.getClass().getName().equals(PGgeometry.class.getName())) {// Crazy netbeans designer!
                return read.toString();
            }
            if (read instanceof org.postgis.Geometry) {
                org.postgis.Geometry g = (org.postgis.Geometry) read;
                StringBuffer sb = new StringBuffer();
                g.outerWKT(sb);
                return sb.toString();
            } else {
                return null;
            }
        }
    }
}
