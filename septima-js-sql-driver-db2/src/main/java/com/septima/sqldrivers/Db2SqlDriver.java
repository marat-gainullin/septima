package com.septima.sqldrivers;

import com.septima.changes.NamedJdbcValue;
import com.septima.metadata.*;
import com.septima.metadata.ForeignKey;
import com.septima.sqldrivers.resolvers.Db2TypesResolver;
import com.septima.sqldrivers.resolvers.TypesResolver;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author mg
 */
public class Db2SqlDriver extends SqlDriver {

    private static final String DB2_DIALECT = "Db2";
    private static final Character ESCAPE = '"';

    private static final String SET_SCHEMA_CLAUSE = "SET SCHEMA %s";
    private static final String GET_SCHEMA_CLAUSE = "VALUES CURRENT SCHEMA";
    private static final String CREATE_SCHEMA_CLAUSE = "CREATE SCHEMA %s";
    private static final Db2TypesResolver resolver = new Db2TypesResolver();
    /**
     * Listing of SQLSTATE values
     */
    private static final int[] db2ErrorCodes = {};
    private static final String[] platypusErrorMessages = {};
    private static final String SQL_RENAME_FIELD = "alter table %s rename column %s to %s";
    private static final String SQL_MODIFY_FIELD = "alter table %s modify ";
    private static final String ALTER_FIELD_SQL_PREFIX = "alter table %s alter column ";
    private static final String REORG_TABLE = "CALL SYSPROC.ADMIN_CMD('REORG TABLE %s')";
    private static final String VOLATILE_TABLE = "ALTER TABLE %s VOLATILE CARDINALITY";

    @Override
    public String getDialect() {
        return DB2_DIALECT;
    }

    @Override
    public boolean is(String aJdbcUrl) {
        return aJdbcUrl.contains("jdbc:db2");
    }

    @Override
    public String getSql4CreateSchema(String aSchemaName, String aPassword) {
        if (aSchemaName != null && !aSchemaName.isEmpty()) {
            return String.format(CREATE_SCHEMA_CLAUSE, aSchemaName);
        }
        throw new IllegalArgumentException("Schema name is null or empty.");
    }

    @Override
    public String[] getSql4CreateColumnComment(String aOwnerName, String aTableName, String aFieldName, String aDescription) {
        aOwnerName = escapeNameIfNeeded(aOwnerName);
        aTableName = escapeNameIfNeeded(aTableName);
        aFieldName = escapeNameIfNeeded(aFieldName);
        String sqlText = aOwnerName == null ? String.join(".", aTableName, aFieldName) : String.join(".", aOwnerName, aTableName, aFieldName);
        if (aDescription == null) {
            aDescription = "";
        }
        return new String[]{"comment on column " + sqlText + " is '" + aDescription + "'"};
    }

    @Override
    public String getSql4CreateTableComment(String aOwnerName, String aTableName, String aDescription) {
        String sqlText = String.join(".", escapeNameIfNeeded(aOwnerName), escapeNameIfNeeded(aTableName));
        if (aDescription == null) {
            aDescription = "";
        }
        return "comment on table " + sqlText + " is '" + aDescription + "'";
    }

    @Override
    public String getSql4DropTable(String aSchemaName, String aTableName) {
        return "drop table " + makeFullName(aSchemaName, aTableName);
    }

    @Override
    public String getSql4DropIndex(String aSchemaName, String aTableName, String aIndexName) {
        return "drop index " + makeFullName(aSchemaName, aIndexName);
    }

    @Override
    public String getSql4DropFkConstraint(String aSchemaName, ForeignKey aFk) {
        String constraintName = escapeNameIfNeeded(aFk.getCName());
        String tableName = makeFullName(aSchemaName, aFk.getTable());
        return "alter table " + tableName + " drop constraint " + constraintName;
    }

    @Override
    public String getSql4CreateFkConstraint(String aSchemaName, ForeignKey aFk) {
        List<ForeignKey> fkList = new ArrayList<>();
        fkList.add(aFk);
        return getSql4CreateFkConstraint(aSchemaName, fkList);
    }

    @Override
    public String getSql4CreateIndex(String aSchemaName, String aTableName, TableIndex aIndex) {
        assert aIndex.getColumns().size() > 0 : "index definition must consist of at least 1 column";
        String indexName = makeFullName(aSchemaName, aIndex.getName());
        String tableName = makeFullName(aSchemaName, aTableName);
        String modifier = "";
        if (aIndex.isUnique()) {
            modifier = "unique";
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
        return "create " + modifier + " index " + indexName + " on " + tableName + "( " + fieldsList + " )";
    }

    @Override
    public String getSql4EmptyTableCreation(String aSchemaName, String aTableName, String aPkFieldName) {
        String tableName = makeFullName(aSchemaName, aTableName);
        aPkFieldName = escapeNameIfNeeded(aPkFieldName);
        return "CREATE TABLE " + tableName + " ("
                + aPkFieldName + " DECIMAL(18,0) NOT NULL,"
                + "CONSTRAINT " + escapeNameIfNeeded(generatePkName(aTableName, PKEY_NAME_SUFFIX)) + " PRIMARY KEY (" + aPkFieldName + "))";
    }

    @Override
    public String parseException(Exception ex) {
        if (ex instanceof SQLException) {
            SQLException sqlEx = (SQLException) ex;
            int errorCode = sqlEx.getErrorCode();
            for (int i = 0; i < db2ErrorCodes.length; i++) {
                if (errorCode == db2ErrorCodes[i]) {
                    return platypusErrorMessages[i];
                }
            }
        }
        return ex.getLocalizedMessage();
    }

    private String getFieldTypeDefinition(JdbcColumn aField) {
        String typeName = aField.getType();
        int size = aField.getSize();
        int scale = aField.getScale();
        if (resolver.isScaled(typeName) && resolver.isSized(typeName) && size > 0) {
            typeName += "(" + String.valueOf(size) + "," + String.valueOf(scale) + ")";
        } else if (resolver.isSized(typeName) && size > 0) {
            typeName += "(" + String.valueOf(size) + ")";
        } else if (resolver.isScaled(typeName) && scale > 0) {
            typeName += "(" + String.valueOf(scale) + ")";
        }
        return typeName;
    }

    @Override
    public String getSql4FieldDefinition(JdbcColumn aField) {
        return escapeNameIfNeeded(aField.getName()) + " " + getFieldTypeDefinition(aField);
    }

    @Override
    public String[] getSqls4ModifyingField(String aSchemaName, String aTableName, JdbcColumn aOldFieldMd, JdbcColumn aNewField) {
        List<String> sqls = new ArrayList<>();
        String fullTableName = makeFullName(aSchemaName, aTableName);
        String updateDefinition = String.format(ALTER_FIELD_SQL_PREFIX, fullTableName) + escapeNameIfNeeded(aOldFieldMd.getName()) + " ";
        String fieldDefination = getFieldTypeDefinition(aNewField);

        String newSqlTypeName = aNewField.getType();
        if (newSqlTypeName == null) {
            newSqlTypeName = "";
        }
        int newScale = aNewField.getScale();
        int newSize = aNewField.getSize();
        boolean newNullable = aNewField.isNullable();

        String oldSqlTypeName = aOldFieldMd.getType();
        if (oldSqlTypeName == null) {
            oldSqlTypeName = "";
        }
        int oldScale = aOldFieldMd.getScale();
        int oldSize = aOldFieldMd.getSize();
        boolean oldNullable = aOldFieldMd.isNullable();

        sqls.add(getSql4VolatileTable(fullTableName));
        if (!oldSqlTypeName.equalsIgnoreCase(newSqlTypeName)
                || (resolver.isSized(newSqlTypeName) && newSize != oldSize)
                || (resolver.isScaled(newSqlTypeName) && newScale != oldScale)) {
            sqls.add(updateDefinition + " set data type " + fieldDefination);
        }
        if (oldNullable != newNullable) {
            sqls.add(updateDefinition + (newNullable ? " drop not null" : " set not null"));
        }
        if (sqls.size() == 1) {
            sqls.clear();
        } else {
            sqls.add(getSql4ReorgTable(fullTableName));
        }
        return sqls.toArray(new String[sqls.size()]);
    }

    @Override
    public String[] getSql4DroppingField(String aSchemaName, String aTableName, String aFieldName) {
        String fullTableName = makeFullName(aSchemaName, aTableName);
        return new String[]{
            getSql4VolatileTable(fullTableName),
            String.format(DROP_FIELD_SQL_PREFIX, fullTableName) + escapeNameIfNeeded(aFieldName),
            getSql4ReorgTable(fullTableName)
        };
    }

    /**
     * DB2 9.7 or later
     */
    @Override
    public String[] getSqls4RenamingField(String aSchemaName, String aTableName, String aOldFieldName, JdbcColumn aNewFieldMd) {
        String fullTableName = makeFullName(aSchemaName, aTableName);
        String sqlText = String.format(SQL_RENAME_FIELD, fullTableName, escapeNameIfNeeded(aOldFieldName), escapeNameIfNeeded(aNewFieldMd.getName()));
        return new String[]{
            getSql4VolatileTable(fullTableName),
            sqlText,
            getSql4ReorgTable(fullTableName)
        };
    }

    private String getSql4VolatileTable(String aTableName) {
        return String.format(VOLATILE_TABLE, aTableName);
    }

    private String getSql4ReorgTable(String aTableName) {
        return String.format(REORG_TABLE, aTableName);
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
    public String getSql4DropPkConstraint(String aSchemaName, PrimaryKey aPk) {
        return "alter table " + makeFullName(aSchemaName, aPk.getTable()) + " drop primary key";
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

            /*
             * The DB2 system does not allow the "on update cascade" option for
             * foreign key constraints.
             */
            String fkRule = " ON UPDATE NO ACTION";
            switch (fk.getDeleteRule()) {
                case CASCADE:
                    fkRule += " ON DELETE CASCADE ";
                    break;
                case NOACTION:
                case SETDEFAULT:
                    fkRule += " ON DELETE no action ";
                    break;
                case SETNULL:
                    fkRule += " ON DELETE set null ";
                    break;
            }
            //fkRule += " NOT ENFORCED";
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
                getSql4VolatileTable(pkTableName),
                String.format("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s)", pkTableName, pkName, pkColumnName),
                getSql4ReorgTable(pkTableName)
            };
        }
        return null;
    }

    @Override
    public boolean isConstraintsDeferrable() {
        return false;
    }

    @Override
    public String[] getSqls4AddingField(String aSchemaName, String aTableName, JdbcColumn aField) {
        List<String> sqls = new ArrayList<>();
        String fullTableName = makeFullName(aSchemaName, aTableName);
        sqls.add(getSql4VolatileTable(fullTableName));
        sqls.add(String.format(SqlDriver.ADD_FIELD_SQL_PREFIX, fullTableName) + getSql4FieldDefinition(aField));
        if (!aField.isNullable()) {
            sqls.add(String.format(ALTER_FIELD_SQL_PREFIX, fullTableName) + escapeNameIfNeeded(aField.getName()) + " set not null");
        }
        sqls.add(getSql4ReorgTable(fullTableName));
        return sqls.toArray(new String[]{});
    }

    @Override
    public Character getEscape() {
        return ESCAPE;
    }

    @Override
    public NamedJdbcValue convertGeometry(String aValue, Connection aConnection) throws SQLException {
        return null;
    }

    @Override
    public String readGeometry(Wrapper aRs, int aColumnIndex, Connection aConnection) throws SQLException {
        return null;
    }
}
