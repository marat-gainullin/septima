package com.septima.sqldrivers;

import com.septima.metadata.ForeignKey;
import com.septima.metadata.JdbcColumn;
import com.septima.metadata.PrimaryKey;
import com.septima.sqldrivers.resolvers.H2TypesResolver;
import com.septima.sqldrivers.resolvers.TypesResolver;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mg
 */
public class H2SqlDriver extends SqlDriver {

    private static final String H2_DIALECT = "H2";
    private static final char ESCAPE = '`';

    private TypesResolver resolver = new H2TypesResolver();
    private static final String GET_SCHEMA_CLAUSE = "SELECT SCHEMA()";
    private static final String CREATE_SCHEMA_CLAUSE = "CREATE SCHEMA IF NOT EXISTS %s";
    private static final String SQL_CREATE_EMPTY_TABLE = "CREATE TABLE %s (%s DECIMAL(18,0) NOT NULL PRIMARY KEY)";
    private static final String SQL_CREATE_TABLE_COMMENT = "COMMENT ON TABLE %s IS '%s'";
    private static final String SQL_CREATE_COLUMN_COMMENT = "COMMENT ON COLUMN %s IS '%s'";
    private static final String SQL_DROP_TABLE = "DROP TABLE %s";
    private static final String SQL_ADD_PK = "ALTER TABLE %s ADD %s PRIMARY KEY (%s)";
    private static final String SQL_DROP_PK = "ALTER TABLE %s DROP PRIMARY KEY";
    private static final String SQL_ADD_FK = "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) %s";
    private static final String SQL_DROP_FK = "ALTER TABLE %s DROP CONSTRAINT %s";
    private static final String SQL_RENAME_COLUMN = "ALTER TABLE %s ALTER COLUMN %s RENAME TO %s";
    private static final String SQL_CHANGE_COLUMN_TYPE = "ALTER TABLE %s ALTER COLUMN %s %s";
    private static final String SQL_CHANGE_COLUMN_NULLABLE = "ALTER TABLE %s ALTER COLUMN %s SET %s NULL";

    public H2SqlDriver() {
        super();
    }

    @Override
    public String getDialect() {
        return H2_DIALECT;
    }

    @Override
    public boolean is(String aJdbcUrl) {
        return aJdbcUrl.contains("jdbc:h2");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isConstraintsDeferrable() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TypesResolver getTypesResolver() {
        return resolver;
    }

    @Override
    public String getSql4GetSchema() {
        return GET_SCHEMA_CLAUSE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSql4CreateSchema(String aSchemaName, String aPassword) {
        if (aSchemaName != null && !aSchemaName.isEmpty()) {
            return String.format(CREATE_SCHEMA_CLAUSE, aSchemaName);
        }
        throw new IllegalArgumentException("Schema name is null or empty.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getSqls4CreateColumnComment(String aOwnerName, String aTableName, String aFieldName, String aDescription) {
        String fullName = escapeNameIfNeeded(aTableName) + "." + escapeNameIfNeeded(aFieldName);
        if (aOwnerName != null && !aOwnerName.isEmpty()) {
            fullName = escapeNameIfNeeded(aOwnerName) + "." + fullName;
        }
        if (aDescription == null) {
            aDescription = "";
        }
        return new String[]{String.format(SQL_CREATE_COLUMN_COMMENT, fullName, escapeSingleQuote(aDescription))};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSql4CreateTableComment(String aOwnerName, String aTableName, String aDescription) {
        String fullName = makeFullName(aOwnerName, aTableName);
        if (aDescription == null) {
            aDescription = "";
        }
        return String.format(SQL_CREATE_TABLE_COMMENT, fullName, escapeSingleQuote(aDescription));
    }

    private String escapeSingleQuote(String str) {
        return str.replaceAll("'", "''"); //NOI18N
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSql4DropTable(String aSchemaName, String aTableName) {
        if (aSchemaName != null && !aSchemaName.isEmpty()) {
            return String.format(SQL_DROP_TABLE, escapeNameIfNeeded(aSchemaName) + "." + escapeNameIfNeeded(aTableName));
        } else {
            return String.format(SQL_DROP_TABLE, escapeNameIfNeeded(aTableName));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSql4DropFkConstraint(String aSchemaName, ForeignKey aFk) {
        String constraintName = escapeNameIfNeeded(aFk.getCName());
        String tableName = makeFullName(aSchemaName, aFk.getTable());
        return String.format(SQL_DROP_FK, tableName, constraintName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getSqls4CreatePkConstraint(String aSchemaName, List<PrimaryKey> listPk) {

        if (listPk != null && listPk.size() > 0) {
            PrimaryKey pk = listPk.get(0);
            String tableName = pk.getTable();
            String pkTableName = makeFullName(aSchemaName, tableName);
            String pkName = escapeNameIfNeeded(tableName + PRIMARY_KEY_NAME_SUFFIX);

            StringBuilder pkColumnName = new StringBuilder(escapeNameIfNeeded(pk.getField()));
            for (int i = 1; i < listPk.size(); i++) {
                pk = listPk.get(i);
                pkColumnName.append(", ").append(escapeNameIfNeeded(pk.getField()));
            }
            return new String[]{
                    String.format(SQL_ADD_PK, pkTableName, "CONSTRAINT " + pkName, pkColumnName.toString())
            };
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSql4DropPkConstraint(String aSchemaName, PrimaryKey aPk) {
        String pkTableName = makeFullName(aSchemaName, aPk.getTable());
        return String.format(SQL_DROP_PK, pkTableName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSql4CreateFkConstraint(String aSchemaName, List<ForeignKey> listFk) {
        if (listFk != null && listFk.size() > 0) {
            ForeignKey fk = listFk.get(0);
            PrimaryKey pk = fk.getReferee();
            String fkTableName = makeFullName(aSchemaName, fk.getTable());
            String pkTableName = makeFullName(aSchemaName, pk.getTable());
            String fkName = fk.getCName();

            // String pkSchemaName = pk.getSchema();
            StringBuilder fkColumnName = new StringBuilder(escapeNameIfNeeded(fk.getField()));
            StringBuilder pkColumnName = new StringBuilder(escapeNameIfNeeded(pk.getField()));

            for (int i = 1; i < listFk.size(); i++) {
                fk = listFk.get(i);
                pk = fk.getReferee();
                fkColumnName.append(", ").append(escapeNameIfNeeded(fk.getField()));
                pkColumnName.append(", ").append(escapeNameIfNeeded(pk.getField()));
            }

            String fkRule = "";
            switch (fk.getUpdateRule()) {
                case CASCADE:
                    fkRule += " ON UPDATE CASCADE";
                    break;
                case NO_ACTION:
//                case SET_DEFAULT:
                    fkRule += " ON UPDATE NO ACTION";
                    break;
                case SET_DEFAULT:
                    fkRule += " ON UPDATE SET DEFAULT";
                    break;
                case SET_NULL:
                    fkRule += " ON UPDATE SET NULL";
                    break;
            }
            switch (fk.getDeleteRule()) {
                case CASCADE:
                    fkRule += " ON DELETE CASCADE";
                    break;
                case NO_ACTION:
//                case SET_DEFAULT:
                    fkRule += " ON DELETE NO ACTION";
                    break;
                case SET_DEFAULT:
                    fkRule += " ON DELETE SET DEFAULT";
                    break;
                case SET_NULL:
                    fkRule += " ON DELETE SET NULL";
                    break;
            }
            return String.format(SQL_ADD_FK, fkTableName, fkName.isEmpty() ? "" : escapeNameIfNeeded(fkName), fkColumnName.toString(), pkTableName, pkColumnName.toString(), fkRule);
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSql4EmptyTableCreation(String aSchemaName, String aTableName, String aPkFieldName) {
        String fullName = makeFullName(aSchemaName, aTableName);
        return String.format(SQL_CREATE_EMPTY_TABLE, fullName, escapeNameIfNeeded(aPkFieldName));
    }

    private String getFieldTypeDefinition(JdbcColumn aField) {
        String typeDefine = "";
        String sqlTypeName = aField.getType().toLowerCase();
        typeDefine += sqlTypeName;
        // field length
        int size = aField.getSize();
        int scale = aField.getScale();

        if (resolver.isScaled(sqlTypeName) && size > 0) {
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
        String fieldDefinition = escapeNameIfNeeded(aField.getName()) + " " + getFieldTypeDefinition(aField);

        if (!aField.isNullable()) {
            fieldDefinition += " NOT NULL";
        } else {
            fieldDefinition += " NULL";
        }
        if (aField.isPk()) {
            fieldDefinition += " PRIMARY KEY";
        }
        return fieldDefinition;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getSqls4FieldModify(String aSchemaName, String aTableName, JdbcColumn aOldFieldMd, JdbcColumn aNewFieldMd) {
        assert aOldFieldMd.getName().toLowerCase().equals(aNewFieldMd.getName().toLowerCase());
        List<String> sql = new ArrayList<>();

        //Change data type
        String lOldTypeName = aOldFieldMd.getType();
        if (lOldTypeName == null) {
            lOldTypeName = "";
        }
        String lNewTypeName = aNewFieldMd.getType();
        if (lNewTypeName == null) {
            lNewTypeName = "";
        }

        String fullTableName = makeFullName(aSchemaName, aTableName);
        if (!lOldTypeName.equalsIgnoreCase(lNewTypeName)
                || aOldFieldMd.getSize() != aNewFieldMd.getSize()
                || aOldFieldMd.getScale() != aNewFieldMd.getScale()) {
            sql.add(String.format(
                    SQL_CHANGE_COLUMN_TYPE,
                    fullTableName,
                    escapeNameIfNeeded(aOldFieldMd.getName()),
                    getFieldTypeDefinition(aNewFieldMd)));
        }

        //Change nullable
        String not = "";
        if (aOldFieldMd.isNullable() != aNewFieldMd.isNullable()) {
            if (!aNewFieldMd.isNullable()) {
                not = "NOT";
            }
            sql.add(String.format(
                    SQL_CHANGE_COLUMN_NULLABLE,
                    fullTableName,
                    escapeNameIfNeeded(aOldFieldMd.getName()),
                    not));
        }

        return sql.toArray(new String[0]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getSqls4FieldRename(String aSchemaName, String aTableName, String aOldFieldName, JdbcColumn aNewFieldMd) {
        String fullTableName = makeFullName(aSchemaName, aTableName);
        String renameSQL = String.format(SQL_RENAME_COLUMN, fullTableName, escapeNameIfNeeded(aOldFieldName), escapeNameIfNeeded(aNewFieldMd.getName()));
        return new String[]{renameSQL};
    }

    @Override
    public String[] getSqls4FieldAdd(String aSchemaName, String aTableName, JdbcColumn aField) {
        String fullTableName = makeFullName(aSchemaName, aTableName);
        return new String[]{
                String.format(SqlDriver.ADD_FIELD_SQL_PREFIX, fullTableName) + getSql4FieldDefinition(aField)
        };
    }

    @Override
    public char getEscape() {
        return ESCAPE;
    }

    @Override
    public NamedJdbcValue geometryFromWkt(String aName, String aValue, Connection aConnection) {
        return new NamedJdbcValue(aName, aValue, Types.OTHER, "GEOMETRY");
    }

    @Override
    public String geometryToWkt(Wrapper aRs, int aColumnIndex, Connection aConnection) throws SQLException {
        String wkt = aRs instanceof ResultSet ? ((ResultSet) aRs).getString(aColumnIndex) : ((CallableStatement) aRs).getString(aColumnIndex);
        boolean wasNull = aRs instanceof ResultSet ? ((ResultSet) aRs).wasNull() : ((CallableStatement) aRs).wasNull();
        if (wasNull) {
            return null;
        } else {
            return wkt;
        }
    }
}
