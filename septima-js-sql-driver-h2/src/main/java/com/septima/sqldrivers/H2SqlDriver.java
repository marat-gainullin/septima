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
    public String[] getSqls4CreateColumnComment(String aOwnerName, String aTableName, String aColumnName, String aDescription) {
        String fullName = escapeNameIfNeeded(aTableName) + "." + escapeNameIfNeeded(aColumnName);
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
    public String[] getSqlsOfCreatePkConstraint(String aSchemaName, List<PrimaryKey> listPk) {

        if (listPk != null && listPk.size() > 0) {
            PrimaryKey pk = listPk.get(0);
            String tableName = pk.getTable();
            String pkTableName = makeFullName(aSchemaName, tableName);
            String pkName = escapeNameIfNeeded(tableName + PRIMARY_KEY_NAME_SUFFIX);

            StringBuilder pkColumnName = new StringBuilder(escapeNameIfNeeded(pk.getColumn()));
            for (int i = 1; i < listPk.size(); i++) {
                pk = listPk.get(i);
                pkColumnName.append(", ").append(escapeNameIfNeeded(pk.getColumn()));
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
            StringBuilder fkColumnName = new StringBuilder(escapeNameIfNeeded(fk.getColumn()));
            StringBuilder pkColumnName = new StringBuilder(escapeNameIfNeeded(pk.getColumn()));

            for (int i = 1; i < listFk.size(); i++) {
                fk = listFk.get(i);
                pk = fk.getReferee();
                fkColumnName.append(", ").append(escapeNameIfNeeded(fk.getColumn()));
                pkColumnName.append(", ").append(escapeNameIfNeeded(pk.getColumn()));
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
    public String getSql4EmptyTableCreation(String aSchemaName, String aTableName, String aPkColumnName) {
        String fullName = makeFullName(aSchemaName, aTableName);
        return String.format(SQL_CREATE_EMPTY_TABLE, fullName, escapeNameIfNeeded(aPkColumnName));
    }

    private String getColumnTypeDefinition(JdbcColumn aColumn) {
        String typeDefine = "";
        String sqlTypeName = aColumn.getRdbmsType().toLowerCase();
        typeDefine += sqlTypeName;
        // field length
        int size = aColumn.getSize();
        int scale = aColumn.getScale();

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
    public String getSqlOfColumnDefinition(JdbcColumn aColumn) {
        String columnDefinition = escapeNameIfNeeded(aColumn.getName()) + " " + getColumnTypeDefinition(aColumn);

        if (!aColumn.isNullable()) {
            columnDefinition += " NOT NULL";
        } else {
            columnDefinition += " NULL";
        }
        if (aColumn.isPk()) {
            columnDefinition += " PRIMARY KEY";
        }
        return columnDefinition;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getSqlsOfColumnModify(String aSchemaName, String aTableName, JdbcColumn aOldColumn, JdbcColumn aNewColumn) {
        assert aOldColumn.getName().toLowerCase().equals(aNewColumn.getName().toLowerCase());
        List<String> sql = new ArrayList<>();

        //EntityAction data type
        String lOldTypeName = aOldColumn.getRdbmsType();
        if (lOldTypeName == null) {
            lOldTypeName = "";
        }
        String lNewTypeName = aNewColumn.getRdbmsType();
        if (lNewTypeName == null) {
            lNewTypeName = "";
        }

        String fullTableName = makeFullName(aSchemaName, aTableName);
        if (!lOldTypeName.equalsIgnoreCase(lNewTypeName)
                || aOldColumn.getSize() != aNewColumn.getSize()
                || aOldColumn.getScale() != aNewColumn.getScale()) {
            sql.add(String.format(
                    SQL_CHANGE_COLUMN_TYPE,
                    fullTableName,
                    escapeNameIfNeeded(aOldColumn.getName()),
                    getColumnTypeDefinition(aNewColumn)));
        }

        //EntityAction nullable
        String not = "";
        if (aOldColumn.isNullable() != aNewColumn.isNullable()) {
            if (!aNewColumn.isNullable()) {
                not = "NOT";
            }
            sql.add(String.format(
                    SQL_CHANGE_COLUMN_NULLABLE,
                    fullTableName,
                    escapeNameIfNeeded(aOldColumn.getName()),
                    not));
        }

        return sql.toArray(new String[0]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getSqlsOfColumnRename(String aSchemaName, String aTableName, String aOldColumnName, JdbcColumn aNewColumn) {
        String fullTableName = makeFullName(aSchemaName, aTableName);
        String renameSQL = String.format(SQL_RENAME_COLUMN, fullTableName, escapeNameIfNeeded(aOldColumnName), escapeNameIfNeeded(aNewColumn.getName()));
        return new String[]{renameSQL};
    }

    @Override
    public String[] getSqlsOfColumnAdd(String aSchemaName, String aTableName, JdbcColumn aColumn) {
        String fullTableName = makeFullName(aSchemaName, aTableName);
        return new String[]{
                String.format(SqlDriver.ADD_COLUMN_SQL_PREFIX, fullTableName) + getSqlOfColumnDefinition(aColumn)
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
