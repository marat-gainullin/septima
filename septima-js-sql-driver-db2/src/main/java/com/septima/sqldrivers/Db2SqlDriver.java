package com.septima.sqldrivers;

import com.septima.metadata.*;
import com.septima.metadata.ForeignKey;
import com.septima.sqldrivers.resolvers.Db2TypesResolver;
import com.septima.sqldrivers.resolvers.TypesResolver;
import java.sql.Connection;
import java.sql.Wrapper;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author mg
 */
public class Db2SqlDriver extends SqlDriver {

    private static final String DB2_DIALECT = "Db2";
    private static final char ESCAPE = '"';

    private static final String GET_SCHEMA_CLAUSE = "VALUES CURRENT SCHEMA";
    private static final String CREATE_SCHEMA_CLAUSE = "CREATE SCHEMA %s";
    private static final Db2TypesResolver resolver = new Db2TypesResolver();
    private static final String SQL_RENAME_COLUMN = "alter table %s rename column %s transform %s";
    private static final String ALTER_COLUMN_SQL_PREFIX = "alter table %s alter column ";
    private static final String RE_ORG_TABLE = "CALL SYSPROC.ADMIN_CMD('REORG TABLE %s')";
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
    public String[] getSqls4CreateColumnComment(String aOwnerName, String aTableName, String aColumnName, String aDescription) {
        aOwnerName = escapeNameIfNeeded(aOwnerName);
        aTableName = escapeNameIfNeeded(aTableName);
        aColumnName = escapeNameIfNeeded(aColumnName);
        String sqlText = aOwnerName == null ? String.join(".", aTableName, aColumnName) : String.join(".", aOwnerName, aTableName, aColumnName);
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
    public String getSql4DropFkConstraint(String aSchemaName, ForeignKey aFk) {
        String constraintName = escapeNameIfNeeded(aFk.getCName());
        String tableName = makeFullName(aSchemaName, aFk.getTable());
        return "alter table " + tableName + " drop constraint " + constraintName;
    }

    @Override
    public String getSql4EmptyTableCreation(String aSchemaName, String aTableName, String aPkColumnName) {
        String tableName = makeFullName(aSchemaName, aTableName);
        aPkColumnName = escapeNameIfNeeded(aPkColumnName);
        return "CREATE TABLE " + tableName + " ("
                + aPkColumnName + " DECIMAL(18,0) NOT NULL,"
                + "CONSTRAINT " + escapeNameIfNeeded(aTableName + PRIMARY_KEY_NAME_SUFFIX) + " PRIMARY KEY (" + aPkColumnName + "))";
    }

    private String getColumnTypeDefinition(JdbcColumn aColumn) {
        String typeName = aColumn.getRdbmsType();
        int size = aColumn.getSize();
        int scale = aColumn.getScale();
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
    public String getSqlOfColumnDefinition(JdbcColumn aColumn) {
        return escapeNameIfNeeded(aColumn.getName()) + " " + getColumnTypeDefinition(aColumn);
    }

    @Override
    public String[] getSqlsOfColumnModify(String aSchemaName, String aTableName, JdbcColumn aOldColumn, JdbcColumn aNewColumn) {
        List<String> sqls = new ArrayList<>();
        String fullTableName = makeFullName(aSchemaName, aTableName);
        String updateDefinition = String.format(ALTER_COLUMN_SQL_PREFIX, fullTableName) + escapeNameIfNeeded(aOldColumn.getName()) + " ";
        String columnDefinition = getColumnTypeDefinition(aNewColumn);

        String newSqlTypeName = aNewColumn.getRdbmsType();
        if (newSqlTypeName == null) {
            newSqlTypeName = "";
        }
        int newScale = aNewColumn.getScale();
        int newSize = aNewColumn.getSize();
        boolean newNullable = aNewColumn.isNullable();

        String oldSqlTypeName = aOldColumn.getRdbmsType();
        if (oldSqlTypeName == null) {
            oldSqlTypeName = "";
        }
        int oldScale = aOldColumn.getScale();
        int oldSize = aOldColumn.getSize();
        boolean oldNullable = aOldColumn.isNullable();

        sqls.add(getSql4VolatileTable(fullTableName));
        if (!oldSqlTypeName.equalsIgnoreCase(newSqlTypeName)
                || (resolver.isSized(newSqlTypeName) && newSize != oldSize)
                || (resolver.isScaled(newSqlTypeName) && newScale != oldScale)) {
            sqls.add(updateDefinition + " set data type " + columnDefinition);
        }
        if (oldNullable != newNullable) {
            sqls.add(updateDefinition + (newNullable ? " drop not null" : " set not null"));
        }
        if (sqls.size() == 1) {
            sqls.clear();
        } else {
            sqls.add(getSql4ReOrgTable(fullTableName));
        }
        return sqls.toArray(new String[sqls.size()]);
    }

    @Override
    public String[] getSqlOfColumnDrop(String aSchemaName, String aTableName, String aColumnName) {
        String fullTableName = makeFullName(aSchemaName, aTableName);
        return new String[]{
            getSql4VolatileTable(fullTableName),
            String.format(DROP_COLUMN_SQL_PREFIX, fullTableName) + escapeNameIfNeeded(aColumnName),
            getSql4ReOrgTable(fullTableName)
        };
    }

    /**
     * DB2 9.7 or later
     */
    @Override
    public String[] getSqlsOfColumnRename(String aSchemaName, String aTableName, String aOldColumnName, JdbcColumn aNewColumn) {
        String fullTableName = makeFullName(aSchemaName, aTableName);
        String sqlText = String.format(SQL_RENAME_COLUMN, fullTableName, escapeNameIfNeeded(aOldColumnName), escapeNameIfNeeded(aNewColumn.getName()));
        return new String[]{
            getSql4VolatileTable(fullTableName),
            sqlText,
            getSql4ReOrgTable(fullTableName)
        };
    }

    private String getSql4VolatileTable(String aTableName) {
        return String.format(VOLATILE_TABLE, aTableName);
    }

    private String getSql4ReOrgTable(String aTableName) {
        return String.format(RE_ORG_TABLE, aTableName);
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
        if (listFk != null && !listFk.isEmpty()) {
            ForeignKey fk = listFk.get(0);
            PrimaryKey pk = fk.getReferee();
            String fkName = fk.getCName();
            String fkTableName = makeFullName(aSchemaName, fk.getTable());
            String pkTableName = makeFullName(aSchemaName, pk.getTable());

            StringBuilder fkColumnName = new StringBuilder(escapeNameIfNeeded(fk.getColumn()));
            StringBuilder pkColumnName = new StringBuilder(escapeNameIfNeeded(pk.getColumn()));
            for (int i = 1; i < listFk.size(); i++) {
                fk = listFk.get(i);
                pk = fk.getReferee();
                fkColumnName.append(", ").append(escapeNameIfNeeded(fk.getColumn()));
                pkColumnName.append(", ").append(escapeNameIfNeeded(pk.getColumn()));
            }

            /*
             * DB2 doesn't allow the "on update cascade" option for
             * foreign key constraints.
             */
            String fkRule = " ON UPDATE NO ACTION";
            switch (fk.getDeleteRule()) {
                case CASCADE:
                    fkRule += " ON DELETE CASCADE ";
                    break;
                case NO_ACTION:
                case SET_DEFAULT:
                    fkRule += " ON DELETE no action ";
                    break;
                case SET_NULL:
                    fkRule += " ON DELETE set null ";
                    break;
            }
            //fkRule += " NOT ENFORCED";
            return String.format("ALTER TABLE %s ADD CONSTRAINT %s"
                    + " FOREIGN KEY (%s) REFERENCES %s (%s) %s", fkTableName, fkName.isEmpty() ? "" : escapeNameIfNeeded(fkName), fkColumnName.toString(), pkTableName, pkColumnName.toString(), fkRule);
        }
        return null;
    }

    @Override
    public String[] getSqlsOfCreatePkConstraint(String aSchemaName, List<PrimaryKey> listPk) {
        if (listPk != null && !listPk.isEmpty()) {
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
                getSql4VolatileTable(pkTableName),
                String.format("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s)", pkTableName, pkName, pkColumnName),
                getSql4ReOrgTable(pkTableName)
            };
        }
        return null;
    }

    @Override
    public boolean isConstraintsDeferrable() {
        return false;
    }

    @Override
    public String[] getSqlsOfColumnAdd(String aSchemaName, String aTableName, JdbcColumn aColumn) {
        List<String> sqls = new ArrayList<>();
        String fullTableName = makeFullName(aSchemaName, aTableName);
        sqls.add(getSql4VolatileTable(fullTableName));
        sqls.add(String.format(SqlDriver.ADD_COLUMN_SQL_PREFIX, fullTableName) + getSqlOfColumnDefinition(aColumn));
        if (!aColumn.isNullable()) {
            sqls.add(String.format(ALTER_COLUMN_SQL_PREFIX, fullTableName) + escapeNameIfNeeded(aColumn.getName()) + " set not null");
        }
        sqls.add(getSql4ReOrgTable(fullTableName));
        return sqls.toArray(new String[]{});
    }

    @Override
    public char getEscape() {
        return ESCAPE;
    }

    @Override
    public NamedJdbcValue geometryFromWkt(String aName, String aValue, Connection aConnection) {
        return null;
    }

    @Override
    public String geometryToWkt(Wrapper aRs, int aColumnIndex, Connection aConnection) {
        return null;
    }
}
