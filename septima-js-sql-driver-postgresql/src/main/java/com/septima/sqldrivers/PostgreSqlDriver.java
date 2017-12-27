package com.septima.sqldrivers;

import com.septima.metadata.ForeignKey;
import com.septima.metadata.JdbcColumn;
import com.septima.metadata.PrimaryKey;
import com.septima.sqldrivers.resolvers.PostgreTypesResolver;
import com.septima.sqldrivers.resolvers.TypesResolver;
import org.postgis.PGgeometry;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author mg
 */
public class PostgreSqlDriver extends SqlDriver {

    private static final String POSTGRES_DIALECT = "Postgres";
    private static final char ESCAPE = '"';

    private static final PostgreTypesResolver resolver = new PostgreTypesResolver();
    private static final String GET_SCHEMA_CLAUSE = "select current_schema()";
    private static final String CREATE_SCHEMA_CLAUSE = "CREATE SCHEMA %s";
    private static final String RENAME_COLUMN_SQL_PREFIX = "alter table %s rename column %s transform %s";
    private static final String MODIFY_COLUMN_SQL_PREFIX = "alter table %s alter ";

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
    public String[] getSqls4CreateColumnComment(String aOwnerName, String aTableName, String aColumnName, String aDescription) {
        String ownerName = escapeNameIfNeeded(aOwnerName);
        String tableName = escapeNameIfNeeded(aTableName);
        String columnName = escapeNameIfNeeded(aColumnName);
        String sqlText = aOwnerName == null ? String.join(".", tableName, columnName) : String.join(".", ownerName, tableName, columnName);
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
    public String getSql4DropFkConstraint(String aSchemaName, ForeignKey aFk) {
        String constraintName = escapeNameIfNeeded(aFk.getCName());
        String tableName = makeFullName(aSchemaName, aFk.getTable());
        return "alter table " + tableName + " drop constraint " + constraintName + " cascade";
    }

    @Override
    public String getSql4EmptyTableCreation(String aSchemaName, String aTableName, String aPkColumnName) {
        String fullName = makeFullName(aSchemaName, aTableName);
        String pkColumnName = escapeNameIfNeeded(aPkColumnName);
        return "CREATE TABLE " + fullName + " ("
                + pkColumnName + " NUMERIC NOT NULL,"
                + "CONSTRAINT " + escapeNameIfNeeded(aTableName + PRIMARY_KEY_NAME_SUFFIX) + " PRIMARY KEY (" + pkColumnName + "))";
    }

    private String getColumnTypeDefinition(JdbcColumn aColumn) {
        String typeDefine = "";
        String sqlTypeName = aColumn.getRdbmsType().toLowerCase();
        // field length
        int size = aColumn.getSize();
        int scale = aColumn.getScale();

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
    public String getSqlOfColumnDefinition(JdbcColumn aColumn) {
        String columnName = escapeNameIfNeeded(aColumn.getName());
        String columnDefinition = columnName + " " + getColumnTypeDefinition(aColumn);

        if (!aColumn.isNullable()) {
            columnDefinition += " not null";
        } else {
            columnDefinition += " null";
        }
        return columnDefinition;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getSqlsOfColumnModify(String aSchemaName, String aTableName, JdbcColumn aOldColumn, JdbcColumn aNewColumn) {
        List<String> sqls = new ArrayList<>();
        String fullTableName = makeFullName(aSchemaName, aTableName);
        String columnName = escapeNameIfNeeded(aOldColumn.getName());
        String updateDefinition = String.format(MODIFY_COLUMN_SQL_PREFIX, fullTableName) + columnName + " ";
        String columnDefination = getColumnTypeDefinition(aNewColumn);

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

        if (!newSqlTypeName.equalsIgnoreCase(oldSqlTypeName)
                || (resolver.isSized(newSqlTypeName) && newSize != oldSize)
                || (resolver.isScaled(newSqlTypeName) && newScale != oldScale)) {
            sqls.add(updateDefinition + " type " + columnDefination + " using " + columnName + "::" + newSqlTypeName);
        }
        if (oldNullable != newNullable) {
            sqls.add(updateDefinition + (newNullable ? " drop not null" : " set not null"));
        }
        return sqls.toArray(new String[sqls.size()]);
    }

    @Override
    public String[] getSqlsOfColumnRename(String aSchemaName, String aTableName, String aOldColumnName, JdbcColumn aNewColumn) {
        return new String[]{
                String.format(RENAME_COLUMN_SQL_PREFIX,
                        makeFullName(aSchemaName, aTableName),
                        escapeNameIfNeeded(aOldColumnName),
                        escapeNameIfNeeded(aNewColumn.getName()))
        };
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
                    fkRule += " ON UPDATE CASCADE ";
                    break;
                case NO_ACTION:
                    fkRule += " ON UPDATE no action";
                    break;
                case SET_DEFAULT:
                    fkRule += " ON UPDATE set default";
                    break;
                case SET_NULL:
                    fkRule += " ON UPDATE set null";
                    break;
            }
            switch (fk.getDeleteRule()) {
                case CASCADE:
                    fkRule += " ON DELETE CASCADE ";
                    break;
                case NO_ACTION:
                    fkRule += " ON DELETE no action ";
                    break;
                case SET_DEFAULT:
                    fkRule += " ON DELETE set default ";
                    break;
                case SET_NULL:
                    fkRule += " ON DELETE set null ";
                    break;
            }
            if (fk.isDeferrable()) {
                fkRule += " DEFERRABLE INITIALLY DEFERRED";
            }
            return String.format("ALTER TABLE %s ADD CONSTRAINT %s"
                    + " FOREIGN KEY (%s) REFERENCES %s (%s) %s", fkTableName, fkName.isEmpty() ? "" : escapeNameIfNeeded(fkName), fkColumnName.toString(), pkTableName, pkColumnName.toString(), fkRule);
        }
        return null;
    }

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
                    String.format("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s)", pkTableName, pkName, pkColumnName.toString())
            };
        }
        return null;
    }

    @Override
    public boolean isConstraintsDeferrable() {
        return true;
    }

    @Override
    public String[] getSqlsOfColumnAdd(String aSchemaName, String aTableName, JdbcColumn aColumn) {
        String fullTableName = makeFullName(aSchemaName, aTableName);
        return new String[]{
                String.format(SqlDriver.ADD_COLUMN_SQL_PREFIX, fullTableName) + getSqlOfColumnDefinition(aColumn)
        };
    }

    @Override
    public String[] getSqlOfColumnDrop(String aSchemaName, String aTableName, String aColumnName) {
        String fullTableName = escapeNameIfNeeded(aTableName);
        if (aSchemaName != null && !aSchemaName.isEmpty()) {
            fullTableName = escapeNameIfNeeded(aSchemaName) + "." + fullTableName;
        }
        return new String[]{
                String.format(DROP_COLUMN_SQL_PREFIX, fullTableName) + escapeNameIfNeeded(aColumnName) + " cascade"
        };
    }

    @Override
    public char getEscape() {
        return ESCAPE;
    }

    @Override
    public NamedJdbcValue geometryFromWkt(String aName, String aValue, Connection aConnection) throws SQLException {
        return new NamedJdbcValue(
                aName,
                aValue != null ? new PGgeometry(aValue) : null,
                Types.OTHER,
                "geometry");
    }

    @Override
    public String geometryToWkt(Wrapper aRs, int aColumnIndex, Connection aConnection) throws SQLException {
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
