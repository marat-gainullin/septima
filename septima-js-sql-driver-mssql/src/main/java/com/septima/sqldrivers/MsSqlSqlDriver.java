package com.septima.sqldrivers;

import com.septima.metadata.*;
import com.septima.metadata.ForeignKey;
import com.septima.sqldrivers.resolvers.MsSqlTypesResolver;
import com.septima.sqldrivers.resolvers.TypesResolver;
import java.sql.Connection;
import java.sql.Wrapper;
import java.util.List;

/**
 *
 * @author mg
 */
public class MsSqlSqlDriver extends SqlDriver {

    private static final String MS_SQL_DIALECT = "MsSql";
    private static final char ESCAPE = '"';

    private static final String GET_SCHEMA_CLAUSE = "SELECT SCHEMA_NAME()";
    private static final String CREATE_SCHEMA_CLAUSE = "CREATE SCHEMA %s";
    private static final MsSqlTypesResolver resolver = new MsSqlTypesResolver();
    private static final String ADD_COLUMN_COMMENT_CLAUSE = ""
            + "begin "
            + "begin try "
            + "EXEC sys.sp_dropextendedproperty @name=N'MS_Description' , @level0type=N'SCHEMA',@level0name=N'%s', @level1type=N'TABLE',@level1name=N'%s', @level2type=N'COLUMN',@level2name=N'%s' "
            + "end try "
            + "begin catch "
            + "end catch "
            + "EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'%s' , @level0type=N'SCHEMA',@level0name=N'%s', @level1type=N'TABLE',@level1name=N'%s', @level2type=N'COLUMN',@level2name=N'%s' "
            + " commit "
            + "end ";
    private static final String ADD_TABLE_COMMENT_CLAUSE = ""
            + "begin "
            + "  begin try "
            + "    EXEC sys.sp_dropextendedproperty @name=N'MS_Description' , @level0type=N'SCHEMA',@level0name=N'%s', @level1type=N'TABLE',@level1name=N'%s'"
            + "  end try  "
            + "  begin catch "
            + "  end catch  "
            + "  EXEC sys.sp_addextendedproperty @name=N'MS_Description', @value=N'%s' , @level0type=N'SCHEMA',@level0name=N'%s', @level1type=N'TABLE',@level1name=N'%s' "
            + " commit "
            + "end ";
    private static final String ALTER_COLUMN_SQL_PREFIX = "alter table %s alter column ";

    public MsSqlSqlDriver() {
        super();
    }

    @Override
    public String getDialect() {
        return MS_SQL_DIALECT;
    }

    @Override
    public boolean is(String aJdbcUrl) {
        return aJdbcUrl.contains("jdbc:jtds:sqlserver");
    }

    @Override
    public String getSql4DropTable(String aSchemaName, String aTableName) {
        String fullName = makeFullName(aSchemaName, aTableName);
        return "drop table " + fullName;
    }

    @Override
    public String getSql4EmptyTableCreation(String aSchemaName, String aTableName, String aPkColumnName) {
        String fullName = makeFullName(aSchemaName, aTableName);
        return "CREATE TABLE " + fullName + " ("
                + escapeNameIfNeeded(aPkColumnName) + " NUMERIC(18, 0) NOT NULL,"
                + "CONSTRAINT " + escapeNameIfNeeded(aTableName + PRIMARY_KEY_NAME_SUFFIX) + " PRIMARY KEY (" + escapeNameIfNeeded(aPkColumnName) + " ASC))";
    }

    private String getColumnTypeDefinition(JdbcColumn aColumn) {
        String typeDefine = "";
        String sqlTypeName = aColumn.getRdbmsType().toLowerCase();
        typeDefine += sqlTypeName;
        // field length
        int size = aColumn.getSize();
        int scale = aColumn.getScale();

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

    @Override
    public String getSql4DropFkConstraint(String aSchemaName, ForeignKey aFk) {
        String tableName = makeFullName(aSchemaName, aFk.getTable());
        return "ALTER TABLE " + tableName + " DROP CONSTRAINT " + escapeNameIfNeeded(aFk.getCName());
    }

    @Override
    public String getSql4GetSchema() {
        return GET_SCHEMA_CLAUSE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String[] getSqlsOfColumnModify(String aSchemaName, String aTableName, JdbcColumn aOldColumn, JdbcColumn aNewColumn) {
        String fullTableName = makeFullName(aSchemaName, aTableName);
        String alterColumnSql = String.format(ALTER_COLUMN_SQL_PREFIX, fullTableName);
        return new String[]{alterColumnSql + getSqlOfColumnDefinition(aNewColumn)};
    }

    @Override
    public String[] getSqlsOfColumnRename(String aSchemaName, String aTableName, String aOldColumnName, JdbcColumn aNewColumn) {
        String fullTableName = makeFullName(aSchemaName, aTableName);
        String sql = String.format("EXEC sp_rename '%s.%s','%s','COLUMN'", fullTableName, aOldColumnName, aNewColumn.getName());
        return new String[]{sql};
    }

    @Override
    public String[] getSqls4CreateColumnComment(String aOwnerName, String aTableName, String aColumnName, String aDescription) {
        if (aDescription == null) {
            aDescription = "";
        }
        return new String[]{String.format(ADD_COLUMN_COMMENT_CLAUSE, unescapeNameIfNeeded(aOwnerName), unescapeNameIfNeeded(aTableName), unescapeNameIfNeeded(aColumnName), aDescription, unescapeNameIfNeeded(aOwnerName), unescapeNameIfNeeded(aTableName), unescapeNameIfNeeded(aColumnName))};
    }

    @Override
    public String getSql4CreateTableComment(String aOwnerName, String aTableName, String aDescription) {
        if (aDescription == null) {
            aDescription = "";
        }
        return String.format(ADD_TABLE_COMMENT_CLAUSE, unescapeNameIfNeeded(aOwnerName), unescapeNameIfNeeded(aTableName), aDescription, unescapeNameIfNeeded(aOwnerName), unescapeNameIfNeeded(aTableName));
    }

    @Override
    public TypesResolver getTypesResolver() {
        return resolver;
    }

    @Override
    public String getSql4CreateSchema(String aSchemaName, String aPassword) {
        if (aSchemaName != null && !aSchemaName.isEmpty()) {
            return String.format(CREATE_SCHEMA_CLAUSE, aSchemaName);
        } else {
            throw new IllegalArgumentException("Schema name is null or empty.");
        }
    }

    @Override
    public String getSql4DropPkConstraint(String aSchemaName, PrimaryKey aPk) {
        String constraintName = escapeNameIfNeeded(aPk.getCName());
        String tableName = makeFullName(aSchemaName, aPk.getTable());
        return "alter table " + tableName + " drop constraint " + constraintName;
    }

    @Override
    public String getSql4CreateFkConstraint(String aSchemaName, List<ForeignKey> listFk) {
        if (listFk != null && listFk.size() > 0) {
            ForeignKey fk = listFk.get(0);
            PrimaryKey pk = fk.getReferee();
            String fkTableName = makeFullName(aSchemaName, fk.getTable());
            String fkName = fk.getCName();
            // String pkSchemaName = pk.getSchema();
            String pkTableName = makeFullName(aSchemaName, pk.getTable());

            StringBuilder fkColumnName = new StringBuilder(escapeNameIfNeeded(fk.getColumn()));
            StringBuilder pkColumnName = new StringBuilder(escapeNameIfNeeded(pk.getColumn()));
            for (int i = 1; i < listFk.size(); i++) {
                fk = listFk.get(i);
                pk = fk.getReferee();
                fkColumnName.append(", ").append(escapeNameIfNeeded(fk.getColumn()));
                pkColumnName.append(", ").append(escapeNameIfNeeded(pk.getColumn()));
            }

            String fkRule = "";
            switch (fk.getDeleteRule()) {
                case CASCADE:
                    fkRule += " ON DELETE CASCADE ";
                    break;
                case NO_ACTION:
                    fkRule += " ON DELETE NO ACTION ";
                    break;
                case SET_DEFAULT:
                    fkRule += " ON DELETE SET DEFAULT ";
                    break;
                case SET_NULL:
                    fkRule += " ON DELETE set null ";
                    break;
            }
            switch (fk.getUpdateRule()) {
                case CASCADE:
                    fkRule += " ON UPDATE CASCADE ";
                    break;
                case NO_ACTION:
                    fkRule += " ON UPDATE NO ACTION ";
                    break;
                case SET_DEFAULT:
                    fkRule += " ON UPDATE SET DEFAULT ";
                    break;
                case SET_NULL:
                    fkRule += " ON UPDATE set null ";
                    break;
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
        return false;
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
        return null;
    }

    @Override
    public String geometryToWkt(Wrapper aRs, int aColumnIndex, Connection aConnection) {
        return null;
    }
}
