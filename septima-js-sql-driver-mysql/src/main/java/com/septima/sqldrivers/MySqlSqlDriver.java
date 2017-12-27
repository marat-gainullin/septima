package com.septima.sqldrivers;

import com.septima.metadata.*;
import com.septima.metadata.ForeignKey;
import com.septima.sqldrivers.resolvers.MySqlTypesResolver;
import com.septima.sqldrivers.resolvers.TypesResolver;
import java.sql.Connection;
import java.sql.Wrapper;
import java.util.List;
import java.util.Set;

/**
 *
 * @author vy
 */
public class MySqlSqlDriver extends SqlDriver {

    private static final String MYSQL_DIALECT = "MySql";
    private final MySqlTypesResolver resolver = new MySqlTypesResolver();
    private static final char ESCAPE = '`';
    private static final String GET_SCHEMA_CLAUSE = "SELECT DATABASE()";
    private static final String CREATE_SCHEMA_CLAUSE = "CREATE DATABASE %s ENGINE=InnoDB";
    private static final Set<String> numericTypes = Set.of(
            "TINYINT",
            "SMALLINT",
            "MEDIUMINT",
            "INT",
            "INTEGER",
            "BIGINT",
            "FLOAT",
            "DOUBLE",
            "DOUBLE PRECISION",
            "REAL",
            "DECIMAL",
            "DEC",
            "NUMERIC"
    );

    public MySqlSqlDriver() {
        super();
    }

    @Override
    public String getDialect() {
        return MYSQL_DIALECT;
    }

    @Override
    public boolean is(String aJdbcUrl) {
        return aJdbcUrl.contains("jdbc:mysql");
    }

    @Override
    public TypesResolver getTypesResolver() {
        return resolver;
    }

    @Override
    public String[] getSqls4CreateColumnComment(String aOwnerName, String aTableName, String aColumnName, String aDescription) {
        String schemaClause = ((aOwnerName != null && !aOwnerName.trim().isEmpty()) ? escapeNameIfNeeded(aOwnerName) + "." : "");
        if (aDescription == null) {
            aDescription = "";
        }
        String sql0 = "DROP PROCEDURE IF EXISTS " + schemaClause + "setColumnComment";
        String sql1 = ""
                + "CREATE PROCEDURE " + schemaClause + "setColumnComment("
                + "    IN aOwnerName VARCHAR(100), "
                + "    IN aTableName VARCHAR(100), "
                + "    IN aColumnName VARCHAR(100), "
                + "    IN aDescription VARCHAR(100),"
                + "    OUT res text)"
                + "	LANGUAGE SQL"
                + "	NOT DETERMINISTIC"
                + "	MODIFIES SQL DATA"
                + "	SQL SECURITY INVOKER"
                + "	COMMENT 'Процедура для задания комментария к полю таблицы. Нужна из-за необходимости задавать ПОЛНОСТЬЮ определение поля. Для вывода генерируемых скриптов: call setColumnComment(schema,table,column,comment,@a);select @a;'"
                + "BEGIN"
                + "   SET @define_column = '';"
                + "   SET @select_stm = CONCAT"
                + "   ('SELECT CONCAT',"
                + "    '(',"
                + "    '  column_type,'' '',',"
                + "    '  (CASE is_nullable WHEN ''YES'' THEN ''NULL'' ELSE ''NOT NULL'' END),'' '',',"
                //+ "    '  IF(column_default is null,'''',CONCAT(''DEFAULT \"'',column_default,''\" '')),',"
                + "    '  IF(column_default is null,'''',CONCAT(''DEFAULT '',IF(CHARACTER_MAXIMUM_LENGTH > 0, ''\"'', ''''),column_default,IF(CHARACTER_MAXIMUM_LENGTH > 0, ''\"'', ''''),'' '')),',"
                + "    '  IF(extra is null,'''',CONCAT(extra,'' ''))',"
                + "    ') ',"
                + "    'INTO @define_column ',"
                + "    'FROM information_schema.COLUMNS ',"
                + "    'WHERE table_schema = ''',aOwnerName,''' AND',"
                + "    '  table_name = ''',aTableName,''' AND', "
                + "    '  column_name = ''',aColumnName,'''');"
                + "   PREPARE result_select FROM @select_stm;"
                + "   EXECUTE result_select;"
                + "   DROP PREPARE result_select;"
                + "   SET @stm = CONCAT('ALTER TABLE ', IF(LENGTH(aOwnerName), CONCAT('`',aOwnerName,'`.'), ''), '`',aTableName,'`'"
                + "   	' MODIFY COLUMN `',aColumnName,'` ',@define_column,"
                + "     IF(aDescription is null,'''',CONCAT(' COMMENT ''',aDescription,'''')));"
                + "   PREPARE alter_stm FROM @stm;"
                + "   EXECUTE alter_stm;"
                + "   DROP PREPARE alter_stm;"
                + "   SET res = CONCAT(@select_stm,';',@stm); "
                + "END";
        String sql2 = "CALL " + schemaClause + "setColumnComment('"
                + unescapeNameIfNeeded(aOwnerName) + "','" + unescapeNameIfNeeded(aTableName) + "','" + unescapeNameIfNeeded(aColumnName) + "','" + aDescription + "',@a)";
        String sql3 = "DROP PROCEDURE " + schemaClause + "setColumnComment";
        return new String[]{sql0, sql1, sql2, sql3};
    }

    @Override
    public String getSql4CreateTableComment(String aOwnerName, String aTableName, String aDescription) {
        String fullName = makeFullName(aOwnerName, aTableName);
        if (aDescription == null) {
            aDescription = "";
        }
        return String.format("ALTER TABLE %s COMMENT='%s'", fullName, aDescription.replaceAll("'", "''"));
    }

    @Override
    public String getSql4DropTable(String aSchemaName, String aTableName) {
        return "DROP TABLE " + makeFullName(aSchemaName, aTableName);
    }

    @Override
    public String getSql4DropFkConstraint(String aSchemaName, ForeignKey aFk) {
        String fkTableName = makeFullName(aSchemaName, aFk.getTable());
        String fkName = aFk.getCName();
        return String.format("ALTER TABLE %s DROP FOREIGN KEY %s", fkTableName, escapeNameIfNeeded(fkName));
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
                    fkRule += " ON UPDATE CASCADE";
                    break;
                case NO_ACTION:
                    fkRule += " ON UPDATE NO ACTION";
                    break;
                case SET_DEFAULT:
                    // !!! не используется
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
                    fkRule += " ON DELETE NO ACTION";
                    break;
                case SET_DEFAULT:
                    // !!! не используется
                    break;
                case SET_NULL:
                    fkRule += " ON DELETE SET NULL";
                    break;
            }
            return String.format("ALTER TABLE %s ADD CONSTRAINT %s"
                    + " FOREIGN KEY (%s) REFERENCES %s (%s) %s", fkTableName, fkName.isEmpty() ? "" : escapeNameIfNeeded(fkName), fkColumnName.toString(), pkTableName, pkColumnName.toString(), fkRule);

        }
        return null;
    }

    @Override
    public String getSql4EmptyTableCreation(String aSchemaName, String aTableName, String aPkColumnName) {
        String fullName = makeFullName(aSchemaName, aTableName);
        return String.format("CREATE TABLE %s (%s DECIMAL(18,0) NOT NULL,"
                + "CONSTRAINT PRIMARY KEY (%s)) ENGINE=InnoDB", fullName, escapeNameIfNeeded(aPkColumnName), escapeNameIfNeeded(aPkColumnName));
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

    @Override
    public String getSqlOfColumnDefinition(JdbcColumn aColumn) {
        String columnDefinition = escapeNameIfNeeded(aColumn.getName()) + " " + getColumnTypeDefinition(aColumn);
        if (!aColumn.isSigned() && isNumeric(aColumn.getRdbmsType())) {
            columnDefinition += " UNSIGNED";
        }
        if (!aColumn.isNullable()) {
            columnDefinition += " NOT NULL";
        } else {
            columnDefinition += " NULL";
        }
        return columnDefinition;
    }

    @Override
    public String[] getSqlsOfColumnModify(String aSchemaName, String aTableName, JdbcColumn aOldColumn, JdbcColumn aNewColumn) {
        return getSqlsOfColumnRename(aSchemaName, aTableName, aOldColumn.getName(), aNewColumn);
    }

    @Override
    public String[] getSqlsOfColumnRename(String aSchemaName, String aTableName, String aOldColumnName, JdbcColumn aNewColumn) {
        String fullTableName = makeFullName(aSchemaName, aTableName);
        return new String[]{String.format("ALTER TABLE %s CHANGE %s %s", fullTableName, escapeNameIfNeeded(aOldColumnName), getSqlOfColumnDefinition(aNewColumn))};
    }

    @Override
    public String getSql4CreateSchema(String aSchemaName, String aPassword) {
        if (aSchemaName != null && !aSchemaName.isEmpty()) {
            return String.format(CREATE_SCHEMA_CLAUSE, aSchemaName);
        }
        throw new IllegalArgumentException("Schema name is null or empty.");
    }

    @Override
    public String getSql4GetSchema() {
        return GET_SCHEMA_CLAUSE;
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
    public String getSql4DropPkConstraint(String aSchemaName, PrimaryKey aPk) {
        String pkTableName = makeFullName(aSchemaName, aPk.getTable());
        return String.format("ALTER TABLE %s DROP PRIMARY KEY", pkTableName);
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

    private static boolean isNumeric(String aType) {
        return numericTypes.contains(aType.toUpperCase());
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
