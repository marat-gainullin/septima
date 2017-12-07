package com.septima.sqldrivers;

import com.septima.client.ClientConstants;
import com.septima.changes.JdbcChangeValue;
import com.septima.metadata.*;
import com.septima.metadata.ForeignKey;
import com.septima.sqldrivers.resolvers.MySqlTypesResolver;
import com.septima.sqldrivers.resolvers.TypesResolver;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Wrapper;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *
 * @author vy
 */
public class MySqlSqlDriver extends SqlDriver {

    // настройка экранирования наименования объектов БД
    private static final TwinString[] charsForWrap = {new TwinString("`", "`")};
    private static final char[] restrictedChars = {' ', ',', '\'', '"'};

    protected static final int[] mySqlErrorCodes = {};
    protected static final String[] platypusErrorMessages = {};
    protected MySqlTypesResolver resolver = new MySqlTypesResolver();
    protected static final String SET_SCHEMA_CLAUSE = "USE %s";
    protected static final String GET_SCHEMA_CLAUSE = "SELECT DATABASE()";
    protected static final String CREATE_SCHEMA_CLAUSE = "CREATE DATABASE %s ENGINE=InnoDB";

    public MySqlSqlDriver() {
        super();
    }

    @Override
    public boolean is(String aDialect) {
        return ClientConstants.SERVER_PROPERTY_MYSQL_DIALECT.equals(aDialect);
    }

    @Override
    public TypesResolver getTypesResolver() {
        return resolver;
    }

    @Override
    public String getUsersSpaceInitResourceName() {
        return "/sqlscripts/MySqlInitUsersSpace.sql";
    }

    @Override
    public String getVersionInitResourceName() {
        return "/sqlscripts/MySqlInitVersion.sql";
    }

    @Override
    public String[] getSql4CreateColumnComment(String aOwnerName, String aTableName, String aFieldName, String aDescription) {
        String schemaClause = ((aOwnerName != null && !aOwnerName.trim().isEmpty()) ? wrapNameIfRequired(aOwnerName) + "." : "");
        if (aDescription == null) {
            aDescription = "";
        }
        String sql0 = "DROP PROCEDURE IF EXISTS " + schemaClause + "setColumnComment";
        String sql1 = ""
                + "CREATE PROCEDURE " + schemaClause + "setColumnComment("
                + "    IN aOwnerName VARCHAR(100), "
                + "    IN aTableName VARCHAR(100), "
                + "    IN aFieldName VARCHAR(100), "
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
                + "    '  column_name = ''',aFieldName,'''');"
                + "   PREPARE result_select FROM @select_stm;"
                + "   EXECUTE result_select;"
                + "   DROP PREPARE result_select;"
                + "   SET @stm = CONCAT('ALTER TABLE ', IF(LENGTH(aOwnerName), CONCAT('`',aOwnerName,'`.'), ''), '`',aTableName,'`'"
                + "   	' MODIFY COLUMN `',aFieldName,'` ',@define_column,"
                + "     IF(aDescription is null,'''',CONCAT(' COMMENT ''',aDescription,'''')));"
                + "   PREPARE alter_stm FROM @stm;"
                + "   EXECUTE alter_stm;"
                + "   DROP PREPARE alter_stm;"
                + "   SET res = CONCAT(@select_stm,';',@stm); "
                + "END";
        String sql2 = "CALL " + schemaClause + "setColumnComment('"
                + unwrapName(aOwnerName) + "','" + unwrapName(aTableName) + "','" + unwrapName(aFieldName) + "','" + aDescription + "',@a)";
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
    public String getSql4DropIndex(String aSchemaName, String aTableName, String aIndexName) {
        return String.format("DROP INDEX %s ON %s", wrapNameIfRequired(aIndexName), makeFullName(aSchemaName, aTableName));
    }

    @Override
    public String getSql4DropFkConstraint(String aSchemaName, ForeignKey aFk) {

        String fkTableName = makeFullName(aSchemaName, aFk.getTable());
        String fkName = aFk.getCName();
        return String.format("ALTER TABLE %s DROP FOREIGN KEY %s", fkTableName, wrapNameIfRequired(fkName));
    }

    @Override
    public String getSql4CreateFkConstraint(String aSchemaName, ForeignKey aFk) {
        List<ForeignKey> fkList = new ArrayList<>();
        fkList.add(aFk);
        return getSql4CreateFkConstraint(aSchemaName, fkList);
    }

    @Override
    public String getSql4CreateFkConstraint(String aSchemaName, List<ForeignKey> listFk) {
        if (listFk != null && listFk.size() > 0) {
            ForeignKey fk = listFk.get(0);
            String fkTableName = makeFullName(aSchemaName, fk.getTable());
            String fkName = fk.getCName();
            String fkColumnName = wrapNameIfRequired(fk.getField());

            PrimaryKey pk = fk.getReferee();
            String pkSchemaName = pk.getSchema();
            String pkTableName = makeFullName(aSchemaName, pk.getTable());
            String pkColumnName = wrapNameIfRequired(pk.getField());

            for (int i = 1; i < listFk.size(); i++) {
                fk = listFk.get(i);
                pk = fk.getReferee();
                fkColumnName += ", " + wrapNameIfRequired(fk.getField());
                pkColumnName += ", " + wrapNameIfRequired(pk.getField());
            }

            String fkRule = "";
            switch (fk.getFkUpdateRule()) {
                case CASCADE:
                    fkRule += " ON UPDATE CASCADE";
                    break;
                case NOACTION:
                    fkRule += " ON UPDATE NO ACTION";
                    break;
                case SETDEFAULT:
                    // !!! не используется
                    break;
                case SETNULL:
                    fkRule += " ON UPDATE SET NULL";
                    break;
            }
            switch (fk.getFkDeleteRule()) {
                case CASCADE:
                    fkRule += " ON DELETE CASCADE";
                    break;
                case NOACTION:
                    fkRule += " ON DELETE NO ACTION";
                    break;
                case SETDEFAULT:
                    // !!! не используется
                    break;
                case SETNULL:
                    fkRule += " ON DELETE SET NULL";
                    break;
            }
            return String.format("ALTER TABLE %s ADD CONSTRAINT %s"
                    + " FOREIGN KEY (%s) REFERENCES %s (%s) %s", fkTableName, fkName.isEmpty() ? "" : wrapNameIfRequired(fkName), fkColumnName, pkTableName, pkColumnName, fkRule);

        }
        return null;
    }

    @Override
    public String getSql4CreateIndex(String aSchemaName, String aTableName, TableIndex aIndex) {
        assert aIndex.getColumns().size() > 0 : "index definition must consist of at least 1 column";

        String tableName = makeFullName(aSchemaName, aTableName);
        String fieldsList = "";
        for (int i = 0; i < aIndex.getColumns().size(); i++) {
            TableIndexColumn column = aIndex.getColumns().get(i);
            fieldsList += wrapNameIfRequired(column.getColumnName());
            if (!column.isAscending()) {
                fieldsList += " DESC";
            }
            if (i != aIndex.getColumns().size() - 1) {
                fieldsList += ", ";
            }
        }
        return "CREATE " + (aIndex.isUnique() ? "UNIQUE " : "")
                + "INDEX " + wrapNameIfRequired(aIndex.getName()) + (aIndex.isHashed() ? " USING HASH " : " ")
                + "ON " + tableName + " (" + fieldsList + ")";
    }

    @Override
    public String getSql4EmptyTableCreation(String aSchemaName, String aTableName, String aPkFieldName) {
        String fullName = makeFullName(aSchemaName, aTableName);
        return String.format("CREATE TABLE %s (%s DECIMAL(18,0) NOT NULL,"
                + "CONSTRAINT PRIMARY KEY (%s)) ENGINE=InnoDB", fullName, wrapNameIfRequired(aPkFieldName), wrapNameIfRequired(aPkFieldName));
    }

    @Override
    public String parseException(Exception ex) {
        if (ex != null && ex instanceof SQLException) {
            SQLException sqlEx = (SQLException) ex;
            int errorCode = sqlEx.getErrorCode();
            for (int i = 0; i < mySqlErrorCodes.length; i++) {
                if (errorCode == mySqlErrorCodes[i]) {
                    return platypusErrorMessages[i];
                }
            }
        }
        return ex != null ? ex.getLocalizedMessage() : null;
    }

    private String getFieldTypeDefinition(JdbcColumn aField) {
        String typeDefine = "";
        String sqlTypeName = aField.getType().toLowerCase();
        typeDefine += sqlTypeName;
        // field length
        int size = aField.getSize();
        int scale = aField.getScale();

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
    public String getSql4FieldDefinition(JdbcColumn aField) {
        String fieldDefinition = wrapNameIfRequired(aField.getName()) + " " + getFieldTypeDefinition(aField);
        if (!aField.isSigned() && isNumeric(aField.getType())) {
            fieldDefinition += " UNSIGNED";
        }
        if (!aField.isNullable()) {
            fieldDefinition += " NOT NULL";
        } else {
            fieldDefinition += " NULL";
        }
        return fieldDefinition;
    }

    @Override
    public String[] getSqls4ModifyingField(String aSchemaName, String aTableName, JdbcColumn aOldFieldMd, JdbcColumn aNewFieldMd) {
        return getSqls4RenamingField(aSchemaName, aTableName, aOldFieldMd.getName(), aNewFieldMd);
    }

    @Override
    public String[] getSqls4RenamingField(String aSchemaName, String aTableName, String aOldFieldName, JdbcColumn aNewFieldMd) {
        String fullTableName = makeFullName(aSchemaName, aTableName);
        return new String[]{String.format("ALTER TABLE %s CHANGE %s %s", fullTableName, wrapNameIfRequired(aOldFieldName), getSql4FieldDefinition(aNewFieldMd))};
    }

    @Override
    public String getSql4CreateSchema(String aSchemaName, String aPassword) {
        if (aSchemaName != null && !aSchemaName.isEmpty()) {
            return String.format(CREATE_SCHEMA_CLAUSE, aSchemaName);
        }
        throw new IllegalArgumentException("Schema name is null or empty.");
    }

    @Override
    public String getSql4GetConnectionContext() {
        return GET_SCHEMA_CLAUSE;
    }

    @Override
    public void applyContextToConnection(Connection aConnection, String aSchema) throws Exception {
        if (aSchema != null && !aSchema.isEmpty()) {
            try (Statement stmt = aConnection.createStatement()) {
                stmt.execute(String.format(SET_SCHEMA_CLAUSE, wrapNameIfRequired(aSchema)));
            }
        }
    }

    @Override
    public String[] getSql4CreatePkConstraint(String aSchemaName, List<PrimaryKey> listPk) {

        if (listPk != null && listPk.size() > 0) {
            PrimaryKey pk = listPk.get(0);
            String tableName = pk.getTable();
            String pkTableName = makeFullName(aSchemaName, tableName);
            String pkName = wrapNameIfRequired(generatePkName(tableName, PKEY_NAME_SUFFIX));
            String pkColumnName = wrapNameIfRequired(pk.getField());
            for (int i = 1; i < listPk.size(); i++) {
                pk = listPk.get(i);
                pkColumnName += ", " + wrapNameIfRequired(pk.getField());
            }
            return new String[]{
                String.format("ALTER TABLE %s ADD CONSTRAINT %s PRIMARY KEY (%s)", pkTableName, pkName, pkColumnName)
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
    public String[] getSqls4AddingField(String aSchemaName, String aTableName, JdbcColumn aField) {
        String fullTableName = makeFullName(aSchemaName, aTableName);
        return new String[]{
            String.format(SqlDriver.ADD_FIELD_SQL_PREFIX, fullTableName) + getSql4FieldDefinition(aField)
        };
    }

    @Override
    public TwinString[] getCharsForWrap() {
        return charsForWrap;
    }

    @Override
    public char[] getRestrictedChars() {
        return restrictedChars;
    }

    @Override
    public boolean isHadWrapped(String aName) {
        return false;
    }

    private String prepareName(String aName) {
        return (isWrappedName(aName) ? unwrapName(aName) : aName);
    }

    private static final Set<String> numericTypes = new HashSet<>(Arrays.asList(new String[]{
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
    }));

    private static boolean isNumeric(String aType) {
        return numericTypes.contains(aType.toUpperCase());
    }

    @Override
    public JdbcChangeValue convertGeometry(String aValue, Connection aConnection) throws SQLException {
        return null;
    }

    @Override
    public String readGeometry(Wrapper aRs, int aColumnIndex, Connection aConnection) throws SQLException {
        return null;
    }
}
