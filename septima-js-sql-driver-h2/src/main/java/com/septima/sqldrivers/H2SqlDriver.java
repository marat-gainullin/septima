package com.septima.sqldrivers;

import com.septima.Constants;
import com.septima.changes.NamedJdbcValue;
import com.septima.metadata.*;
import com.septima.metadata.ForeignKey;
import com.septima.sqldrivers.resolvers.H2TypesResolver;
import com.septima.sqldrivers.resolvers.TypesResolver;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.ArrayList;
import java.util.List;

/**
 * @author vv
 */
public class H2SqlDriver extends SqlDriver {

    // настройка экранирования наименования объектов БД
    private static final Escape ESCAPE = new Escape("`", "`");

    protected TypesResolver resolver = new H2TypesResolver();
    protected static final int[] h2ErrorCodes = {};
    protected static final String[] platypusErrorMessages = {};
    protected static final String SET_SCHEMA_CLAUSE = "SET SCHEMA %s";
    protected static final String GET_SCHEMA_CLAUSE = "SELECT SCHEMA()";
    protected static final String CREATE_SCHEMA_CLAUSE = "CREATE SCHEMA IF NOT EXISTS %s";
    protected static final String SQL_CREATE_EMPTY_TABLE = "CREATE TABLE %s (%s DECIMAL(18,0) NOT NULL PRIMARY KEY)";
    protected static final String SQL_CREATE_TABLE_COMMENT = "COMMENT ON TABLE %s IS '%s'";
    protected static final String SQL_CREATE_COLUMN_COMMENT = "COMMENT ON COLUMN %s IS '%s'";
    protected static final String SQL_DROP_TABLE = "DROP TABLE %s";
    protected static final String SQL_CREATE_INDEX = "CREATE %s INDEX %s ON %s (%s)";
    protected static final String SQL_DROP_INDEX = "DROP INDEX %s";
    protected static final String SQL_ADD_PK = "ALTER TABLE %s ADD %s PRIMARY KEY (%s)";
    protected static final String SQL_DROP_PK = "ALTER TABLE %s DROP PRIMARY KEY";
    protected static final String SQL_ADD_FK = "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) %s";
    protected static final String SQL_DROP_FK = "ALTER TABLE %s DROP CONSTRAINT %s";
    protected static final String SQL_PARENTS_LIST = ""
            + "WITH RECURSIVE parents(mdent_id, mdent_parent_id) AS "
            + "( "
            + "SELECT m1.mdent_id, m1.mdent_parent_id FROM mtd_entities m1 WHERE m1.mdent_id = %s "
            + "    UNION ALL "
            + "SELECT m2.mdent_id, m2.mdent_parent_id FROM parents p, mtd_entities m2 WHERE m2.mdent_id = p.mdent_parent_id "
            + ") "
            + "SELECT mdent_id, mdent_parent_id FROM parents";
    protected static final String SQL_CHILDREN_LIST = ""
            + "WITH recursive children(mdent_id, mdent_name, mdent_parent_id, mdent_type, mdent_content_txt, mdent_content_txt_size, mdent_content_txt_crc32) AS"
            + "( "
            + "SELECT m1.mdent_id, m1.mdent_name, m1.mdent_parent_id, m1.mdent_type, m1.mdent_content_txt, m1.mdent_content_txt_size, m1.mdent_content_txt_crc32 FROM mtd_entities m1 WHERE m1.mdent_id = :%s "
            + "    union all "
            + "SELECT m2.mdent_id, m2.mdent_name, m2.mdent_parent_id, m2.mdent_type, m2.mdent_content_txt, m2.mdent_content_txt_size, m2.mdent_content_txt_crc32 FROM children c, mtd_entities m2 WHERE c.mdent_id = m2.mdent_parent_id "
            + ") "
            + "SELECT mdent_id, mdent_name, mdent_parent_id, mdent_type, mdent_content_txt, mdent_content_txt_size, mdent_content_txt_crc32 FROM children";
    protected static final String SQL_RENAME_COLUMN = "ALTER TABLE %s ALTER COLUMN %s RENAME TO %s";
    protected static final String SQL_CHANGE_COLUMN_TYPE = "ALTER TABLE %s ALTER COLUMN %s %s";
    protected static final String SQL_CHANGE_COLUMN_NULLABLE = "ALTER TABLE %s ALTER COLUMN %s SET %s NULL";

    public H2SqlDriver() {
        super();
    }

    @Override
    public String getDialect() {
        return Constants.H2_DIALECT;
    }

    @Override
    public boolean is(String aDialect) {
        return Constants.H2_DIALECT.equals(aDialect);
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
    public String[] getSql4CreateColumnComment(String aOwnerName, String aTableName, String aFieldName, String aDescription) {
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
    public String getSql4DropIndex(String aSchemaName, String aTableName, String aIndexName) {
        String indexName = makeFullName(aSchemaName, aIndexName);
        return String.format(SQL_DROP_INDEX, indexName);
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
                    String.format(SQL_ADD_PK, pkTableName, "CONSTRAINT " + pkName, pkColumnName)
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
    public String getSql4CreateFkConstraint(String aSchemaName, ForeignKey aFk) {
        List<ForeignKey> fkList = new ArrayList<>();
        fkList.add(aFk);
        return getSql4CreateFkConstraint(aSchemaName, fkList);
    }

    /**
     * {@inheritDoc}
     */
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
                    fkRule += " ON UPDATE CASCADE";
                    break;
                case NOACTION:
//                case SETDEFAULT:
                    fkRule += " ON UPDATE NO ACTION";
                    break;
                case SETDEFAULT:
                    fkRule += " ON UPDATE SET DEFAULT";
                    break;
                case SETNULL:
                    fkRule += " ON UPDATE SET NULL";
                    break;
            }
            switch (fk.getDeleteRule()) {
                case CASCADE:
                    fkRule += " ON DELETE CASCADE";
                    break;
                case NOACTION:
//                case SETDEFAULT:
                    fkRule += " ON DELETE NO ACTION";
                    break;
                case SETDEFAULT:
                    fkRule += " ON DELETE SET DEFAULT";
                    break;
                case SETNULL:
                    fkRule += " ON DELETE SET NULL";
                    break;
            }
            return String.format(SQL_ADD_FK, fkTableName, fkName.isEmpty() ? "" : escapeNameIfNeeded(fkName), fkColumnName, pkTableName, pkColumnName, fkRule);
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSql4CreateIndex(String aSchemaName, String aTableName, TableIndex aIndex) {
        assert aIndex.getColumns().size() > 0 : "index definition must consist of at least 1 column";
        String tableName = makeFullName(aSchemaName, aTableName);
        String fieldsList = aIndex.getColumns().stream()
                .map(column -> new StringBuilder(escapeNameIfNeeded(column.getColumnName()))
                        .append(!column.isAscending() ? " DESC" : ""))
                .reduce((s1, s2) -> new StringBuilder()
                        .append(s1)
                        .append(", ")
                        .append(s2))
                .map(sb -> sb.toString())
                .orElse("");
        return String.format(SQL_CREATE_INDEX,
                (aIndex.isUnique() ? "UNIQUE " : "") + (aIndex.isHashed() ? "HASH " : ""),
                escapeNameIfNeeded(aIndex.getName()),
                tableName,
                fieldsList);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSql4EmptyTableCreation(String aSchemaName, String aTableName, String aPkFieldName) {
        String fullName = makeFullName(aSchemaName, aTableName);
        return String.format(SQL_CREATE_EMPTY_TABLE, fullName, escapeNameIfNeeded(aPkFieldName));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String parseException(Exception ex) {
        if (ex != null && ex instanceof SQLException) {
            SQLException sqlEx = (SQLException) ex;
            int errorCode = sqlEx.getErrorCode();
            for (int i = 0; i < h2ErrorCodes.length; i++) {
                if (errorCode == h2ErrorCodes[i]) {
                    return platypusErrorMessages[i];
                }
            }
        }
        return ex.getLocalizedMessage();
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
    public String[] getSqls4ModifyingField(String aSchemaName, String aTableName, JdbcColumn aOldFieldMd, JdbcColumn aNewFieldMd) {
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
    public String[] getSqls4RenamingField(String aSchemaName, String aTableName, String aOldFieldName, JdbcColumn aNewFieldMd) {
        String fullTableName = makeFullName(aSchemaName, aTableName);
        String renameSQL = String.format(SQL_RENAME_COLUMN, fullTableName, escapeNameIfNeeded(aOldFieldName), escapeNameIfNeeded(aNewFieldMd.getName()));
        return new String[]{renameSQL};
    }

    @Override
    public String[] getSqls4AddingField(String aSchemaName, String aTableName, JdbcColumn aField) {
        String fullTableName = makeFullName(aSchemaName, aTableName);
        return new String[]{
                String.format(SqlDriver.ADD_FIELD_SQL_PREFIX, fullTableName) + getSql4FieldDefinition(aField)
        };
    }

    @Override
    public Escape getEscape() {
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
