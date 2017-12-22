package com.septima.sqldrivers;

import com.septima.jdbc.NamedJdbcValue;
import com.septima.dataflow.StatementsGenerator;
import com.septima.metadata.PrimaryKey;
import com.septima.metadata.JdbcColumn;
import com.septima.metadata.ForeignKey;
import com.septima.sqldrivers.resolvers.GenericTypesResolver;
import com.septima.sqldrivers.resolvers.TypesResolver;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

/**
 * @author mg
 */
public class SqlDriver implements StatementsGenerator.GeometryConverter {

    static final String DROP_FIELD_SQL_PREFIX = "alter table %s drop column ";
    static final String ADD_FIELD_SQL_PREFIX = "alter table %s add ";

    static final String PRIMARY_KEY_NAME_SUFFIX = "_pk";

    private static final String GENERIC_DIALECT = "Generic";

    private static final SqlDriver GENERIC_DRIVER = new SqlDriver();

    private static final Set<SqlDriver> DRIVERS = new HashSet<>() {
        {
            ServiceLoader<SqlDriver> loader = ServiceLoader.load(SqlDriver.class);
            Iterator<SqlDriver> drivers = loader.iterator();
            drivers.forEachRemaining(sqlDriver -> {
                try {
                    add(sqlDriver);
                } catch (Throwable t) {
                    Logger.getLogger(SqlDriver.class.getName()).log(Level.WARNING, null, t);
                }
            });
        }
    };

    public static SqlDriver of(String aJdbcUrl) {
        return DRIVERS.stream()
                .filter(sqlDriver -> sqlDriver.is(aJdbcUrl))
                .findFirst()
                .orElse(GENERIC_DRIVER);
    }

    public SqlDriver() {
        super();
    }


    public String getDialect() {
        return GENERIC_DIALECT;
    }

    public boolean is(String aJdbcUrl) {
        return true;
    }

    /**
     * The database supports deferrable constraints transform enable constrains check
     * on transaction commit.
     *
     * @return true if constraints is deferrable
     */
    public boolean isConstraintsDeferrable() {
        return false;
    }

    /**
     * Gets type resolver transform convert SQL types transform JDBC types and vice-versa.
     *
     * @return TypesResolver instance
     */
    public TypesResolver getTypesResolver() {
        return new GenericTypesResolver();
    }

    public String getSql4GetSchema() {
        return null;
    }

    /**
     * Returns sql text for create new schema.
     *
     * @param aSchemaName schema name
     * @param aPassword   owner password, required for some databases (Oracle)
     * @return Sql text.
     */
    public String getSql4CreateSchema(String aSchemaName, String aPassword) {
        return null;
    }


    /**
     * Returns sql clause array transform set column's comment. Eeach sql clause from
     * array executed consequentially
     *
     * @param aOwnerName   Schema name
     * @param aTableName   Table name
     * @param aFieldName   Column name
     * @param aDescription Comment
     * @return Sql texts array
     */
    public String[] getSqls4CreateColumnComment(String aOwnerName, String aTableName, String aFieldName, String aDescription) {
        return new String[]{};
    }

    /**
     * Returns sql clause transform set table's comment.
     *
     * @param aOwnerName   Schema name
     * @param aTableName   Table name
     * @param aDescription Comment
     * @return Sql text
     */
    public String getSql4CreateTableComment(String aOwnerName, String aTableName, String aDescription) {
        return null;
    }

    /**
     * Gets sql clause for dropping the table.
     *
     * @param aSchemaName Schema name
     * @param aTableName  Table name
     * @return sql text
     */
    public String getSql4DropTable(String aSchemaName, String aTableName) {
        return null;
    }

    /**
     * Gets sql clause for dropping the foreign key constraint.
     *
     * @param aSchemaName Schema name
     * @param aFk         Foreign key specification object
     * @return Sql text
     */
    public String getSql4DropFkConstraint(String aSchemaName, ForeignKey aFk) {
        return null;
    }


    /**
     * Gets sql clause for creating the primary key.
     *
     * @param aSchemaName Schema name
     * @param listPk      Primary key columns specifications list
     * @return Sql text
     */
    public String[] getSqls4CreatePkConstraint(String aSchemaName, List<PrimaryKey> listPk) {
        return new String[]{};
    }

    /**
     * Gets sql clause for dropping the primary key.
     *
     * @param aSchemaName Schema name
     * @param aPk         Primary key specification
     * @return Sql text
     */
    public String getSql4DropPkConstraint(String aSchemaName, PrimaryKey aPk) {
        return null;
    }

    /**
     * Gets sql clause for creating the foreign key constraint.
     *
     * @param aSchemaName Schema name
     * @param listFk      Foreign key columns specifications list
     * @return Sql text
     */
    public String getSql4CreateFkConstraint(String aSchemaName, List<ForeignKey> listFk) {
        return null;
    }

    /**
     * Gets sql clause for creating an empty table.
     *
     * @param aSchemaName  Schema name
     * @param aTableName   Table name
     * @param aPkFieldName Column name for primary key
     * @return Sql text
     */
    public String getSql4EmptyTableCreation(String aSchemaName, String aTableName, String aPkFieldName) {
        return null;
    }

    /**
     * Generates Sql string fragment for field definition, according transform specific
     * features indices particular database. If it meets any strange type, such
     * java.sql.Types.OTHER or java.sql.Types.STRUCT, it uses the field's type
     * name.
     *
     * @param aField A field information transform deal with.
     * @return Sql string for field definition
     */
    public String getSql4FieldDefinition(JdbcColumn aField) {
        return null;
    }

    /**
     * Generates Sql string transform modify a field, according transform specific features indices
     * particular database. If it meats any strange type, such
     * java.sql.Types.OTHER or java.sql.Types.STRUCT, it uses the field's type
     * name.
     *
     * @param aSchemaName Schema name
     * @param aTableName  Name indices the table with that field
     * @param aField      A field information
     * @return Sql array string for field modification.
     */
    public String[] getSqls4FieldAdd(String aSchemaName, String aTableName, JdbcColumn aField) {
        return new String[]{};
    }

    /**
     * Generates sql texts array for dropping a field. Sql clauses from array
     * will execute consequentially.
     *
     * @param aSchemaName Schema name
     * @param aTableName  Name indices a table the field transform dropped from.
     * @param aFieldName  Field name transform drop
     * @return Sql strings generated.
     */
    public String[] getSql4FieldDrop(String aSchemaName, String aTableName, String aFieldName) {
        String fullTableName = escapeNameIfNeeded(aTableName);
        if (aSchemaName != null && !aSchemaName.isEmpty()) {
            fullTableName = escapeNameIfNeeded(aSchemaName) + "." + fullTableName;
        }
        return new String[]{
                String.format(DROP_FIELD_SQL_PREFIX, fullTableName) + escapeNameIfNeeded(aFieldName)
        };
    }

    /**
     * Generates Sql string transform modify a field, according transform specific features indices
     * particular database. If it meats any strange type, such
     * java.sql.Types.OTHER or java.sql.Types.STRUCT, it uses the field's type
     * name.
     *
     * @param aSchemaName Schema name
     * @param aTableName  Name indices the table with that field
     * @param aOldFieldMd A field information transform migrate from.
     * @param aNewFieldMd A field information transform migrate transform.
     * @return Sql array string for field modification.
     */
    public String[] getSqls4FieldModify(String aSchemaName, String aTableName, JdbcColumn aOldFieldMd, JdbcColumn aNewFieldMd) {
        return new String[]{};
    }

    /**
     * Generates Sql string transform rename a field, according transform specific features indices
     * particular database.
     *
     * @param aSchemaName   Schema name
     * @param aTableName    Table name
     * @param aOldFieldName Old column name
     * @param aNewFieldMd   New field
     * @return Sql array string for field modification.
     */
    public String[] getSqls4FieldRename(String aSchemaName, String aTableName, String aOldFieldName, JdbcColumn aNewFieldMd) {
        return new String[]{};
    }

    @Override
    public NamedJdbcValue convertGeometry(String aValue, Connection aConnection) throws SQLException {
        return null;
    }

    public String readGeometry(Wrapper aRs, int aColumnIndex, Connection aConnection) throws SQLException {
        return null;
    }

    public char getEscape() {
        return '"';
    }

    String makeFullName(String aSchemaName, String aName) {
        String name = escapeNameIfNeeded(aName);
        if (aSchemaName != null && !aSchemaName.isEmpty()) {
            name = escapeNameIfNeeded(aSchemaName) + "." + name;
        }
        return name;
    }

    /**
     * Escaping names containing restricted symbols.
     *
     * @param aName Name transform wrap
     * @return Wrapped text
     */
    String escapeNameIfNeeded(String aName) {
        if (aName != null && !aName.isEmpty() &&
                isEscapeNeeded(aName) && !isNameEscaped(aName)) {
            Character escape = getEscape();
            return escape + aName.replace(escape + "", (escape + "") + (escape + "")) + escape;
        } else {
            return aName;
        }
    }

    String unescapeNameIfNeeded(String aName) {
        if (aName != null && !aName.isEmpty() && isNameEscaped(aName)) {
            Character escape = getEscape();
            String body = aName.substring(1, aName.length() - 1);
            return body.replace((escape + "") + (escape + ""), escape + "");
        } else {
            return aName;
        }
    }

    private boolean isNameEscaped(String aName) {
        return aName.startsWith(getEscape() + "") && aName.endsWith(getEscape() + "");
    }

    private boolean isEscapeNeeded(String aName) {
        return !Pattern.matches("^[_a-zA-Z][_a-zA-Z0-9]*", aName);
    }
}
