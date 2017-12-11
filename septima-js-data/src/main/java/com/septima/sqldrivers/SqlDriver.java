package com.septima.sqldrivers;

import com.septima.changes.NamedJdbcValue;
import com.septima.dataflow.StatementsGenerator;
import com.septima.metadata.PrimaryKey;
import com.septima.metadata.TableIndex;
import com.septima.metadata.JdbcColumn;
import com.septima.metadata.ForeignKey;
import com.septima.sqldrivers.resolvers.TypesResolver;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * @author mg
 */
public abstract class SqlDriver implements StatementsGenerator.GeometryConverter {

    protected static class Escape {

        private final String left;
        private final String right;

        Escape(String aLeft, String aRight) {
            left = aLeft;
            right = aRight;
        }

        public String getLeft() {
            return left;
        }

        public String getRight() {
            return right;
        }
    }

    static final String DROP_FIELD_SQL_PREFIX = "alter table %s drop column ";
    static final String ADD_FIELD_SQL_PREFIX = "alter table %s add ";

    static final String PKEY_NAME_SUFFIX = "_pk";

    public SqlDriver() {
        super();
    }

    public abstract String getDialect();

    /**
     * *
     * The database supports deferrable constraints to enable constrains check
     * on transaction commit.
     *
     * @return true if constraints is deferrable
     */
    public abstract boolean isConstraintsDeferrable();

    /**
     * *
     * Gets type resolver to convert SQL types to JDBC types and vice-versa.
     *
     * @return TypesResolver instance
     */
    public abstract TypesResolver getTypesResolver();

    public abstract String getSql4GetSchema();

    /**
     * Returns sql text for create new schema.
     *
     * @param aSchemaName schema name
     * @param aPassword   owner password, required for some databases (Oracle)
     * @return Sql text.
     */
    public abstract String getSql4CreateSchema(String aSchemaName, String aPassword);

    /**
     * *
     * Returns sql clause array to set column's comment. Eeach sql clause from
     * array executed consequentially
     *
     * @param aOwnerName   Schema name
     * @param aTableName   Table name
     * @param aFieldName   Column name
     * @param aDescription Comment
     * @return Sql texts array
     */
    public abstract String[] getSql4CreateColumnComment(String aOwnerName, String aTableName, String aFieldName, String aDescription);

    /**
     * *
     * Returns sql clause to set table's comment.
     *
     * @param aOwnerName   Schema name
     * @param aTableName   Table name
     * @param aDescription Comment
     * @return Sql text
     */
    public abstract String getSql4CreateTableComment(String aOwnerName, String aTableName, String aDescription);

    /**
     * *
     * Gets sql clause for dropping the table.
     *
     * @param aSchemaName Schema name
     * @param aTableName  Table name
     * @return sql text
     */
    public abstract String getSql4DropTable(String aSchemaName, String aTableName);

    /**
     * *
     * Gets sql clause for dropping the index on the table.
     *
     * @param aSchemaName Schema name
     * @param aTableName  Table name
     * @param aIndexName  Index name
     * @return sql text
     */
    public abstract String getSql4DropIndex(String aSchemaName, String aTableName, String aIndexName);

    /**
     * *
     * Gets sql clause for dropping the foreign key constraint.
     *
     * @param aSchemaName Schema name
     * @param aFk         Foreign key specification object
     * @return Sql text
     */
    public abstract String getSql4DropFkConstraint(String aSchemaName, ForeignKey aFk);

    /**
     * *
     * Gets sql clause for creating the primary key.
     *
     * @param aSchemaName Schema name
     * @param listPk      Primary key columns specifications list
     * @return Sql text
     */
    public abstract String[] getSql4CreatePkConstraint(String aSchemaName, List<PrimaryKey> listPk);

    /**
     * *
     * Gets sql clause for dropping the primary key.
     *
     * @param aSchemaName Schema name
     * @param aPk         Primary key specification
     * @return Sql text
     */
    public abstract String getSql4DropPkConstraint(String aSchemaName, PrimaryKey aPk);

    /**
     * *
     * Gets sql clause for creating the foreign key constraint.
     *
     * @param aSchemaName Schema name
     * @param aFk         Foreign key specification
     * @return Sql text
     */
    public abstract String getSql4CreateFkConstraint(String aSchemaName, ForeignKey aFk);

    /**
     * *
     * Gets sql clause for creating the foreign key constraint.
     *
     * @param aSchemaName Schema name
     * @param listFk      Foreign key columns specifications list
     * @return Sql text
     */
    public abstract String getSql4CreateFkConstraint(String aSchemaName, List<ForeignKey> listFk);

    /**
     * *
     * Gets sql clause for creating the index
     *
     * @param aSchemaName Schema name
     * @param aTableName  Table name
     * @param aIndex      Index specification
     * @return Sql text
     */
    public abstract String getSql4CreateIndex(String aSchemaName, String aTableName, TableIndex aIndex);

    /**
     * *
     * Gets sql clause for creating an empty table.
     *
     * @param aSchemaName  Schema name
     * @param aTableName   Table name
     * @param aPkFieldName Column name for primary key
     * @return Sql text
     */
    public abstract String getSql4EmptyTableCreation(String aSchemaName, String aTableName, String aPkFieldName);

    /**
     * *
     * Gets specific exception message.
     *
     * @param ex Exception
     * @return Exception message
     */
    public abstract String parseException(Exception ex);

    /**
     * Generates Sql string fragment for field definition, according to specific
     * features of particular database. If it meets any strange type, such
     * java.sql.Types.OTHER or java.sql.Types.STRUCT, it uses the field's type
     * name.
     *
     * @param aField A field information to deal with.
     * @return Sql string for field definition
     */
    public abstract String getSql4FieldDefinition(JdbcColumn aField);

    /**
     * Generates Sql string to modify a field, according to specific features of
     * particular database. If it meats any strange type, such
     * java.sql.Types.OTHER or java.sql.Types.STRUCT, it uses the field's type
     * name.
     *
     * @param aSchemaName Schema name
     * @param aTableName  Name of the table with that field
     * @param aField      A field information
     * @return Sql array string for field modification.
     */
    public abstract String[] getSqls4AddingField(String aSchemaName, String aTableName, JdbcColumn aField);

    /**
     * Generates sql texts array for dropping a field. Sql clauses from array
     * will execute consequentially
     *
     * @param aSchemaName Schema name
     * @param aTableName  Name of a table the field to dropped from.
     * @param aFieldName  Field name to drop
     * @return Sql string generted.
     */
    public String[] getSql4DroppingField(String aSchemaName, String aTableName, String aFieldName) {
        String fullTableName = escapeNameIfNeeded(aTableName);
        if (aSchemaName != null && !aSchemaName.isEmpty()) {
            fullTableName = escapeNameIfNeeded(aSchemaName) + "." + fullTableName;
        }
        return new String[]{
                String.format(DROP_FIELD_SQL_PREFIX, fullTableName) + escapeNameIfNeeded(aFieldName)
        };
    }

    /**
     * Generates Sql string to modify a field, according to specific features of
     * particular database. If it meats any strange type, such
     * java.sql.Types.OTHER or java.sql.Types.STRUCT, it uses the field's type
     * name.
     *
     * @param aSchemaName Schema name
     * @param aTableName  Name of the table with that field
     * @param aOldFieldMd A field information to migrate from.
     * @param aNewFieldMd A field information to migrate to.
     * @return Sql array string for field modification.
     */
    public abstract String[] getSqls4ModifyingField(String aSchemaName, String aTableName, JdbcColumn aOldFieldMd, JdbcColumn aNewFieldMd);

    /**
     * *
     * Generates Sql string to rename a field, according to specific features of
     * particular database.
     *
     * @param aSchemaName   Schema name
     * @param aTableName    Table name
     * @param aOldFieldName Old column name
     * @param aNewFieldMd   New field
     * @return Sql array string for field modification.
     */
    public abstract String[] getSqls4RenamingField(String aSchemaName, String aTableName, String aOldFieldName, JdbcColumn aNewFieldMd);

    public String makeFullName(String aSchemaName, String aName) {
        String name = escapeNameIfNeeded(aName);
        if (aSchemaName != null && !aSchemaName.isEmpty()) {
            name = escapeNameIfNeeded(aSchemaName) + "." + name;
        }
        return name;
    }

    protected String constructIn(Set<String> strings) {
        StringBuilder sb = new StringBuilder();
        String delimiter = "";
        for (String l : strings) {
            sb.append(delimiter).append("'").append(l.replaceAll("'", "''")).append("'");
            delimiter = ", ";
        }
        return sb.toString();
    }

    @Override
    public abstract NamedJdbcValue convertGeometry(String aValue, Connection aConnection) throws SQLException;

    public abstract String readGeometry(Wrapper aRs, int aColumnIndex, Connection aConnection) throws SQLException;

    abstract public Escape getEscape();

    protected boolean hasLowerCase(String aValue) {
        if (aValue != null) {
            for (char c : aValue.toCharArray()) {
                if (Character.isLowerCase(c)) {
                    return true;
                }
            }
        }
        return false;
    }

    protected boolean hasUpperCase(String aValue) {
        if (aValue != null) {
            for (char c : aValue.toCharArray()) {
                if (Character.isUpperCase(c)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Wrapping names containing restricted symbols.
     *
     * @param aName Name to wrap
     * @return Wrapped text
     */
    public String escapeNameIfNeeded(String aName) {
        return wrapName(aName, isEscapeNeeded(aName));
    }

    public String wrapName(String aName, boolean requiredOnly) {
        if (aName != null && !aName.isEmpty() && !isNameWrapped(aName) && requiredOnly) {
            Escape escape = getEscape();
            if (escape != null) {
                String wrapL = escape.getLeft();
                String wrapR = escape.getRight();
                StringBuilder sb = new StringBuilder();
                sb.append(wrapL);
                if (wrapL.length() == 1) {
                    if (wrapL.equals(wrapR)) {
                        sb.append(aName.replaceAll(wrapL, wrapL + wrapL));
                    } else {
                        sb.append(aName.replaceAll(wrapL, wrapL + wrapL).replaceAll(wrapR, wrapR + wrapR));
                    }
                } else {
                    sb.append(aName);
                }
                sb.append(wrapR);
                return sb.toString();
            }
        }
        return aName;
    }

    public String unescapeName(String aName) {
        int wrapLength = getWrapLength(aName);
        if (wrapLength > 0) {
            int length = aName.length();
            String left = aName.substring(0, wrapLength);
            String right = aName.substring(length - wrapLength);
            if (left.equals(right)) {
                return aName.substring(wrapLength, length - wrapLength).replaceAll(left + right, left);
            }
            return aName.substring(wrapLength, length - wrapLength);
        }
        return aName;
    }

    public abstract boolean is(String aDialect);

    public boolean isNameWrapped(String aName) {
        return getWrapLength(aName) > 0;
    }

    public int getWrapLength(String aName) {
        if (aName != null && !aName.isEmpty()) {
            Escape escape = getEscape();
            if (escape != null) {
                String left = escape.getLeft();
                String right = escape.getRight();
                if (aName.startsWith(left) && aName.endsWith(right)) {
                    return left.length();
                }
            }
        }
        return 0;
    }

    public boolean isEscapeNeeded(String aName) {
        return !Pattern.matches("^[_a-zA-Z][_a-zA-Z0-9]*", aName);
    }

    public String generatePkName(String aTableName, String aSuffix) {
        int wrapLength = getWrapLength(aTableName);
        StringBuilder sb = new StringBuilder();
        sb.append(aTableName.substring(0, aTableName.length() - wrapLength));
        sb.append(aSuffix);
        sb.append(aTableName.substring(aTableName.length() - wrapLength));
        return sb.toString();
    }
}
