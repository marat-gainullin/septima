package com.septima.sqldrivers;

import com.septima.Constants;
import com.septima.changes.JdbcChangeValue;
import com.septima.metadata.ForeignKey;
import com.septima.metadata.PrimaryKey;
import com.septima.metadata.TableIndex;
import com.septima.metadata.JdbcColumn;
import com.septima.sqldrivers.resolvers.GenericTypesResolver;
import com.septima.sqldrivers.resolvers.TypesResolver;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.List;

/**
 *
 * @author mg
 */
public class GenericSqlDriver extends SqlDriver {

    @Override
    public String getDialect() {
        return Constants.GENERIC_DIALECT;
    }

    @Override
    public boolean is(String aDialect) {
        return true;
    }

    @Override
    public boolean isConstraintsDeferrable() {
        return false;
    }

    @Override
    public TypesResolver getTypesResolver() {
        return new GenericTypesResolver();
    }

    @Override
    public String getUsersSpaceInitResourceName() {
        return null;
    }

    @Override
    public String getVersionInitResourceName() {
        return null;
    }

    @Override
    public void applyContextToConnection(Connection aConnection, String aSchema) throws Exception {
    }

    @Override
    public String getSql4GetConnectionContext() {
        return null;
    }

    @Override
    public String getSql4CreateSchema(String aSchemaName, String aPassword) {
        return null;
    }

    @Override
    public String[] getSql4CreateColumnComment(String aOwnerName, String aTableName, String aFieldName, String aDescription) {
        return null;
    }

    @Override
    public String getSql4CreateTableComment(String aOwnerName, String aTableName, String aDescription) {
        return null;
    }

    @Override
    public String getSql4DropTable(String aSchemaName, String aTableName) {
        return null;
    }

    @Override
    public String getSql4DropIndex(String aSchemaName, String aTableName, String aIndexName) {
        return null;
    }

    @Override
    public String getSql4DropFkConstraint(String aSchemaName, ForeignKey aFk) {
        return null;
    }

    @Override
    public String[] getSql4CreatePkConstraint(String aSchemaName, List<PrimaryKey> listPk) {
        return null;
    }

    @Override
    public String getSql4DropPkConstraint(String aSchemaName, PrimaryKey aPk) {
        return null;
    }

    @Override
    public String getSql4CreateFkConstraint(String aSchemaName, ForeignKey aFk) {
        return null;
    }

    @Override
    public String getSql4CreateFkConstraint(String aSchemaName, List<ForeignKey> listFk) {
        return null;
    }

    @Override
    public String getSql4CreateIndex(String aSchemaName, String aTableName, TableIndex aIndex) {
        return null;
    }

    @Override
    public String getSql4EmptyTableCreation(String aSchemaName, String aTableName, String aPkFieldName) {
        return null;
    }

    @Override
    public String parseException(Exception ex) {
        String res = ex.getLocalizedMessage();
        if (res == null) {
            res = ex.getMessage();
        }
        if (res == null) {
            res = ex.toString();
        }
        return res;
    }

    @Override
    public String getSql4FieldDefinition(JdbcColumn aField) {
        return null;
    }

    @Override
    public String[] getSqls4AddingField(String aSchemaName, String aTableName, JdbcColumn aField) {
        return null;
    }

    @Override
    public String[] getSqls4ModifyingField(String aSchemaName, String aTableName, JdbcColumn aOldFieldMd, JdbcColumn aNewFieldMd) {
        return null;
    }

    @Override
    public String[] getSqls4RenamingField(String aSchemaName, String aTableName, String aOldFieldName, JdbcColumn aNewFieldMd) {
        return null;
    }

    @Override
    public JdbcChangeValue convertGeometry(String aValue, Connection aConnection) throws SQLException {
        return null;
    }

    @Override
    public String readGeometry(Wrapper aRs, int aColumnIndex, Connection aConnection) throws SQLException {
        return null;
    }

    @Override
    public TwinString[] getCharsForWrap() {
        return null;
    }

    @Override
    public char[] getRestrictedChars() {
        return null;
    }

    @Override
    public boolean isHadWrapped(String aName) {
        return true;
    }

}
