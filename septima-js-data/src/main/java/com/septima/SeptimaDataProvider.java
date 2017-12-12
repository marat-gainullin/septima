package com.septima;

import com.septima.changes.NamedJdbcValue;
import com.septima.dataflow.JdbcDataProvider;
import com.septima.dataflow.ResultSetReader;
import com.septima.metadata.Field;
import com.septima.metadata.Parameter;
import com.septima.sqldrivers.SqlDriver;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * This flow provider implements transaction capability for standard
 * JdbcDataProvider. It enqueues changes in transactional queue instead of
 * actual writing to underlying database. It relies on transactional assumption:
 * all enqueued changes will be actually applied at commit or reverted at
 * rollback.
 *
 * @author mg
 */
public class SeptimaDataProvider extends JdbcDataProvider<String> {

    private final String entityName;
    private final SqlDriver sqlDriver;

    SeptimaDataProvider(SqlDriver aSqlDriver, String aEntityName, DataSource aDataSource, Consumer<Runnable> aDataPuller, String aClause, Map<String, Field> aExpectedFields) {
        super(aDataSource, aDataPuller, aClause, aExpectedFields);
        entityName = aEntityName;
        sqlDriver = aSqlDriver;
    }

    @Override
    public String getEntityName() {
        return entityName;
    }

    @Override
    protected ResultSetReader obtainJdbcReader() {
        return new ResultSetReader(expectedFields,
                sqlDriver::readGeometry,
                sqlDriver.getTypesResolver()::toApplicationType
        );
    }

    @Override
    protected int assignParameter(Parameter aParameter, PreparedStatement aStatement, int aParameterIndex, Connection aConnection) throws SQLException {
        if (ApplicationTypes.GEOMETRY_TYPE_NAME.equals(aParameter.getType())) {
            try {
                NamedJdbcValue jv = sqlDriver.convertGeometry(aParameter.getValue().toString(), aConnection);
                Object paramValue = jv.getValue();
                int jdbcType = jv.getJdbcType();
                String sqlTypeName = jv.getSqlTypeName();
                int assignedJdbcType = assign(paramValue, aParameterIndex, aStatement, jdbcType, sqlTypeName);
                checkOutParameter(aParameter, aStatement, aParameterIndex, jdbcType);
                return assignedJdbcType;
            } catch (Exception ex) {
                throw new SQLException(ex);
            }
        } else {
            return super.assignParameter(aParameter, aStatement, aParameterIndex, aConnection);
        }
    }

    @Override
    protected void acceptOutParameter(Parameter aParameter, CallableStatement aStatement, int aParameterIndex, Connection aConnection) throws SQLException {
        if (ApplicationTypes.GEOMETRY_TYPE_NAME.equals(aParameter.getType())) {
            try {
                String sGeometry = sqlDriver.readGeometry(aStatement, aParameterIndex, aConnection);
                aParameter.setValue(sGeometry);
            } catch (Exception ex) {
                Logger.getLogger(SeptimaDataProvider.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else {
            super.acceptOutParameter(aParameter, aStatement, aParameterIndex, aConnection);
        }
    }
}
