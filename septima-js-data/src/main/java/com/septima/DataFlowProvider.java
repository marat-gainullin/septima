package com.septima.client;

import com.septima.client.changes.JdbcChangeValue;
import com.septima.client.dataflow.JdbcFlowProvider;
import com.septima.client.dataflow.JdbcReader;
import com.septima.client.metadata.Fields;
import com.septima.client.metadata.Parameter;
import com.septima.client.sqldrivers.SqlDriver;
import com.septima.script.Scripts;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * This flow provider implements transaction capability for standard
 * JdbcFlowProvider. It enqueues changes in transactional queue instead of
 * actual writing to underlying database. It relies on transactional assumption:
 * all enqueued changes will be actually applied at commit or reverted at
 * rollback.
 *
 * @author mg
 */
public class SeptimaFlowProvider extends JdbcFlowProvider<String> {

    protected String entityName;
    protected Databases databases;
    protected Metadata metadata;
    protected SqlDriver sqlDriver;

    public SeptimaFlowProvider(Databases aClient, String aDataSourceName, String aEntityName, DataSource aDataSource, Consumer<Runnable> aDataPuller, Metadata aCache, String aClause, Fields aExpectedFields) throws Exception {
        super(aDataSourceName, aDataSource, aDataPuller, aClause, aExpectedFields);
        entityName = aEntityName;
        databases = aClient;
        metadata = aCache;
        sqlDriver = metadata.getDatasourceSqlDriver();
    }

    @Override
    public String getEntityName() {
        return entityName;
    }

    @Override
    protected JdbcReader obtainJdbcReader() {
        return new JdbcReader(expectedFields,
                (Wrapper aRsultSetOrCallableStatement, int aColumnIndex, Connection aConnection) -> {
            return sqlDriver.readGeometry(aRsultSetOrCallableStatement, aColumnIndex, aConnection);
        }, (int aJdbcType, String aRDBMSType) -> {
            return sqlDriver.getTypesResolver().toApplicationType(aJdbcType, aRDBMSType);
        });
    }
    
    @Override
    protected int assignParameter(Parameter aParameter, PreparedStatement aStatement, int aParameterIndex, Connection aConnection) throws SQLException {
        if (Scripts.GEOMETRY_TYPE_NAME.equals(aParameter.getType())) {
            try {
                JdbcChangeValue jv = sqlDriver.convertGeometry(aParameter.getValue().toString(), aConnection);
                Object paramValue = jv.value;
                int jdbcType = jv.jdbcType;
                String sqlTypeName = jv.sqlTypeName;
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
        if (Scripts.GEOMETRY_TYPE_NAME.equals(aParameter.getType())) {
            try {
                String sGeometry = sqlDriver.readGeometry(aStatement, aParameterIndex, aConnection);
                aParameter.setValue(sGeometry);
            } catch (Exception ex) {
                Logger.getLogger(SeptimaFlowProvider.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else {
            super.acceptOutParameter(aParameter, aStatement, aParameterIndex, aConnection);
        }
    }
}
