package com.septima.dataflow;

import com.septima.Parameter;
import com.septima.sqldrivers.SqlDriver;

import java.sql.*;

public interface StatementResultSetHandler {

    int assignInParameter(Parameter aParameter, PreparedStatement aStatement, int aParameterIndex, Connection aConnection) throws SQLException;

    void acceptOutParameter(Parameter aParameter, CallableStatement aStatement, int aParameterIndex, Connection aConnection) throws SQLException;

    Object readTypedValue(Wrapper aRs, int aColumnIndex) throws SQLException;

    SqlDriver getSqlDriver();
}
