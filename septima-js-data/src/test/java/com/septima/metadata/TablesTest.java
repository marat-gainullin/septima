package com.septima.metadata;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

public class TablesTest {

    @BeforeClass
    public static void setupDataSource() throws NamingException {
        InitialContext ctx = new InitialContext();
        DataSource dataSource = new JdbcDataSource();
        ctx.bind("h2", dataSource);
    }


    @Test
    public void easSchema() {
    }

    @Test
    public void hrSchema() {
    }

    @Test
    public void defaultSchema() {
    }
}
