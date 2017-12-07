package com.septima.sqldrivers.resolvers;

import com.septima.metadata.JdbcColumn;

import java.util.Set;

/**
 * Resolver incapsulates functionality, involved in fields types resolving
 * from/to RDBMS friendly form.
 *
 * @author mg
 */
public interface TypesResolver {

    public String toApplicationType(int aJdbcType, String aRDBMSType);
    
    public Set<String> getSupportedTypes();
    
    public boolean isSized(String aTypeName);

    public boolean isScaled(String aTypeName);
    
    public void resolveSize(JdbcColumn aField);

}
