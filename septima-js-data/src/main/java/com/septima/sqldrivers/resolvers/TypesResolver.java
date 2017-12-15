package com.septima.sqldrivers.resolvers;

import java.util.Set;

/**
 * Resolver incapsulates functionality, involved in fields types resolving
 * from/to RDBMS friendly form.
 *
 * @author mg
 */
public interface TypesResolver {

    String toApplicationType(int aJdbcType, String aRdbmsTypeName);
    
    Set<String> getSupportedTypes();
    
    boolean isSized(String aTypeName);

    boolean isScaled(String aTypeName);
    
    int resolveSize(String aRdbmsTypeName, int aSize);

}
