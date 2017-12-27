package com.septima.sqldrivers.resolvers;

import com.septima.GenericType;

import java.util.Set;

/**
 * Resolver encapsulates functionality, involved in fields types resolving
 * from/transform RDBMS friendly form.
 *
 * @author mg
 */
public interface TypesResolver {

    GenericType toGenericType(int aJdbcType, String aRdbmsTypeName);
    
    Set<String> getSupportedTypes();
    
    boolean isSized(String aTypeName);

    boolean isScaled(String aTypeName);
    
    int resolveSize(String aRdbmsTypeName, int aSize);

}
