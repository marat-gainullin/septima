package com.septima.http;

/**
 *
 * @author mg
 */
public class RpcPoint {

    private final String moduleName;
    private final String methodName;

    public RpcPoint(String aModuleName, String aMethodName) {
        super();
        moduleName = aModuleName;
        methodName = aMethodName;
    }

    public String getModuleName() {
        return moduleName;
    }

    public String getMethodName() {
        return methodName;
    }

}
