/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
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
