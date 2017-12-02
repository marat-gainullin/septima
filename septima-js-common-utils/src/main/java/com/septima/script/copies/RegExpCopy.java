package com.septima.script.copies;

/**
 *
 * @author mg
 */
public class RegExpCopy {

    protected String source;
    protected String flags;

    public RegExpCopy(String aSource, String aFlags) {
        super();
        source = aSource;
        flags = aFlags;
    }

    public String getSource() {
        return source;
    }

    public String getFlags() {
        return flags;
    }
    
}
