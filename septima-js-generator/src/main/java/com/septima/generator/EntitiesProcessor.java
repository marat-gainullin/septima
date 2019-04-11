package com.septima.generator;

import com.septima.entities.SqlEntities;

import java.nio.charset.Charset;
import java.nio.file.Path;

public class EntitiesProcessor {

    protected final SqlEntities entities;
    protected final Path destination;
    protected final String lf;
    protected final Charset charset;

    protected EntitiesProcessor(SqlEntities anEntities, Path aDestination,
                          String aLf, Charset aCharset) {
        entities = anEntities;
        destination = aDestination;
        lf = aLf;
        charset = aCharset;
    }
}
