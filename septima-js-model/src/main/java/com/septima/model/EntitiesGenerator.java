package com.septima.model;

import com.septima.GenericType;
import com.septima.entities.SqlEntities;
import com.septima.entities.SqlEntity;
import com.septima.jdbc.UncheckedSQLException;
import com.septima.metadata.EntityField;
import net.sf.jsqlparser.UncheckedJSqlParserException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class EntitiesGenerator {

    private final SqlEntities entities;
    private final Path source;
    private final Path destination;
    private final String indent;
    private final String lf;
    private final Charset charset;

    public EntitiesGenerator(SqlEntities anEntities, Path aDestination) {
        this(anEntities, anEntities.getApplicationPath(), aDestination, "    ", System.getProperty("line.separator"), StandardCharsets.UTF_8);
    }

    public EntitiesGenerator(SqlEntities anEntities, Path aSource, Path aDestination, String aIndent, String aLf, Charset aCharset) {
        entities = anEntities;
        source = aSource;
        destination = aDestination;
        indent = aIndent;
        lf = aLf;
        charset = aCharset;
    }

    public int generateRows() throws IOException {
        return Files.walk(source)
                .filter(path -> path.toString().toLowerCase().endsWith(".sql"))
                .map(path -> {
                    Path entityPath = entities.getApplicationPath().relativize(path);
                    Path entityDirPath = entityPath.getParent();
                    String entityPathName = entityPath.toString().replace('\\', '/');
                    String entityRef = entityPathName.substring(0, entityPathName.length() - 4);
                    try {
                        SqlEntity entity = entities.loadEntity(entityRef);
                        StringBuilder propertiesFields = entity.getFields().values().stream()
                                .sorted(Comparator.comparing(EntityField::getName))
                                .map(f ->
                                        new StringBuilder(indent).append("private").append(" ").append(javaType(f)).append(" ").append(property(f)).append(";").append(lf))
                                .reduce(StringBuilder::append)
                                .orElse(new StringBuilder());
                        StringBuilder propertiesGettersMutators = entity.getFields().values().stream()
                                .sorted(Comparator.comparing(EntityField::getName))
                                .map(f -> new StringBuilder(indent).append("public").append(" ").append(javaType(f)).append(" ").append("get").append(accessor(f)).append("() {").append(lf)
                                        .append(indent).append(indent).append("return").append(" ").append(property(f)).append(";").append(lf)
                                        .append(indent).append("}").append(lf)
                                        .append(lf)
                                        .append(indent).append("public").append(" void ").append("set").append(accessor(f)).append("(").append(javaType(f)).append(" ").append(mutatorArg(f)).append(") {").append(lf)
                                        .append(indent).append(indent).append(javaType(f)).append(" ").append("old").append(" = ").append(property(f)).append(";").append(lf)
                                        .append(indent).append(indent).append(property(f)).append(" = ").append(mutatorArg(f)).append(";").append(lf)
                                        .append(indent).append(indent).append("changeSupport.firePropertyChange(").append("\"").append(f.getName()).append("\"").append(", ").append("old").append(", ").append(property(f)).append(");").append(lf)
                                        .append(indent).append("}").append(lf)
                                        .append(lf)
                                )
                                .reduce(StringBuilder::append)
                                .orElse(new StringBuilder());
                        String entityPathLastName = entityPath.getName(entityPath.getNameCount() - 1).toString();
                        StringBuilder entityRow = new StringBuilder()
                                .append("package").append(" ").append(entityDirPath.toString().replace('\\', '/').replace('/', '.')).append(";").append(lf)
                                .append(lf)
                                .append("import").append(" ").append("com.septima.model.Observable").append(";").append(lf)
                                .append(entity.getFields().values().stream().anyMatch(f-> GenericType.DATE == f.getType()) ? new StringBuilder().append("import").append(" ").append("java.util.Date").append(";").append(lf) : "")
                                .append(lf)
                                .append("public").append(" ").append("class").append(" ")
                                .append(entityRow(entityPathLastName.substring(0, entityPathLastName.length() - 4)))
                                .append(" ").append("extends").append(" ").append("Observable").append(" ").append("{").append(lf)
                                .append(lf)
                                .append(propertiesFields)
                                .append(lf)
                                .append(propertiesGettersMutators)
                                .append("}")
                                .append(lf);
                        Path entityClassFile = destination.resolve(entityDirPath.resolve(entityRow(entityPathLastName.substring(0, entityPathLastName.length() - 4)).append(".java").toString()));
                        if (!entityClassFile.getParent().toFile().exists()) {
                            entityClassFile.getParent().toFile().mkdirs();
                        }
                        Files.write(entityClassFile, entityRow.toString().getBytes(charset));
                        return 1;
                    } catch (UncheckedSQLException | UncheckedJSqlParserException | IOException ex) {
                        Logger.getLogger(EntitiesGenerator.class.getName()).log(Level.SEVERE, "Entity '" + entityRef + "' skipped due to an exception", ex);
                        return 0;
                    }
                })
                .reduce(Integer::sum)
                .orElse(0);
    }

    private static String javaType(EntityField aField) {
        if (aField.getType() != null) {
            switch (aField.getType()) {
                case STRING:
                    return "String";
                case LONG:
                    return aField.isNullable() ? "Long" : "long";
                case DOUBLE:
                    return aField.isNullable() ? "Double" : "double";
                case DATE:
                    return "Date";
                case BOOLEAN:
                    return aField.isNullable() ? "Boolean" : "boolean";
                case GEOMETRY:
                    return "String";
                default:
                    return "String";
            }
        } else {
            return "String";
        }
    }

    private static StringBuilder capitalize(String aValue, String stopAt) {
        return Stream.of(aValue.split(stopAt))
                .map(part -> new StringBuilder(part.substring(0, 1).toUpperCase() + part.substring(1)))
                .reduce(StringBuilder::append)
                .orElse(new StringBuilder());
    }

    private static String sanitize(String aValue) {
        return sanitize(aValue, "_");
    }

    private static String sanitize(String aValue, String replaceWith) {
        return aValue.replaceAll("[^0-9a-zA-Z_]", replaceWith);
    }

    private static StringBuilder accessor(EntityField field) {
        return entity(field.getName());
    }

    private static StringBuilder entity(String name) {
        return capitalize(sanitize(name), "_+");
    }

    private static StringBuilder entityRow(String rowName) {
        return entity(rowName).append("Row");
    }

    private static StringBuilder mutatorArg(EntityField field) {
        return new StringBuilder("a").append(accessor(field));
    }

    private static StringBuilder property(EntityField field) {
        String entityName = entity(field.getName()).toString();
        return new StringBuilder(entityName.substring(0, 1).toLowerCase() + entityName.substring(1));
    }

}
