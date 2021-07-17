package com.septima.generator;

import com.septima.GenericType;
import com.septima.metadata.EntityField;
import com.septima.metadata.Parameter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Utils {

    public static class ModelField {
        private final String propertyType;
        private final String property;
        private final String propertyGetter;
        private final String propertyMutator;
        private final String fieldName;
        private final GenericType genericType;

        public ModelField(EntityField field) {
            propertyType = Utils.javaType(field);
            String accessor = Utils.toPascalCase(field.getName());
            propertyGetter = "get" + accessor;
            propertyMutator = "set" + accessor;
            property = accessor.substring(0, 1).toLowerCase() + accessor.substring(1);
            fieldName = field.getName();
            genericType = field.getType();
        }

        public String getPropertyType() {
            return propertyType;
        }

        public String getProperty() {
            return property;
        }

        public String getPropertyGetter() {
            return propertyGetter;
        }

        public String getPropertyMutator() {
            return propertyMutator;
        }

        public String getFieldName() {
            return fieldName;
        }

        public GenericType getGenericType() {
            return genericType;
        }
    }

    public static String javaType(EntityField aField) {
        return javaType(aField, false);
    }

    public static String javaType(EntityField aField, boolean aBoxed) {
        if (aField.getType() != null) {
            switch (aField.getType()) {
                case STRING:
                    return "String";
                case LONG:
                    return aField.isNullable() || aBoxed ? "Long" : "long";
                case DOUBLE:
                    return aField.isNullable() || aBoxed ? "Double" : "double";
                case DATE:
                    return "Date";
                case BOOLEAN:
                    return aField.isNullable() || aBoxed ? "Boolean" : "boolean";
                case GEOMETRY:
                    return "String";
                default:
                    return "String";
            }
        } else {
            return "String";
        }
    }

    public static String javaType(Parameter aParameter) {
        if (aParameter.getType() != null) {
            switch (aParameter.getType()) {
                case STRING:
                    return "String";
                case LONG:
                    return "Long";
                case DOUBLE:
                    return "Double";
                case DATE:
                    return "java.time.Instant";
                case BOOLEAN:
                    return "Boolean";
                case GEOMETRY:
                    return "String";
                default:
                    return "String";
            }
        } else {
            return "String";
        }
    }
    public static String toPascalCase(String name) {
        return Stream.of(name.replaceAll("[^0-9a-zA-Z_]", "_").split("_+"))
                .filter(part -> !part.isEmpty())
                .map(part -> new StringBuilder(part.substring(0, 1).toUpperCase() + part.substring(1)))
                .reduce(StringBuilder::append)
                .orElse(new StringBuilder())
                .toString();
    }

    public static String rawClass(String name) {
        return toPascalCase(name) + "Row";
    }

    public static String loadResource(String resourceName, Charset aCharset, String lf) throws IOException {
        try (BufferedReader buffered = new BufferedReader(new InputStreamReader(EntitiesRaws.class.getResourceAsStream(resourceName), aCharset))) {
            return buffered.lines().collect(Collectors.joining(lf, "", lf));
        }
    }

    private static final Pattern VAR_PATTERN = Pattern.compile("\\$\\{([a-zA-Z0-9_]+)\\}");

    public static StringBuilder replaceVariables(String aBody, Map<String, String> aVariables, String lf) {
        StringBuilder body = new StringBuilder();
        Matcher matcher = VAR_PATTERN.matcher(aBody);
        while (matcher.find()) {
            if (!aVariables.containsKey(matcher.group(1))) {
                throw new IllegalStateException("Unbound variable '" + matcher.group(1) + "' in template:" + lf + aBody);
            }
            matcher.appendReplacement(body, aVariables.get(matcher.group(1)));
        }
        matcher.appendTail(body);
        return body;
    }
}
