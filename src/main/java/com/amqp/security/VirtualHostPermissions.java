package com.amqp.security;

import java.util.regex.Pattern;

public class VirtualHostPermissions {
    private final Pattern configurePattern;
    private final Pattern writePattern;
    private final Pattern readPattern;

    public VirtualHostPermissions(String configureRegex, String writeRegex, String readRegex) {
        this.configurePattern = compilePattern(configureRegex);
        this.writePattern = compilePattern(writeRegex);
        this.readPattern = compilePattern(readRegex);
    }

    private Pattern compilePattern(String regex) {
        if (regex == null || regex.isEmpty()) {
            return Pattern.compile("^$"); // Match nothing
        }
        return Pattern.compile(regex);
    }

    public boolean canConfigure(String resource) {
        return configurePattern.matcher(resource).matches();
    }

    public boolean canWrite(String resource) {
        return writePattern.matcher(resource).matches();
    }

    public boolean canRead(String resource) {
        return readPattern.matcher(resource).matches();
    }

    public boolean hasPermission(Permission permission, String resource) {
        return switch (permission) {
            case CONFIGURE -> canConfigure(resource);
            case WRITE -> canWrite(resource);
            case READ -> canRead(resource);
        };
    }

    public String getConfigurePattern() {
        return configurePattern.pattern();
    }

    public String getWritePattern() {
        return writePattern.pattern();
    }

    public String getReadPattern() {
        return readPattern.pattern();
    }

    @Override
    public String toString() {
        return String.format("Permissions{configure='%s', write='%s', read='%s'}",
                           configurePattern.pattern(), writePattern.pattern(), readPattern.pattern());
    }
}
