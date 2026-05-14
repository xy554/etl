package com.xy.etl.cli.exception;

import org.springframework.boot.ExitCodeGenerator;

public class ConfigFileSyncException extends RuntimeException implements ExitCodeGenerator {

    public ConfigFileSyncException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    public int getExitCode() {
        return 1;
    }
}
