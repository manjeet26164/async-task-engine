package com.engine.taskflow.exception;

public class DuplicateJobException extends RuntimeException {

    public DuplicateJobException(String message) {
        super(message);
    }

    public DuplicateJobException(String idempotencyKey, String message) {
        super("Duplicate job detected for idempotency key [" + idempotencyKey + "]: " + message);
    }
}
