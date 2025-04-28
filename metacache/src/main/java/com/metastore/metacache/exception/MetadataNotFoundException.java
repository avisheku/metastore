package com.metastore.metacache.exception;

public class MetadataNotFoundException extends RuntimeException {
    public MetadataNotFoundException(String message) {
        super(message);
    }
} 