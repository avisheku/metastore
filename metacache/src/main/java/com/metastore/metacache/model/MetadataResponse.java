package com.metastore.metacache.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import java.time.Duration;

@Data
public class MetadataResponse {
    @JsonProperty("data")
    private Object data;
    
    @JsonProperty("timeTakenMs")
    private double timeTakenMs;
    
    @JsonProperty("source")
    private String source;

    public MetadataResponse() {
    }

    public MetadataResponse(Object data, Duration timeTaken, String source) {
        this.data = data;
        this.timeTakenMs = timeTaken.toNanos() / 1_000_000.0;
        this.source = source;
    }
} 