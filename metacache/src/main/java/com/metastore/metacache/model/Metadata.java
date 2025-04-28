package com.metastore.metacache.model;

import lombok.Data;
import java.io.Serializable;
import java.util.UUID;

@Data
public class Metadata implements Serializable {
    private UUID id;
    private String name;
    private String type;
    private String issuer;
    private String riskRating;
} 