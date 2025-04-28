package com.metastore.metacache.controller;

import com.metastore.metacache.model.Metadata;
import com.metastore.metacache.model.MetadataResponse;
import com.metastore.metacache.service.MetacacheService;
import com.metastore.metacache.exception.MetadataNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/metadata")
public class MetacacheController {

    private static final Logger logger = LoggerFactory.getLogger(MetacacheController.class);
    private final MetacacheService metacacheService;

    @Autowired
    public MetacacheController(MetacacheService metacacheService) {
        this.metacacheService = metacacheService;
    }

    @GetMapping("/{id}")
    public ResponseEntity<MetadataResponse> getMetadata(@PathVariable UUID id) {
        try {
            MetadataResponse response = metacacheService.getMetadata(id);
            return ResponseEntity.ok(response);
        } catch (MetadataNotFoundException e) {
            logger.warn("Metadata not found: {}", e.getMessage());
            return ResponseEntity.notFound().build();
        } catch (Exception e) {
            logger.error("Error getting metadata: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/all")
    public ResponseEntity<MetadataResponse> getAllMetadata() {
        try {
            MetadataResponse response = metacacheService.getAllMetadata();
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Error getting all metadata: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    @PostMapping
    public ResponseEntity<List<Metadata>> createMetadata(@RequestBody List<Metadata> metadataList) {
        try {
            List<Metadata> createdMetadata = metacacheService.createMetadata(metadataList);
            return ResponseEntity.ok(createdMetadata);
        } catch (Exception e) {
            logger.error("Error creating metadata: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    @PatchMapping("/{id}")
    public ResponseEntity<Metadata> updateMetadata(@PathVariable UUID id, @RequestBody Map<String, Object> updates) {
        try {
            if (updates == null || updates.isEmpty()) {
                return ResponseEntity.badRequest().build();
            }

            Metadata updatedMetadata = metacacheService.updateMetadata(id, updates);
            return ResponseEntity.ok(updatedMetadata);
        } catch (MetadataNotFoundException e) {
            logger.warn("Metadata not found: {}", e.getMessage());
            return ResponseEntity.notFound().build();
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid update request: {}", e.getMessage());
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            logger.error("Error updating metadata: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteMetadata(@PathVariable UUID id) {
        try {
            metacacheService.deleteMetadata(id);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            logger.error("Error deleting metadata: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/search")
    public ResponseEntity<MetadataResponse> searchMetadata(@RequestParam String keyword) {
        try {
            MetadataResponse response = metacacheService.searchMetadata(keyword);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Error searching metadata: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/types")
    public ResponseEntity<MetadataResponse> getAllTypes() {
        try {
            MetadataResponse response = metacacheService.getAllTypes();
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Error getting types: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    @GetMapping("/issuers")
    public ResponseEntity<MetadataResponse> getAllIssuers() {
        try {
            MetadataResponse response = metacacheService.getAllIssuers();
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Error getting issuers: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }

    @PostMapping("/load/test-data/{count}")
    public ResponseEntity<String> loadTestData(@PathVariable int count) {
        try {
            String result = metacacheService.loadTestData(count);
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            logger.error("Error loading test data: {}", e.getMessage());
            return ResponseEntity.internalServerError().build();
        }
    }
} 