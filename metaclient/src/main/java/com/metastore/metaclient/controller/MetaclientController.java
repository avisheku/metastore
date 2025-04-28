package com.metastore.metaclient.controller;

import com.metastore.metaclient.model.Metadata;
import com.metastore.metaclient.service.MetaclientService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.HashMap;

@RestController
@RequestMapping("/metadata")
public class MetaclientController {
    private final MetaclientService metaclientService;

    @Autowired
    public MetaclientController(MetaclientService metaclientService) {
        this.metaclientService = metaclientService;
    }

    @GetMapping("/all")
    public ResponseEntity<List<Metadata>> getMetadataAll() {
        return ResponseEntity.ok(metaclientService.getAllMetadata());
    }

    @GetMapping("/{id}")
    public ResponseEntity<Metadata> getMetadata(@PathVariable UUID id) {
        return metaclientService.getMetadataById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping
    public ResponseEntity<List<Metadata>> createMetadata(@RequestBody List<Metadata> metadataList) {
        return ResponseEntity.ok(metaclientService.createMetadata(metadataList));
    }

    @PatchMapping("/{id}")
    public ResponseEntity<Metadata> updateMetadata(@PathVariable UUID id, @RequestBody Map<String, Object> updates) {
        try {
            
            if (updates == null || updates.isEmpty()) {
                return ResponseEntity.badRequest().build();
            }

            Metadata updatedMetadata = metaclientService.updateMetadata(id, updates);
            return ResponseEntity.ok(updatedMetadata);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteMetadata(@PathVariable UUID id) {
        metaclientService.deleteMetadata(id);
        return ResponseEntity.ok().build();
    }

    @GetMapping("/search")
    public ResponseEntity<List<Metadata>> searchMetadata(@RequestParam String keyword) {
        return ResponseEntity.ok(metaclientService.searchByKeyword(keyword));
    }

    @GetMapping("/types")
    public ResponseEntity<List<String>> getAllTypes() {
        return ResponseEntity.ok(metaclientService.getAllTypes());
    }

    @GetMapping("/issuers")
    public ResponseEntity<List<String>> getAllIssuers() {
        return ResponseEntity.ok(metaclientService.getAllIssuers());
    }

    @PostMapping("/load/test-data/{count}")
    public ResponseEntity<Map<String, String>> loadTestData(@PathVariable int count) {
        metaclientService.loadTestData(count);
        Map<String, String> response = new HashMap<>();
        response.put("message", "Successfully loaded " + count + " test records");
        return ResponseEntity.ok(response);
    }

    @PutMapping("/{id}")
    public ResponseEntity<Metadata> updateMetadataPut(@PathVariable UUID id, @RequestBody Map<String, Object> updates) {
        try {
            if (updates == null || updates.isEmpty()) {
                return ResponseEntity.badRequest().build();
            }

            Metadata updatedMetadata = metaclientService.updateMetadata(id, updates);
            return ResponseEntity.ok(updatedMetadata);
        } catch (IllegalArgumentException e) {
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            return ResponseEntity.internalServerError().build();
        }
    }
} 