package com.metastore.metaclient.service;

import com.metastore.metaclient.model.Metadata;
import com.metastore.metaclient.repository.MetaclientRepository;
import com.metastore.metaclient.util.DataLoader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Service
public class MetaclientService {
    private static final int BATCH_SIZE = 100;
    
    private final MetaclientRepository metaclientRepository;
    @Autowired
    private DataLoader dataLoader;

    @Autowired
    public MetaclientService(MetaclientRepository metaclientRepository) {
        this.metaclientRepository = metaclientRepository;
    }

    public List<Metadata> getAllMetadata() {
        return metaclientRepository.findAll();
    }

    public Optional<Metadata> getMetadataById(UUID id) {
        return metaclientRepository.findById(id);
    }

    @Transactional
    public List<Metadata> createMetadata(List<Metadata> metadataList) {
        metadataList.forEach(metadata -> {
            metadata.setId(null);
        });
        return metaclientRepository.saveAll(metadataList);
    }

    @Transactional
    public Metadata updateMetadata(UUID id, Map<String, Object> updates) {
        if (id == null) {
            throw new IllegalArgumentException("Metadata ID cannot be null");
        }
        
        Metadata existingMetadata = metaclientRepository.findById(id)
                .orElseThrow(() -> new IllegalArgumentException("Metadata with ID " + id + " not found"));
        
        boolean hasUpdates = false;
        
        if (updates.containsKey("name")) {
            String name = (String) updates.get("name");
            if (name != null && !name.trim().isEmpty()) {
                existingMetadata.setName(name);
                hasUpdates = true;
            }
        }
        if (updates.containsKey("type")) {
            String type = (String) updates.get("type");
            if (type != null && !type.trim().isEmpty()) {
                existingMetadata.setType(type);
                hasUpdates = true;
            }
        }
        if (updates.containsKey("issuer")) {
            String issuer = (String) updates.get("issuer");
            if (issuer != null && !issuer.trim().isEmpty()) {
                existingMetadata.setIssuer(issuer);
                hasUpdates = true;
            }
        }
        if (updates.containsKey("riskRating")) {
            String riskRating = (String) updates.get("riskRating");
            if (riskRating != null && !riskRating.trim().isEmpty()) {
                existingMetadata.setRiskRating(riskRating);
                hasUpdates = true;
            }
        }
        
        if (!hasUpdates) {
            throw new IllegalArgumentException("No valid updates provided");
        }
        
        return metaclientRepository.save(existingMetadata);
    }

    @Transactional
    public void deleteMetadata(UUID id) {
        metaclientRepository.deleteById(id);
    }

    public List<Metadata> searchByKeyword(String keyword) {
        return metaclientRepository.searchByKeyword(keyword);
    }

    public List<String> getAllTypes() {
        return metaclientRepository.findAllTypes();
    }

    public List<String> getAllIssuers() {
        return metaclientRepository.findAllIssuers();
    }

    @Transactional
    public void loadTestData(int count) {
        dataLoader.loadTestData(count);
    }
} 