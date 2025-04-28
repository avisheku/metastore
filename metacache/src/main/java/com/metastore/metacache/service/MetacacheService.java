package com.metastore.metacache.service;

import com.metastore.metacache.exception.MetadataNotFoundException;
import com.metastore.metacache.model.Metadata;
import com.metastore.metacache.model.MetadataResponse;
import com.metastore.metacache.model.CacheInvalidationType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class MetacacheService {
    private static final Logger logger = LoggerFactory.getLogger(MetacacheService.class);
    private static final String KEY_PREFIX = "metadata:";
    private static final String ALL_METADATA_KEY = "metadata:all";
    private static final String TYPES_KEY = "metadata:types";
    private static final String ISSUERS_KEY = "metadata:issuers";
    private static final String SEARCH_KEY_PREFIX = "metadata:search:";
    private static final long CACHE_TTL_HOURS = 24;
    private static final String METACLIENT_BASE_URL = "http://metaclient:8081";

    private final RedisTemplate<String, Object> redisTemplate;
    private final RestTemplate restTemplate;

    @Autowired
    public MetacacheService(RedisTemplate<String, Object> redisTemplate, RestTemplate restTemplate) {
        this.redisTemplate = redisTemplate;
        this.restTemplate = restTemplate;
    }

    public MetadataResponse getMetadata(UUID id) {
        Instant start = Instant.now();
        String key = KEY_PREFIX + id;

        Object cachedData = redisTemplate.opsForValue().get(key);

        if (cachedData instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) cachedData;
            Duration timeTaken = Duration.between(start, Instant.now());
            Metadata metadata = new Metadata();
            metadata.setId(UUID.fromString(map.get("id").toString()));
            metadata.setName((String) map.get("name"));
            metadata.setType((String) map.get("type"));
            metadata.setIssuer((String) map.get("issuer"));
            metadata.setRiskRating((String) map.get("riskRating"));
            return new MetadataResponse(metadata, timeTaken, "CACHE");
        }

        try {
            String url = METACLIENT_BASE_URL + "/metadata/" + id;
            ResponseEntity<Metadata> response = restTemplate.exchange(
                    url,
                    HttpMethod.GET,
                    null,
                    Metadata.class
            );

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                Metadata metadata = response.getBody();
                Duration timeTaken = Duration.between(start, Instant.now());
                redisTemplate.opsForValue().set(key, metadata, CACHE_TTL_HOURS, TimeUnit.HOURS);
                return new MetadataResponse(metadata, timeTaken, "DB");
            }
            throw new RuntimeException("Invalid response format from database");
        } catch (org.springframework.web.client.HttpClientErrorException.NotFound e) {
            throw new MetadataNotFoundException("Metadata with ID " + id + " not found");
        } catch (Exception e) {
            if (e instanceof MetadataNotFoundException) {
                throw e;
            }
            throw new RuntimeException("Failed to fetch metadata", e);
        }
    }

    public MetadataResponse getAllMetadata() {
        Instant start = Instant.now();

        Object cachedData = redisTemplate.opsForValue().get(ALL_METADATA_KEY);
        if (cachedData instanceof List) {
            @SuppressWarnings("unchecked")
            List<Metadata> metadata = (List<Metadata>) cachedData;
            Duration timeTaken = Duration.between(start, Instant.now());
            return new MetadataResponse(metadata, timeTaken, "CACHE");
        }

        try {
            ResponseEntity<List<Metadata>> response = restTemplate.exchange(
                    METACLIENT_BASE_URL + "/metadata/all",
                    HttpMethod.GET,
                    null,
                    new ParameterizedTypeReference<>() {
                    }
            );

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                List<Metadata> metadata = response.getBody();
                Duration timeTaken = Duration.between(start, Instant.now());
                redisTemplate.opsForValue().set(ALL_METADATA_KEY, metadata, CACHE_TTL_HOURS, TimeUnit.HOURS);
                metadata.forEach(this::saveToCache);
                return new MetadataResponse(metadata, timeTaken, "DB");
            }
            throw new RuntimeException("Invalid response format from database");
        } catch (Exception e) {
            logger.error("Error fetching all metadata: {}", e.getMessage());
            throw new RuntimeException("Failed to fetch all metadata", e);
        }
    }

    public List<Metadata> createMetadata(List<Metadata> metadataList) {
        try {
            ResponseEntity<List<Metadata>> response = restTemplate.exchange(
                    METACLIENT_BASE_URL + "/metadata",
                    HttpMethod.POST,
                    new HttpEntity<>(metadataList),
                    new ParameterizedTypeReference<>() {
                    }
            );

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                List<Metadata> createdMetadata = response.getBody();
                createdMetadata.forEach(this::saveToCache);
                invalidateCaches(null, CacheInvalidationType.ALL);
                return createdMetadata;
            }
            throw new RuntimeException("Invalid response format from database");
        } catch (Exception e) {
            logger.error("Error creating metadata: {}", e.getMessage());
            throw new RuntimeException("Failed to create metadata", e);
        }
    }

    public Metadata updateMetadata(UUID id, Map<String, Object> updates) {
        try {
            String key = KEY_PREFIX + id;
            Object cachedData = redisTemplate.opsForValue().get(key);
            
            if (cachedData == null) {
                try {
                    ResponseEntity<Metadata> getResponse = restTemplate.exchange(
                            METACLIENT_BASE_URL + "/metadata/" + id,
                            HttpMethod.GET,
                            null,
                            Metadata.class
                    );
                    
                    if (!getResponse.getStatusCode().is2xxSuccessful() || getResponse.getBody() == null) {
                        throw new MetadataNotFoundException("Metadata with ID " + id + " not found");
                    }
                } catch (Exception e) {
                    throw new MetadataNotFoundException("Metadata with ID " + id + " not found");
                }
            }

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<Map<String, Object>> requestEntity = new HttpEntity<>(updates, headers);
            
            ResponseEntity<Metadata> response = restTemplate.exchange(
                    METACLIENT_BASE_URL + "/metadata/" + id,
                    HttpMethod.PUT,
                    requestEntity,
                    Metadata.class
            );

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                Metadata updatedMetadata = response.getBody();
                saveToCache(updatedMetadata);
                invalidateCaches(id, CacheInvalidationType.SPECIFIC_ID);
                return updatedMetadata;
            }
            
            throw new RuntimeException("Failed to update metadata: Invalid response from metaclient");
        } catch (MetadataNotFoundException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to update metadata: " + e.getMessage(), e);
        }
    }

    public void deleteMetadata(UUID id) {
        try {
            restTemplate.delete(METACLIENT_BASE_URL + "/metadata/" + id);
            invalidateCaches(id, CacheInvalidationType.SPECIFIC_ID);
        } catch (Exception e) {
            logger.error("Error deleting metadata: {}", e.getMessage());
            throw new RuntimeException("Failed to delete metadata", e);
        }
    }

    public MetadataResponse searchMetadata(String keyword) {
        Instant start = Instant.now();
        String searchKey = SEARCH_KEY_PREFIX + keyword.toLowerCase();

        Object cachedData = redisTemplate.opsForValue().get(searchKey);
        if (cachedData instanceof List) {
            @SuppressWarnings("unchecked")
            List<Metadata> results = (List<Metadata>) cachedData;
            Duration timeTaken = Duration.between(start, Instant.now());
            return new MetadataResponse(results, timeTaken, "CACHE");
        }

        try {
            ResponseEntity<List<Metadata>> response = restTemplate.exchange(
                    METACLIENT_BASE_URL + "/metadata/search?keyword=" + keyword,
                    HttpMethod.GET,
                    null,
                    new ParameterizedTypeReference<>() {
                    }
            );

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                List<Metadata> results = response.getBody();
                Duration timeTaken = Duration.between(start, Instant.now());
                redisTemplate.opsForValue().set(searchKey, results, CACHE_TTL_HOURS, TimeUnit.HOURS);
                return new MetadataResponse(results, timeTaken, "DB");
            }
            throw new RuntimeException("Invalid response format from database");
        } catch (Exception e) {
            logger.error("Error searching metadata: {}", e.getMessage());
            throw new RuntimeException("Failed to search metadata", e);
        }
    }

    public MetadataResponse getAllTypes() {
        Instant start = Instant.now();

        Object cachedData = redisTemplate.opsForValue().get(TYPES_KEY);
        if (cachedData instanceof List) {
            @SuppressWarnings("unchecked")
            List<String> types = (List<String>) cachedData;
            Duration timeTaken = Duration.between(start, Instant.now());
            return new MetadataResponse(types, timeTaken, "CACHE");
        }

        try {
            ResponseEntity<List<String>> response = restTemplate.exchange(
                    METACLIENT_BASE_URL + "/metadata/types",
                    HttpMethod.GET,
                    null,
                    new ParameterizedTypeReference<>() {
                    }
            );

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                List<String> types = response.getBody();
                Duration timeTaken = Duration.between(start, Instant.now());
                redisTemplate.opsForValue().set(TYPES_KEY, types, CACHE_TTL_HOURS, TimeUnit.HOURS);
                return new MetadataResponse(types, timeTaken, "DB");
            }
            throw new RuntimeException("Invalid response format from database");
        } catch (Exception e) {
            logger.error("Error fetching all types: {}", e.getMessage());
            throw new RuntimeException("Failed to fetch all types", e);
        }
    }

    public MetadataResponse getAllIssuers() {
        Instant start = Instant.now();

        Object cachedData = redisTemplate.opsForValue().get(ISSUERS_KEY);
        if (cachedData instanceof List) {
            @SuppressWarnings("unchecked")
            List<String> issuers = (List<String>) cachedData;
            Duration timeTaken = Duration.between(start, Instant.now());
            return new MetadataResponse(issuers, timeTaken, "CACHE");
        }

        try {
            ResponseEntity<List<String>> response = restTemplate.exchange(
                    METACLIENT_BASE_URL + "/metadata/issuers",
                    HttpMethod.GET,
                    null,
                    new ParameterizedTypeReference<>() {
                    }
            );

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                List<String> issuers = response.getBody();
                Duration timeTaken = Duration.between(start, Instant.now());
                redisTemplate.opsForValue().set(ISSUERS_KEY, issuers, CACHE_TTL_HOURS, TimeUnit.HOURS);
                return new MetadataResponse(issuers, timeTaken, "DB");
            }
            throw new RuntimeException("Invalid response format from database");
        } catch (Exception e) {
            logger.error("Error fetching all issuers: {}", e.getMessage());
            throw new RuntimeException("Failed to fetch all issuers", e);
        }
    }

    public String loadTestData(int count) {
        try {
            ResponseEntity<Map<String, String>> response = restTemplate.exchange(
                    METACLIENT_BASE_URL + "/metadata/load/test-data/" + count,
                    HttpMethod.POST,
                    null,
                    new ParameterizedTypeReference<Map<String, String>>() {}
            );

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                invalidateCaches(null, CacheInvalidationType.ALL);
                return response.getBody().get("message");
            }
            throw new RuntimeException("Invalid response format from database");
        } catch (Exception e) {
            logger.error("Error loading test data: {}", e.getMessage());
            throw new RuntimeException("Failed to load test data", e);
        }
    }

    private void invalidateCaches(UUID id, CacheInvalidationType type) {
        if (type == CacheInvalidationType.ALL) {
            redisTemplate.delete(ALL_METADATA_KEY);
            redisTemplate.delete(TYPES_KEY);
            redisTemplate.delete(ISSUERS_KEY);
            redisTemplate.delete(redisTemplate.keys(SEARCH_KEY_PREFIX + "*"));
        } else if (type == CacheInvalidationType.SPECIFIC_ID && id != null) {
            redisTemplate.delete(KEY_PREFIX + id);
            redisTemplate.delete(ALL_METADATA_KEY);
            redisTemplate.delete(TYPES_KEY);
            redisTemplate.delete(ISSUERS_KEY);
            redisTemplate.delete(redisTemplate.keys(SEARCH_KEY_PREFIX + "*"));
        }
    }

    private void saveToCache(Metadata metadata) {
        String key = KEY_PREFIX + metadata.getId();
        redisTemplate.opsForValue().set(key, metadata, CACHE_TTL_HOURS, TimeUnit.HOURS);
    }
} 