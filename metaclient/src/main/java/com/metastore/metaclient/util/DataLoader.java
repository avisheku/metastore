package com.metastore.metaclient.util;

import com.metastore.metaclient.model.Metadata;
import com.metastore.metaclient.repository.MetaclientRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Component
@Profile("!prod")
public class DataLoader implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(DataLoader.class);
    private static final int DEFAULT_TEST_DATA_COUNT = 10;
    private static final int BATCH_SIZE = 100;
    
    private final MetaclientRepository metaclientRepository;

    @Autowired
    public DataLoader(MetaclientRepository metaclientRepository) {
        this.metaclientRepository = metaclientRepository;
    }

    @Override
    public void run(String... args) {
        logger.info("DataLoader is disabled. Use /metadata/load-test-data/{count} endpoint to load test data.");
    }

    public void loadTestData(int count) {
        logger.info("Loading {} test metadata records", count);
        
        String[] types = {"Bond", "Stock", "ETF", "Mutual Fund", "Derivative"};
        String[] issuers = {"JP Morgan", "Goldman Sachs", "Morgan Stanley", "BlackRock", "Vanguard"};
        String[] riskRatings = {"Low", "Medium", "High", "Very High"};
        
        List<Metadata> batch = new ArrayList<>(BATCH_SIZE);
        
        for (int i = 0; i < count; i++) {
            Metadata metadata = new Metadata();
            metadata.setName("Test Asset " + i);
            metadata.setType(types[ThreadLocalRandom.current().nextInt(types.length)]);
            metadata.setIssuer(issuers[ThreadLocalRandom.current().nextInt(issuers.length)]);
            metadata.setRiskRating(riskRatings[ThreadLocalRandom.current().nextInt(riskRatings.length)]);
            
            batch.add(metadata);
            
            if (batch.size() >= BATCH_SIZE) {
                metaclientRepository.saveAll(batch);
                batch.clear();
                logger.info("Processed {} records", i + 1);
            }
        }
        
        if (!batch.isEmpty()) {
            metaclientRepository.saveAll(batch);
        }
        
        logger.info("Loaded {} test metadata records into the database", count);
    }
} 