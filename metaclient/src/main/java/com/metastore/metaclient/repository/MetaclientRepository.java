package com.metastore.metaclient.repository;

import com.metastore.metaclient.model.Metadata;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface MetaclientRepository extends JpaRepository<Metadata, UUID> {
    
    @Query("SELECT DISTINCT m.type FROM Metadata m")
    List<String> findAllTypes();
    
    @Query("SELECT DISTINCT m.issuer FROM Metadata m")
    List<String> findAllIssuers();
    
    @Query("SELECT m FROM Metadata m WHERE " +
           "LOWER(m.name) LIKE LOWER(CONCAT('%', :keyword, '%')) OR " +
           "LOWER(m.type) LIKE LOWER(CONCAT('%', :keyword, '%')) OR " +
           "LOWER(m.issuer) LIKE LOWER(CONCAT('%', :keyword, '%')) OR " +
           "LOWER(m.riskRating) LIKE LOWER(CONCAT('%', :keyword, '%'))")
    List<Metadata> searchByKeyword(@Param("keyword") String keyword);
} 