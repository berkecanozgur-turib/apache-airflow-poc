package com.turib.airflowscheduler.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.turib.airflowscheduler.model.Endpoint;

@Repository
public interface EndpointRepository extends JpaRepository<Endpoint, Long> {
}