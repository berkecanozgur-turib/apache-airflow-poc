package com.turib.airflowscheduler.service;

import org.springframework.stereotype.Service;

import com.turib.airflowscheduler.model.Endpoint;
import com.turib.airflowscheduler.repository.EndpointRepository;

import java.util.List;

@Service
public class EndpointService {
    private final EndpointRepository repository;

    public EndpointService(EndpointRepository repository) {
        this.repository = repository;
    }

    public List<Endpoint> getAllEndpoints() {
        return repository.findAll();
    }

    public Endpoint saveEndpoint(Endpoint endpoint) {
        return repository.save(endpoint);
    }

    public void deleteEndpoint(Long id) {
        repository.deleteById(id);
    }
}