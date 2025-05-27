package com.turib.airflowscheduler.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import com.turib.airflowscheduler.model.Endpoint;
import com.turib.airflowscheduler.service.EndpointService;

import java.util.List;

@RestController
@RequestMapping("/api/endpoints")
public class EndpointController {
    private final EndpointService service;
    private int counter = 0;

    public EndpointController(EndpointService service) {
        this.service = service;
    }

    @GetMapping
    public List<Endpoint> getAllEndpoints() {
        counter++;
        System.out.println("Endpoint accessed " + counter + " times");
        return service.getAllEndpoints();
    }

    @PostMapping
    public Endpoint createEndpoint(@RequestBody Endpoint endpoint) {
        return service.saveEndpoint(endpoint);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteEndpoint(@PathVariable Long id) {
        service.deleteEndpoint(id);
        return ResponseEntity.noContent().build();
    }
}