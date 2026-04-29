package com.example.archive.events;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/events")
public class EventController {

    private final EventService events;

    public EventController(EventService events) {
        this.events = events;
    }

    @PostMapping
    public ResponseEntity<Void> create(@RequestBody Event event) {
        events.insert(event);
        return ResponseEntity.accepted().build();
    }

    @PostMapping("/seed")
    public Map<String, Integer> seed(
            @RequestParam(defaultValue = "14") int days,
            @RequestParam(defaultValue = "1000") int perDay) {
        int inserted = events.seed(days, perDay);
        return Map.of("inserted", inserted, "days", days, "perDay", perDay);
    }
}
