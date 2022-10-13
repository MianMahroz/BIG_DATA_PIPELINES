package com.audit.trail.datagenerator.controller

import com.audit.trail.datagenerator.service.EventService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController


@RestController
class EventController(val eventService: EventService) {

    @GetMapping("/generateEvents")
    fun generateRawEvents(): ResponseEntity<Any> {
        return  ResponseEntity.ok(eventService.generateEvents())
    }

    @GetMapping("/getEventsByType/{type}")
    fun generateRawEvents(@PathVariable(name="type")type:String): ResponseEntity<Any> {
        return  ResponseEntity.ok(eventService.getEventsByType(type))
    }



}