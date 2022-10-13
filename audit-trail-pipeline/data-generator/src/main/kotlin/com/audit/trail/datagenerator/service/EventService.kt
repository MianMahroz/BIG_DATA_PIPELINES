package com.audit.trail.datagenerator.service

import com.audit.trail.datagenerator.model.EventModel
import org.springframework.stereotype.Service

@Service
interface EventService {

    fun generateEvents(): String
    fun getEventsByType(type:String): List<EventModel>

}