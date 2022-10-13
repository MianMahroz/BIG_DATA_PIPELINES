package com.audit.trail.datagenerator.service

import com.audit.trail.datagenerator.model.EventDetails
import com.audit.trail.datagenerator.model.EventModel
import com.audit.trail.datagenerator.repo.EventRepository
import com.audit.trail.datagenerator.util.EventTypes
import com.audit.trail.datagenerator.util.RawActions
import com.audit.trail.datagenerator.util.RawScreens
import org.springframework.stereotype.Service
import kotlin.random.Random


@Service
class EventServiceImpl(val eventRepo: EventRepository):EventService {

    override fun generateEvents():String {

        for (i in 1 until 10000){
           eventRepo.save(
               EventModel(Random.nextLong(),
                   EventTypes.values()[Random.nextInt(5)],
                   EventDetails(RawScreens.values()[Random.nextInt(3)],
                       RawActions.values()[Random.nextInt(2)],
                       " ${Random.nextLong()}"," ${Random.nextLong()}"))
           )
        }
       return "RAW DATA GENERATED SUCCESSFULLY"
    }


    override fun getEventsByType(type: String): List<EventModel> {
        return eventRepo.findEventByType(type)
    }
}