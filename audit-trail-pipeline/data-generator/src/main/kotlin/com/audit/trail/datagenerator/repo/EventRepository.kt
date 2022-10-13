package com.audit.trail.datagenerator.repo

import com.audit.trail.datagenerator.model.EventModel
import org.springframework.data.mongodb.repository.MongoRepository
import org.springframework.data.mongodb.repository.Query
import org.springframework.stereotype.Repository

@Repository
interface EventRepository: MongoRepository<EventModel,Long> {


    @Query("{type:?0}")
    fun findEventByType(type:String): List<EventModel> ;
        

}