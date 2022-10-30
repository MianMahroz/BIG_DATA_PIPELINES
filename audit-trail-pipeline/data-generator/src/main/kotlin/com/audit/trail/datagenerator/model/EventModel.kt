package com.audit.trail.datagenerator.model

import com.audit.trail.datagenerator.util.EventTypes
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import java.time.LocalDateTime

@Document(value = "auditlog")
class EventModel(@Id var id:Long, var type:EventTypes,var details:Any,var created_at:String) {
}