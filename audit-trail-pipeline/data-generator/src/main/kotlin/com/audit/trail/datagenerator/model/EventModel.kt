package com.audit.trail.datagenerator.model

import com.audit.trail.datagenerator.util.EventTypes
import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document

@Document(value = "auditlog")
class EventModel(@Id var id:Long, var type:EventTypes,var details:Any) {
}