package com.audit.trail.datagenerator.model

import com.audit.trail.datagenerator.util.RawActions
import com.audit.trail.datagenerator.util.RawScreens
import org.springframework.data.mongodb.core.mapping.Document

@Document
class EventDetails(var name: RawScreens, var action: RawActions, var userId:String, var productId:String) {
}