package com.audit.trail.datagenerator.model

import com.audit.trail.datagenerator.util.RawActions
import com.audit.trail.datagenerator.util.RawScreens

class EventDetails(var name: RawScreens, var action: RawActions, var userId:String, var productId:String) {
}