package com.vpals.apps.entities

class TargetInfoDetails {
    private var _targetPath = ""
    private var _tableName = ""
    
    //getters
    
    def targetPath = _targetPath
    def tableName = _tableName
    
    //setters
    
    def targetPath_(value:String):Unit = _targetPath = value
    def tableName_(value:String):Unit = _tableName = value
}