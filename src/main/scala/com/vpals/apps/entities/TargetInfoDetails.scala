package com.vpals.apps.entities

class TargetInfoDetails {
    private var _targetPath = ""
    private var _tableName = ""
    private var _tableSchema = ""
    private var _sparkUrl = ""
    
    //getters
    
    def targetPath = _targetPath
    def tableName = _tableName
    def tableSchema = _tableSchema
    def sparkUrl = _sparkUrl
    
    //setters
    
    def targetPath_(value:String):Unit = _targetPath = value
    def tableName_(value:String):Unit = _tableName = value
    def tableSchema_(value:String):Unit = _tableSchema = value
    def sparkUrl_(value:String):Unit = _sparkUrl = value
}