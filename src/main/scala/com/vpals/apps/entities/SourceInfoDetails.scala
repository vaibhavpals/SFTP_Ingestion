package com.vpals.apps.entities

class SourceInfoDetails() {
    private var _hostName = ""
    private var _fileLocation = ""
    private var _userName = ""
    private var _password = ""
    private var _port = 0
    private var _fileFormat = "csv"
    //getters
    
    def hostName = _hostName
    def fileLocation = _fileLocation
    def userName = _userName
    def password = _password
    def port = _port
    def fileFormat = _fileFormat
    
    //setters
    
    def hostName_(value:String):Unit = _hostName = value
    def fileLocation_(value:String):Unit = _fileLocation = value
    def userName_(value:String):Unit = _userName = value
    def password_(value:String):Unit = _password = value
    def port_(value:Int):Unit = _port = value
    def fileFormat_(value:String):Unit = _fileFormat = value
}
    