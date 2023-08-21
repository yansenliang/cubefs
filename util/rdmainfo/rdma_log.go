package rdmaInfo
type RdmaLog interface {
    Debug(format string, v ...interface{})
    Info(format string, v ...interface{})
    Warn(format string, v ...interface{})
    Error(format string, v ...interface{})
}

var rdmaLog RdmaLog

func debugF(format string, v ...interface{}) {
    if rdmaLog == nil {
        return
    }
    rdmaLog.Debug(format, v...)
    return
}

func infoF(format string, v ...interface{}) {
    if rdmaLog == nil {
        return
    }
    rdmaLog.Info(format, v...)
    return
}

func warnF(format string, v ...interface{}) {
    if rdmaLog == nil {
        return
    }
    rdmaLog.Warn(format, v...)
    return
}

func errorF(format string, v ...interface{}) {
    if rdmaLog == nil {
        return
    }
    rdmaLog.Error(format, v...)
    return
}
