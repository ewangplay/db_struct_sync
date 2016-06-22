package main

import (
	"strings"
)

var g_logLevel *LogLevel

func LOG_DEBUG(format string, a ...interface{}) {
	if DEBUG() {
		g_logger.Debug(format, a...)
	}
}

func LOG_INFO(format string, a ...interface{}) {
	if INFO() {
		g_logger.Info(format, a...)
	}
}

func LOG_WARN(format string, a ...interface{}) {
	if WARN() {
		g_logger.Warn(format, a...)
	}
}

func LOG_ERROR(format string, a ...interface{}) {
	if ERROR() {
		g_logger.Error(format, a...)
	}
}

func DEBUG() bool {
	if g_logLevel != nil {
		return g_logLevel.IsDebug()
	}
	return true
}

func INFO() bool {
	if g_logLevel != nil {
		return g_logLevel.IsInfo()
	}
	return true
}

func WARN() bool {
	if g_logLevel != nil {
		return g_logLevel.IsWarn()
	}
	return true
}

func ERROR() bool {
	if g_logLevel != nil {
		return g_logLevel.IsError()
	}
	return true
}

type LogLevel struct {
	is_debug bool
	is_info  bool
	is_warn  bool
	is_error bool
}

func NewLogLevel() (*LogLevel, error) {
	logLevel := &LogLevel{}

	level, ok := g_config.Get("log.level")
	if !ok || level == "" {
		logLevel.is_debug = true
		logLevel.is_info = true
		logLevel.is_warn = true
		logLevel.is_error = true
	} else {
		switch {
		case strings.Contains(level, "error"):
			logLevel.is_debug = false
			logLevel.is_info = false
			logLevel.is_warn = false
			logLevel.is_error = true
		case strings.Contains(level, "warn"):
			logLevel.is_debug = false
			logLevel.is_info = false
			logLevel.is_warn = true
			logLevel.is_error = true
		case strings.Contains(level, "info"):
			logLevel.is_debug = false
			logLevel.is_info = true
			logLevel.is_warn = true
			logLevel.is_error = true
		case strings.Contains(level, "debug"):
			logLevel.is_debug = true
			logLevel.is_info = true
			logLevel.is_warn = true
			logLevel.is_error = true
		default:
			logLevel.is_debug = true
			logLevel.is_info = true
			logLevel.is_warn = true
			logLevel.is_error = true
		}
	}

	return logLevel, nil
}

func (this *LogLevel) IsDebug() bool {
	return this.is_debug
}

func (this *LogLevel) IsInfo() bool {
	return this.is_info
}

func (this *LogLevel) IsWarn() bool {
	return this.is_warn
}

func (this *LogLevel) IsError() bool {
	return this.is_error
}
