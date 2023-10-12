package logs

import (
	"fmt"

	"io"

	"net/http"

	"log"

	"os"
)

var Logger *SimpleLogger

func init() {

	Logger, _ = NewSimpleLogger("app.log")

}

type SimpleLogger struct {
	infoLogger *log.Logger

	warningLogger *log.Logger

	errorLogger *log.Logger
}

func Createlogfile(w http.ResponseWriter) *SimpleLogger {

	Logger, err := NewSimpleLogger("error.log")

	if err != nil {

		http.Error(w, "Failed to initialize logger", http.StatusInternalServerError)

	}

	return Logger

}

func NewSimpleLogger(logFileName string) (*SimpleLogger, error) {

	file, err := os.OpenFile(logFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)

	if err != nil {

		return nil, err

	}

	return &SimpleLogger{

		infoLogger: log.New(io.MultiWriter(os.Stdout, file), "[INFO] ", log.Ldate|log.Ltime|log.Lshortfile),

		warningLogger: log.New(io.MultiWriter(os.Stdout, file), "[WARNING] ", log.Ldate|log.Ltime|log.Lshortfile),

		errorLogger: log.New(io.MultiWriter(os.Stdout, file), "[ERROR] ", log.Ldate|log.Ltime|log.Lshortfile),
	}, nil

}

func (l *SimpleLogger) Warning(message string) {

	l.warningLogger.Println(message)

}

func (l *SimpleLogger) Error(message string, err error) {

	errorMsg := message

	if err != nil {

		errorMsg += ": " + err.Error()

	}

	l.errorLogger.Println(errorMsg)

}

func (l *SimpleLogger) Info(format string, args ...interface{}) {

	message := fmt.Sprintf(format, args...)

	l.infoLogger.Println(message)

}
