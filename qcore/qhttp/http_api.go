package qhttp

import (
	"fmt"
	"github.com/gin-gonic/gin/json"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"time"
)

type ApiHandler func(http.ResponseWriter, *http.Request, httprouter.Params) (interface{}, error)
type Decorator func(ApiHandler) ApiHandler

type HttpErr struct {
	Code int
	Text string
}

func (e HttpErr) Error() string {
	return e.Text
}

func Decorate(f ApiHandler, ds ...Decorator) httprouter.Handle {
	d := f
	for _, v := range ds {
		d = v(d)
	}

	return func(writer http.ResponseWriter, request *http.Request, params httprouter.Params) {
		d(writer, request, params)
	}
}

func Log(handler ApiHandler) ApiHandler {
	return func(writer http.ResponseWriter, request *http.Request, params httprouter.Params) (i interface{}, e error) {
		start := time.Now()
		response, err := handler(writer, request, params)
		elapsed := time.Since(start)
		status := 200
		if e, ok := err.(HttpErr); ok {
			status = e.Code
		}
		fmt.Println(elapsed, status)
		return response, err
	}
}

func HandleV1(f ApiHandler) ApiHandler {
	return func(writer http.ResponseWriter, request *http.Request, params httprouter.Params) (i interface{}, e error) {
		data, err := f(writer, request, params)
		if err != nil {
			RespondV1(writer, err.(HttpErr).Code, err)
		} else {
			RespondV1(writer, 200, data)
		}

		return nil, nil
	}
}

func RespondV1(w http.ResponseWriter, code int, data interface{}) {
	var response []byte
	var err error
	var isJson bool

	if code == 200 {
		switch data.(type) {
		case string:
			response = []byte(data.(string))
		case []byte:
			response = data.([]byte)
		case nil:
			response = []byte{}
		default:
			isJson = true
			response, err = json.Marshal(data)
			if err != nil {
				code = 500
				data = err
			}

		}
	}

	if code != 200 {
		isJson = true
		response, _ = json.Marshal(struct {
			Message string `json:"message"`
		}{fmt.Sprintf("%s", data)})
	}

	if isJson {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	}

	w.Header().Set("X-NSQ-Content-Type", "nsq; version=1.0")
	w.WriteHeader(code)
	w.Write(response)
}

func HttpPanicHandler() func(w http.ResponseWriter, req *http.Request, p interface{}) {
	f := Decorate(func(writer http.ResponseWriter, request *http.Request, params httprouter.Params) (i interface{}, e error) {
		return nil, HttpErr{500, "INTERNAL_ERROR"}
	}, Log, HandleV1)

	return func(w http.ResponseWriter, req *http.Request, p interface{}) {
		f(w, req, nil)
	}
}

func HttpNotFoundHandler() http.Handler {
	f := Decorate(func(writer http.ResponseWriter, request *http.Request, params httprouter.Params) (i interface{}, e error) {
		return nil, HttpErr{404, "NOT_FOUND"}
	}, Log, HandleV1)

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		f(w, req, nil)
	})
}

func MethodNotAllowedHandler() http.Handler {
	f := Decorate(func(writer http.ResponseWriter, request *http.Request, params httprouter.Params) (i interface{}, e error) {
		return nil, HttpErr{405, "METHOD_NOT_ALLOWED"}
	}, Log, HandleV1)

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		f(w, req, nil)
	})
}
