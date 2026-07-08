package response

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
	TraceID string      `json:"trace_id,omitempty"`
}

const (
	CodeSuccess = 0
	CodeFail    = 1
)

func OK(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, Response{
		Code:    CodeSuccess,
		Message: "success",
		Data:    data,
		TraceID: c.GetString("trace_id"),
	})
}

func Fail(c *gin.Context, code int, msg string) {
	c.JSON(http.StatusOK, Response{
		Code:    code,
		Message: msg,
		Data:    nil,
		TraceID: c.GetString("trace_id"),
	})
}

func FailWithStatus(c *gin.Context, status, code int, msg string) {
	c.JSON(status, Response{
		Code:    code,
		Message: msg,
		Data:    nil,
		TraceID: c.GetString("trace_id"),
	})
}

type PageData struct {
	List     interface{} `json:"list"`
	Total    int64       `json:"total"`
	Page     int         `json:"page"`
	PageSize int         `json:"page_size"`
}