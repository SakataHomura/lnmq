package qerror

import "fmt"

const INVALID string = "INVALID"
const INVALID_PARAMETER string = "INVALID_PARAMETER"
const INVALID_MESSAGE string = "INVALID_MESSAGE"
const AUTH_FAILED string = "AUTH_FAILED"
const AUTH_UNAUTH string = "AUTH_UNAUTH"

func MakeError(code, desc string) error {
	return fmt.Errorf("%s %s", code, desc)
}
