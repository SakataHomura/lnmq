package qerror

import "fmt"

const INVALID string = "INVALID"
const INVALID_PARAMETER string = "INVALID_PARAMETER"
const INVALID_MESSAGE string = "INVALID_MESSAGE"

func MakeError(code, desc string) error {
    return fmt.Errorf("%s %s", code, desc)
}
