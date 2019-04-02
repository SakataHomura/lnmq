package qcore

import "regexp"

var validTopicFormatRegex = regexp.MustCompile(`^[a-zA-Z0-9_]+$`)

func IsValidName(name string) bool {
	if len(name) > 64 || len(name) < 1 {
		return false
	}

	return validTopicFormatRegex.MatchString(name)
}
