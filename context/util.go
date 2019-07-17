package context

import (
	"strings"
	"time"
)

// Since looks up key, which should be a time.Time, and returns the duration
// since that time. If the key is not found, the value returned will be zero.
// This is helpful when inferring metrics related to context execution times.
func Since(ctx Context, key interface{}) time.Duration {
	if startedAt, ok := ctx.Value(key).(time.Time); ok {
		return time.Since(startedAt)
	}
	return 0
}

// GetStringValue returns a string value from the context. The empty string
// will be returned if not found.
func GetStringValue(ctx Context, key interface{}) (value string) {
	if valuev, ok := ctx.Value(key).(string); ok {
		value = valuev
	}
	return value
}

func getName(ctx Context) (name string) {
	return GetStringValue(ctx, "vars.name")
}

//TYPE XXX USRADDR XXX REPONAME XXX
//manifest or layer?
func GetType(ctx Context) (name string) {
	varsname := GetStringValue(ctx, "vars.name")
	tmps := strings.Split(varsname, "USRADDR")[0]
	rtype := strings.Split(tmps, "TYPE")[1]
	return rtype
}

//usraddr
func GetUsrAddr(ctx Context) (name string) {
	varsname := GetStringValue(ctx, "vars.name")
	tmps := strings.Split(varsname, "REPONAME")[0]
	usraddr := strings.Split(tmps, "USRADDR")[1]
	return usraddr
}

//reponame
func GetRepoName(ctx Context) (name string) {
	varsname := GetStringValue(ctx, "vars.name")
	reponame := strings.Split(varsname, "REPONAME")[1]
	return reponame
}
