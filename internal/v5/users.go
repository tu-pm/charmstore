package v5

import (
	"encoding/json"
	"github.com/juju/charmrepo/v6/csclient/params"
	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charmstore.v5/internal/mongodoc"
	"net/http"
)

// GET users or POST users?username=xx&password=xx or DELETE users?username=xx
func (h *ReqHandler) serveUsers(_ http.Header, req *http.Request) (interface{}, error) {
	auth, err := h.Authenticate(req)
	if err != nil {
		return nil, errgo.WithCausef(nil, params.ErrUnauthorized, "invalid admin credentials")
	}
	if !auth.Admin {
		return nil, errgo.WithCausef(nil, params.ErrUnauthorized, "only admins can preform this action")
	}
	switch req.Method {
	case "GET":
		users := h.Store.ListUsers()
		return users, nil
	case "POST":
		user, err := extractUser(req)
		if err != nil {
			return nil, errgo.WithCausef(err, params.ErrBadRequest, "failed to extract user info from request")
		}
		if user.Username == "" || user.Password == "" {
			return nil, errgo.WithCausef(nil, params.ErrBadRequest, "user or password is not specified")
		}
		return nil, h.Store.AddUser(&user)
	case "DELETE":
		user, err := extractUser(req)
		if err != nil {
			return nil, errgo.WithCausef(err, params.ErrBadRequest, "failed to extract user info from request")
		}
		return nil, h.Store.DeleteUser(user.Username)
	default:
		return nil, errgo.WithCausef(nil, params.ErrMethodNotAllowed, "%s not allowed", req.Method)
	}
}

func extractUser(req *http.Request) (mongodoc.User, error) {
	decoder := json.NewDecoder(req.Body)
	var user mongodoc.User
	err := decoder.Decode(&user)
	return user, err
}
