// Copyright 2014 Canonical Ltd.
// Licensed under the AGPLv3, see LICENCE file for details.

package v4

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"gopkg.in/mgo.v2/bson"

	"github.com/juju/charmstore/internal/mongodoc"
	"github.com/juju/charmstore/params"
)

var statusChecks = map[string]struct {
	name  string
	check func(*Handler) (string, bool)
}{
	"mongo_connected": {
		name:  "MongoDB is connected",
		check: (*Handler).checkMongoConnected,
	},
	"mongo_collections": {
		name:  "MongoDB collections",
		check: (*Handler).checkCollections,
	},
	"entities": {
		name:  "Entities in charm store",
		check: (*Handler).checkEntities,
	},
	"server_started": {
		name:  "Server started",
		check: (*Handler).checkServerStarted,
	},
	"ingestion": {
		name:  "Ingestion",
		check: (*Handler).checkIngestion,
	},
}

// GET /debug/status
// http://tinyurl.com/qdm5yg7
func (h *Handler) serveDebugStatus(_ http.Header, req *http.Request) (interface{}, error) {
	status := make(map[string]params.DebugStatus)
	for key, check := range statusChecks {
		value, ok := check.check(h)
		status[key] = params.DebugStatus{
			Name:   check.name,
			Value:  value,
			Passed: ok,
		}
	}
	return status, nil
}

func (h *Handler) checkMongoConnected() (string, bool) {
	err := h.store.DB.Session.Ping()
	if err == nil {
		return "Connected", true
	}
	return "Ping error: " + err.Error(), false
}

func (h *Handler) checkCollections() (string, bool) {
	names, err := h.store.DB.CollectionNames()
	if err != nil {
		return "Cannot get collections: " + err.Error(), false
	}
	var missing []string
	for _, coll := range h.store.DB.Collections() {
		found := false
		for _, name := range names {
			if name == coll.Name {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, coll.Name)
		}
	}
	if len(missing) == 0 {
		return "All required collections exist", true
	}
	return fmt.Sprintf("Missing collections: %s", missing), false
}

func (h *Handler) checkEntities() (string, bool) {
	iter := h.store.DB.Entities().Find(nil).Select(bson.D{{"_id", 1}}).Iter()
	charms, bundles, promulgated := 0, 0, 0
	var entity mongodoc.Entity
	for iter.Next(&entity) {
		if entity.URL.Series == "bundle" {
			bundles++
		} else {
			charms++
		}
		if entity.URL.User == "" {
			promulgated++
		}
	}
	if err := iter.Close(); err != nil {
		return "Cannot count entities: " + err.Error(), false
	}
	return fmt.Sprintf("%d charms; %d bundles; %d promulgated", charms, bundles, promulgated), true
}

func (h *Handler) checkIngestion() (string, bool) {
	var start time.Time
	var end time.Time
	var log mongodoc.Log
	iter := h.store.DB.Logs().
		Find(bson.D{
		{"level", mongodoc.InfoLevel},
		{"type", mongodoc.IngestionType},
	}).Sort("-time").Iter()
	for iter.Next(&log) {
		var msg string
		if err := json.Unmarshal(log.Data, &msg); err != nil {
			// an error here probably means the log isn't in the form we are looking for.
			continue
		}
		if start.IsZero() && strings.HasPrefix(msg, "ingestion started") {
			start = log.Time
		}
		if end.IsZero() && strings.HasPrefix(msg, "ingestion completed") {
			end = log.Time
		}
		if !start.IsZero() && !end.IsZero() {
			break
		}
	}
	if err := iter.Close(); err != nil {
		return "Cannot query ingestion logs: " + err.Error(), false
	}
	return fmt.Sprintf("started: %s, completed: %s", start.Format(time.RFC3339), end.Format(time.RFC3339)), !(start.IsZero() || end.IsZero())
}

// startTime holds the time that the code started running.
var startTime = time.Now()

func (h *Handler) checkServerStarted() (string, bool) {
	return startTime.String(), true
}
