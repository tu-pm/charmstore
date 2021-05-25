package charmstore

import (
	"github.com/juju/charmrepo/v6/csclient/params"
	"golang.org/x/crypto/bcrypt"
	"gopkg.in/errgo.v1"
	"gopkg.in/juju/charmstore.v5/internal/mongodoc"
	"gopkg.in/mgo.v2/bson"
)

func (s *Store) ListUsers() []string {
	var (
		users []string
		r     mongodoc.User
	)
	iter := s.DB.Users().Find(bson.D{}).Iter()
	for iter.Next(&r) {
		users = append(users, r.Username)
	}
	return users
}

func (s *Store) AddUser(user *mongodoc.User) error {
	count, err := s.DB.Users().Find(bson.D{{"username", user.Username}}).Count()
	if err != nil || count > 0 {
		return errgo.Newf("user %s already exists", user.Username)
	}
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(user.Password), bcrypt.DefaultCost)
	if err != nil {
		return errgo.Mask(err)
	}
	dbUser := mongodoc.User{Username: user.Username, Password: string(hashedPassword)}
	err = s.DB.Users().Insert(&dbUser)
	return err
}

func (s *Store) DeleteUser(username string) error {
	err := s.DB.Users().Remove(bson.D{{"username", username}})
	if err != nil {
		return errgo.Mask(err, errgo.Is(params.ErrNotFound))
	}
	return nil
}

func (s *Store) ValidateUser(user *mongodoc.User) bool {
	var dbUser mongodoc.User
	err := s.DB.Users().Find(bson.D{{"username", user.Username}}).One(&dbUser)
	if err == nil {
		err = bcrypt.CompareHashAndPassword([]byte(dbUser.Password), []byte(user.Password))
	}
	return err == nil
}
