// Copyright 2017 Matt Ho
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package dynastore

import (
	"context"
	"encoding/base32"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	av "github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/gorilla/securecookie"
	"github.com/gorilla/sessions"
)

const (
	DefaultTableName = "sessions"

	DefaultPrimaryKey = "id"

	// DefaultTTLField contains the default name of the ttl field
	DefaultTTLField = "ttl"
)

var (
	errStateNotFound = fmt.Errorf("state missing or deleted from store")
)

// Store provides an implementation of the gorilla sessions.Store interface backed by DynamoDB
type Store struct {
	tableName      string
	primaryKey     string
	refreshCookies bool
	enableTTL      bool
	ttlKey         string

	ddb     *dynamodb.Client
	options sessions.Options
}

// New instantiates a new Store that implements gorilla's sessions.Store interface
func New(client *dynamodb.Client, opts ...Option) *Store {
	store := &Store{
		ddb:        client,
		tableName:  DefaultTableName,
		primaryKey: DefaultPrimaryKey,
		ttlKey:     DefaultTTLField,
	}

	for _, opt := range opts {
		opt(store)
	}

	return store
}

// Get should return a cached session.
func (store *Store) Get(req *http.Request, name string) (*sessions.Session, error) {
	return sessions.GetRegistry(req).Get(store, name)
}

// New should create and return a new session.
//
// Note that New should never return a nil session, even in the case of
// an error if using the Registry infrastructure to cache the session.
func (store *Store) New(req *http.Request, name string) (*sessions.Session, error) {
	if cookie, errCookie := req.Cookie(name); errCookie == nil {
		s := sessions.NewSession(store, name)
		err := store.Load(req.Context(), cookie.Value, s)
		if err == nil {
			return s, nil
		}
	}

	s := sessions.NewSession(store, name)
	s.ID = strings.TrimRight(base32.StdEncoding.EncodeToString(securecookie.GenerateRandomKey(32)), "=")
	s.IsNew = true
	s.Options = &sessions.Options{
		Path:     store.options.Path,
		Domain:   store.options.Domain,
		MaxAge:   store.options.MaxAge,
		Secure:   store.options.Secure,
		HttpOnly: store.options.HttpOnly,
	}

	return s, nil
}

// Save should persist session to the underlying store implementation.
func (store *Store) Save(req *http.Request, w http.ResponseWriter, session *sessions.Session) error {
	err := store.Persist(req.Context(), session.Name(), session)
	if err != nil {
		return err
	}

	if session.Options != nil && session.Options.MaxAge < 0 {
		cookie := newCookie(session, session.Name(), "")
		http.SetCookie(w, cookie)
		return store.Delete(req.Context(), session.ID)
	}

	if store.canSetCookie(session) {
		cookie := newCookie(session, session.Name(), session.ID)
		http.SetCookie(w, cookie)
	}

	return nil
}

func (store *Store) canSetCookie(session *sessions.Session) bool {
	return session.IsNew || store.refreshCookies
}

func newCookie(session *sessions.Session, name, value string) *http.Cookie {
	cookie := &http.Cookie{
		Name:  name,
		Value: value,
	}

	if opts := session.Options; opts != nil {
		cookie.Path = opts.Path
		cookie.Domain = opts.Domain
		cookie.MaxAge = opts.MaxAge
		cookie.HttpOnly = opts.HttpOnly
		cookie.Secure = opts.Secure
	}

	return cookie
}

func (store *Store) Persist(ctx context.Context, name string, session *sessions.Session) error {

	session.Values[store.primaryKey] = session.ID

	v := convertToMapStringAny(session.Values)

	if store.enableTTL {
		v[store.ttlKey] = time.Now().Add(time.Second * time.Duration(store.options.MaxAge)).Unix()
	}

	items, err := av.MarshalMap(v)
	if err != nil {
		return fmt.Errorf("failed marshall session for dynamodb: %w", err)
	}

	_, err = store.ddb.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(store.tableName),
		Item:      items,
	})

	return err
}

func convertToMapStringAny(in map[any]any) map[string]any {
	out := make(map[string]any, 0)
	for i, v := range in {
		if _, ok := i.(string); !ok {
			continue
		}

		out[i.(string)] = v
	}

	return out
}

func (store *Store) Delete(ctx context.Context, id string) error {

	_, err := store.ddb.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(store.tableName),
		Key: map[string]types.AttributeValue{
			store.primaryKey: &types.AttributeValueMemberS{Value: id},
		},
	})

	return err
}

// load loads a session data from the database.
// True is returned if there is a session data in the database.
func (store *Store) Load(ctx context.Context, value string, session *sessions.Session) error {

	result, err := store.ddb.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(store.tableName),
		Key: map[string]types.AttributeValue{
			store.primaryKey: &types.AttributeValueMemberS{Value: value},
		},
	})

	if err != nil {
		return err
	}

	if result.Item == nil {
		return errStateNotFound
	}

	out := make(map[string]any, 0)

	err = attributevalue.UnmarshalMap(result.Item, &out)
	if err != nil {
		return err
	}

	for i, v := range out {
		session.Values[i] = v
	}

	if _, ok := session.Values[store.primaryKey]; ok {
		session.ID = session.Values[store.primaryKey].(string)
	}

	return err
}
