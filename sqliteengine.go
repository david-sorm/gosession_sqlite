package gosession_sqlite

import (
	"database/sql"
	"encoding/json"
	"github.com/david-sorm/gosession"
	_ "github.com/mattn/go-sqlite3"
)

const tableName =  "gosessions"

const dropStmt = "DROP TABLE "+tableName
const createStmt = "CREATE TABLE "+tableName+" (sessionID text, dataSerialized text)"
const checkStmt = "SELECT name FROM sqlite_master WHERE sql = '"+createStmt+"' AND type = 'table'"
const createSessionStmt = "INSERT INTO "+tableName+" VALUES (?, '')"
const destroySessionStmt = "DELETE FROM "+tableName+" WHERE sessionID = ?"
const readKeysStmt = "SELECT dataSerialized FROM "+tableName+" WHERE sessionID = ?"
const writeKeysStmt = "UPDATE "+tableName+" SET dataSerialized = ? WHERE sessionID = ?"

var marshaller = json.Marshal
var unmarshaller = json.Unmarshal

type key map[string]interface{}

type SqliteEngine struct {
	// Filename has to contain a path to the file which will contain the SQLite
	// database (eg. "./sessions.sqlite").
	Filename string

	initialised bool
	closed bool
	db *sql.DB
	es gosession.EngineState
}

func (se SqliteEngine) doTablesExist() bool {
	rows, _ := se.db.Query(checkStmt)
	defer rows.Close()

	compare := ""
	for rows.Next() {
		_ = rows.Scan(&compare)
		return compare == tableName
	}
	return false
}
func (se SqliteEngine) createTables() error {
	_, err := se.db.Exec(createStmt)
	return err

}

func (se SqliteEngine) saveAllKeys(sessionID string, kv map[string]interface{}) {
	raw, _ := marshaller(kv)

	_, _ = se.db.Exec(writeKeysStmt, string(raw), sessionID)
}

func (se SqliteEngine) readAllKeys(sessionID string) map[string]interface{} {
	rows, _ := se.db.Query(readKeysStmt, sessionID)
	rawString := ""
	for rows.Next() {
		err := rows.Scan(&rawString)
		if err != nil {
			panic(err)
		}
	}
	kv := make(map[string]interface{})
	_ = unmarshaller([]byte(rawString), &kv)
	return kv
}

func (se SqliteEngine) GetEngineStatePointer() *gosession.EngineState {
	return &se.es
}

// Returns a new GosessionEngine. Argument filename has to contain a path to the
// file which will contain the SQLite database (eg. "./sessions.sqlite).
// Returns GosessionEngine and an error if it encounters any problems while
// initializing it.
func (se SqliteEngine) Init() error {
	db, err := sql.Open("sqlite3", se.Filename)
	if err != nil {
		return err
	}
	// makes sure the db actually works
	err = db.Ping()
	if err != nil {
		return err
	}

	se.db = db
	if !se.doTablesExist() {
		if err = se.createTables(); err != nil {
			return err
		}
	}
	return nil
}

func (se SqliteEngine) Close() {
	_ = se.db.Close()
}

func (se SqliteEngine) SessionExists(sessionID string) bool {
	rows, _ := se.db.Query(readKeysStmt, sessionID)
	defer rows.Close()

	return rows.Next()
}

func (se SqliteEngine) CreateSession(sessionID string) {
	_, _ = se.db.Exec(createSessionStmt, sessionID)
}

func (se SqliteEngine) DestroySession(sessionID string) {
	_, _ = se.db.Exec(destroySessionStmt, sessionID)
}

func (se SqliteEngine) DestroyAllSessions() {
	_, _ = se.db.Exec(dropStmt)
	_, _ = se.db.Exec(createStmt)
}

func (se SqliteEngine) ReadKey(sessionID string, key string) interface{} {
	return se.readAllKeys(sessionID)[key]
}

func (se SqliteEngine) WriteKey(sessionID string, key string, value interface{}) {
	kv := se.readAllKeys(sessionID)
	if kv == nil {
		return
	}
	kv[key] = value
	se.saveAllKeys(sessionID, kv)
}

func (se SqliteEngine) DeleteKey(sessionID string, key string) {
	kv := se.readAllKeys(sessionID)
	if kv == nil {
		return
	}
	delete(kv, key)
	se.saveAllKeys(sessionID, kv)
}

