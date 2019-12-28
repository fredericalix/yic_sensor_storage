package main

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/gofrs/uuid"
	_ "github.com/lib/pq" // PostgreSQL driver
)

// Store interface to store Sensor type
type Store interface {
	NewSensor(Sensor) error
	BatchSensors(sensors []Sensor) error
	FindLastByAID(aid uuid.UUID) ([]Sensor, error)
	Purge(aid uuid.UUID) error
}

// Sensor represent a generic sensors data
type Sensor struct {
	AID        uuid.UUID `json:"aid"`
	SID        uuid.UUID `json:"sid"`
	ReceivedAt time.Time `json:"received_at"`

	Data json.RawMessage `json:"data"`
}

// PostgreSQL handle the storage of sensor-storage service
type PostgreSQL struct {
	db *sql.DB
}

// NewPostgreSQL create a new PostgreSQL aut.Store
func NewPostgreSQL(uri string) Store {
	db, err := sql.Open("postgres", uri)
	if err != nil {
		return nil
	}
	// test the connection
	err = db.Ping()
	if err != nil {
		panic(err)
	}

	pg := &PostgreSQL{db: db}
	if err := pg.createSchema(); err != nil {
		panic(err)
	}
	return pg
}

func (pg *PostgreSQL) createSchema() (err error) {
	query := `CREATE TABLE IF NOT EXISTS sensors (
		aid UUID NOT NULL,
		sid UUID NOT NULL,
		received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		data JSONB NOT NULL
	);
	CREATE INDEX IF NOT EXISTS sensors_aid_sid_rec ON sensors (aid,sid,received_at);`
	_, err = pg.db.Query(query)
	if err != nil {
		return
	}

	return
}

// NewSensor store a Sensor into DB
func (pg *PostgreSQL) NewSensor(s Sensor) error {
	query := `INSERT INTO sensors(aid, sid, received_at, data) VALUES($1,$2,$3,$4)`
	_, err := pg.db.Exec(query, s.AID, s.SID, s.ReceivedAt, s.Data)
	return err
}

// BatchSensors insert a list a sensors in one transaction
func (pg *PostgreSQL) BatchSensors(sensors []Sensor) error {
	tx, err := pg.db.Begin()
	if err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO sensors(aid, sid, received_at, data) VALUES($1,$2,$3,$4)`)
	if err != nil {
		tx.Rollback()
		return err
	}
	for _, s := range sensors {
		_, err := stmt.Exec(s.AID, s.SID, s.ReceivedAt, s.Data)
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}

// FindLastByAID find the latest sensors data for each sensor id from a given account id
func (pg *PostgreSQL) FindLastByAID(aid uuid.UUID) (sensors []Sensor, err error) {
	query := `SELECT s.aid, s.sid, s.received_at, s.data
	FROM (
		SELECT sid, MAX(received_at) AS maxt
		FROM sensors
		WHERE aid = $1
	    GROUP BY sid
	) m 
	JOIN sensors AS s ON m.sid = s.sid AND m.maxt = s.received_at AND s.aid = $1;`
	rows, err := pg.db.Query(query, aid)
	if err == sql.ErrNoRows {
		return sensors, nil
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var s Sensor
		err = rows.Scan(&s.AID, &s.SID, &s.ReceivedAt, &s.Data)
		if err != nil {
			return nil, err
		}
		sensors = append(sensors, s)
	}
	return sensors, nil
}

// Purge delete every sensors for a gien account ID
func (pg *PostgreSQL) Purge(aid uuid.UUID) error {
	query := "DELETE FROM sensors WHERE aid = $1;"
	_, err := pg.db.Exec(query, aid)
	return err
}
