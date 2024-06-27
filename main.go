package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	_ "github.com/godror/godror"
	"github.com/joho/godotenv"
)

type SlowQuery struct {
	SID            string
	Serial         string
	Username       string
	Machine        string
	Program        string
	SQLID          string
	SQLText        string
	Event          string
	WaitClass      string
	SecondsInWait  int
	MinutesRunning float64
	Status         string
	KillSessionSQL string
}

func main() {
	// データベース接続情報
	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file")
	}

	dbUser := os.Getenv("DB_USER")
	dbPass := os.Getenv("DB_PASS")
	dbHost := os.Getenv("DB_HOST")
	dbHost_slave := os.Getenv("DB_HOST_SLAVE")
	dbPort := os.Getenv("DB_PORT")
	dbService := os.Getenv("DB_SERVICE")
	// Slack Bot Token
	slackWebhookEndpoint := os.Getenv("SLACK_WEBHOOK_ENDPOINT")
	// slackChannel := os.Getenv("SLACK_CHANNEL")
	// データベース接続文字列
	connString := fmt.Sprintf("%s/%s@%s:%s/%s", dbUser, dbPass, dbHost, dbPort, dbService)
	connString_slave := fmt.Sprintf("%s/%s@%s:%s/%s", dbUser, dbPass, dbHost_slave, dbPort, dbService)
	// データベース接続
	db, err := sql.Open("godror", connString)
	if err != nil {
		log.Fatalf("データベース接続エラー: %v", err)
	}
	defer db.Close()
	// SQLクエリ実行
	var slowQueries []SlowQuery
	slowQueries, err = selectSlowQueries(db)
	if err != nil {
		log.Fatalf("SQLクエリ実行エラー: %v", err)
	}

	// スレーブデータベース接続
	db_slave, err := sql.Open("godror", connString_slave)
	if err != nil {
		log.Fatalf("スレーブデータベース接続エラー: %v", err)
	}
	defer db_slave.Close()
	// SQLクエリ実行
	var slowQueries_slave []SlowQuery
	slowQueries_slave, err = selectSlowQueries(db_slave)
	if err != nil {
		log.Fatalf("スレーブSQLクエリ実行エラー: %v", err)
	}

	if len(slowQueries)+len(slowQueries_slave) > 0 {
		// Slack通知
		webhookURL := slackWebhookEndpoint
		message := formatSlackMessage(slowQueries, "本番DB１号機")
		message += formatSlackMessage(slowQueries_slave, "本番DB２号機")
		payload := map[string]string{"text": message}
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			log.Printf("JSONマーシャリングエラー: %v", err)
			return
		}
		resp, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(payloadBytes))
		if err != nil {
			log.Printf("Slack通知エラー: %v", err)
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			log.Printf("Slack通知失敗: ステータスコード %d", resp.StatusCode)
		} else {
			log.Println("Slack通知を送信しました")
		}
	} else {
		log.Println("スロークエリは検出されませんでした")
	}
}

func selectSlowQueries(db *sql.DB) ([]SlowQuery, error) {
	rows, err := db.Query(`
	WITH long_running_queries AS (
		SELECT
			s.sid,
			s.serial#,
			s.username,
			s.machine,
			s.program,
			s.sql_id,
			q.sql_text,
			s.event,
			s.wait_class,
			s.seconds_in_wait,
			ROUND((SYSDATE - s.sql_exec_start) * 24 * 60, 2) as minutes_running,
			s.status
		FROM
			v$session s
		JOIN
			v$sql q ON s.sql_id = q.sql_id
		WHERE
			s.type = 'USER'
			AND s.status = 'ACTIVE'
			AND s.sql_exec_start IS NOT NULL
			AND (
				(SYSDATE - s.sql_exec_start) * 24 * 60 >= 30
				OR s.seconds_in_wait > 300
			)
	)
	SELECT
		lrq.*,
		'ALTER SYSTEM KILL SESSION ''' || lrq.sid || ',' || lrq.serial# || ''' IMMEDIATE;' AS kill_session_sql
	FROM
		long_running_queries lrq
	ORDER BY
		lrq.minutes_running DESC
	`)
	if err != nil {
		log.Fatalf("クエリ実行エラー: %v", err)
	}
	defer rows.Close()
	var slowQueries []SlowQuery
	for rows.Next() {
		var sq SlowQuery
		err := rows.Scan(
			&sq.SID, &sq.Serial, &sq.Username, &sq.Machine, &sq.Program,
			&sq.SQLID, &sq.SQLText, &sq.Event, &sq.WaitClass, &sq.SecondsInWait,
			&sq.MinutesRunning, &sq.Status, &sq.KillSessionSQL,
		)
		if err != nil {
			log.Printf("行のスキャンエラー: %v", err)
			continue
		}
		slowQueries = append(slowQueries, sq)
	}

	return slowQueries, nil
}

func formatSlackMessage(queries []SlowQuery, dbName string) string {
	if len(queries) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("* %sにてスロークエリが検出されました*\n\n", dbName))
	for _, q := range queries {
		sb.WriteString(fmt.Sprintf("> *ユーザー:* %s, ", q.Username))
		sb.WriteString(fmt.Sprintf(" *マシン:* %s, ", q.Machine))
		sb.WriteString(fmt.Sprintf(" *プログラム:* %s, ", q.Program))
		sb.WriteString(fmt.Sprintf(" *実行時間:* %.2f分\n", q.MinutesRunning))
		sb.WriteString(fmt.Sprintf("> *SQL ID:* %s\n", q.SQLID))
		sb.WriteString(fmt.Sprintf("> *SQL:* ```%s```\n", q.SQLText))
		sb.WriteString(fmt.Sprintf("> *停止コマンド(%sで実行してください):* ```%s```\n", dbName, q.KillSessionSQL))
		sb.WriteString("\n")
	}
	return sb.String()
}
