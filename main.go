package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
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

type DatabaseConfig struct {
	User     string
	Password string
	Host     string
	Port     string
	Service  string
}

func main() {
	// 環境変数の読み込み
	loadEnv()

	// データベース設定の取得
	mainDBConfig, slaveDBConfig := getDBConfigs()
	slackWebhookEndpoint := os.Getenv("SLACK_WEBHOOK_ENDPOINT")

	// スロークエリの取得
	slowQueries := getSlowQueries(mainDBConfig, "データベース接続エラー", "SQLクエリ実行エラー")
	slowQueriesSlave := getSlowQueries(slaveDBConfig, "スレーブデータベース接続エラー", "スレーブSQLクエリ実行エラー")

	// スロークエリが存在する場合、Slackに通知
	if len(slowQueries)+len(slowQueriesSlave) > 0 {
		sendSlackNotification(slackWebhookEndpoint, slowQueries, slowQueriesSlave)
	} else {
		log.Println("スロークエリは検出されませんでした")
	}
}

func loadEnv() {
	argEnv := flag.String("env", ".env", "環境変数ファイルのパス")
	flag.Parse()
	log.Printf("Loading .env file from : %s", *argEnv)
	if err := godotenv.Load(*argEnv); err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}
}

func getDBConfigs() (DatabaseConfig, DatabaseConfig) {
	mainConfig := DatabaseConfig{
		User:     os.Getenv("DB_USER"),
		Password: os.Getenv("DB_PASS"),
		Host:     os.Getenv("DB_HOST"),
		Port:     os.Getenv("DB_PORT"),
		Service:  os.Getenv("DB_SERVICE"),
	}

	slaveConfig := DatabaseConfig{
		User:     os.Getenv("DB_USER"),
		Password: os.Getenv("DB_PASS"),
		Host:     os.Getenv("DB_HOST_SLAVE"),
		Port:     os.Getenv("DB_PORT"),
		Service:  os.Getenv("DB_SERVICE"),
	}

	return mainConfig, slaveConfig
}

func createConnString(config DatabaseConfig) string {
	return fmt.Sprintf("%s/%s@%s:%s/%s", config.User, config.Password, config.Host, config.Port, config.Service)
}

func getSlowQueries(config DatabaseConfig, connectErrorMsg, queryErrorMsg string) []SlowQuery {
	connString := createConnString(config)
	db, err := sql.Open("godror", connString)
	if err != nil {
		log.Fatalf(connectErrorMsg+": %v", err)
	}
	defer db.Close()

	slowQueries, err := selectSlowQueries(db)
	if err != nil {
		log.Fatalf(queryErrorMsg+": %v", err)
	}

	return slowQueries
}

func sendSlackNotification(slackWebhookEndpoint string, slowQueries, slowQueriesSlave []SlowQuery) {
	message := formatSlackMessage(slowQueries, "本番DB１号機")
	message += formatSlackMessage(slowQueriesSlave, "本番DB２号機")
	payload := map[string]string{"text": message}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		log.Printf("JSONマーシャリングエラー: %v", err)
		return
	}

	resp, err := http.Post(slackWebhookEndpoint, "application/json", bytes.NewBuffer(payloadBytes))
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
}

func selectSlowQueries(db *sql.DB) ([]SlowQuery, error) {
	query := `
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
    `

	rows, err := db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("クエリ実行エラー: %v", err)
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
