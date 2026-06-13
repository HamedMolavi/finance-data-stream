package config

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/HamedMolavi/finance-data-stream/types"
	"github.com/joho/godotenv"
	"github.com/sirupsen/logrus"
)

var C = SetupConfig()

type Config interface {
	LogFile() string
	App() string

	Market() int32
	LogLevel() logrus.Level
	EnabledUI() bool
	RawBufferSize() int
	SubscriptionBufferSize() int
	SubscriptionAggGoroutines() int
	MaxSymbolPerConnection() int

	PublisherBufferSize() int
	MaxPublisherWroker() int
	EmitterRowBufferSize() int
	MaxEmitterLength() int

	HttpRetries() int
	MaxDbConnections() int
	MaxCuncurrentDownload() int
	MaxPendingLength() int
	KlineBaseUrl() string
	WsBaseUrl() string
	RedisUrl() string
	SqlUrl() string
	SymbolCache() *types.SymbolsCache
	HttpAddress() string
	FP() string
	SyncUrl() string
	Server() *http.ServeMux
	Token() string
}

type config struct {
	logFile          string
	program          string
	maxWeight        uint64
	fp               string
	httpAddress      string
	enableClickHouse bool
	enableRedis      bool
	enableSql        bool
	enable300        bool

	market          int32
	symbolCache     *types.SymbolsCache
	logLevel        logrus.Level
	enabledFileHook bool
	rawBufferSize   int

	// Redis
	publisherMaxBatchSize int
	publisherMinBatchSize int
	cacherMaxBatchSize    int
	cacherMinBatchSize    int
	publisherChannelSize  int
	publisherPoolSize     int
	publisherWroker       int

	subscriptionBufferSize    int
	subscriptionAggGoroutines int
	maxSymbolPerConnection    int
	httpRetries               int
	maxDbConnections          int
	maxEmitterLength          int
	lenSymbols                int
	maxCuncurrentDownload     int
	emitterRowBufferSize      int
	klineBaseUrl              string
	wsBaseUrl                 string
	liveRedisUrl              string
	cacheRedisUrl             string
	sqlUrl                    string
	sqlMode                   types.SQL_MODE
	sqlTimeout                int
	sqlAttempts               int
	sClickHouseUrl            string
	mClickHouseUrl            string
	forceClickHouse           bool
	syncUrl                   string
	server                    *http.ServeMux
	tokenLastUpdate           time.Time
	tokenMu                   sync.Mutex
	liveToken                 string
	histToken                 string
	logSymbol                 string
	logInterval               types.Interval
	// warmUpAlert               types.AlertsGet
	alertImpactUrl string
}

func SetupConfig() *config {
	c := &config{}

	exe, _ := os.Executable()      // e.g. /tmp/go-build1424942958/b001/exe/live
	c.program = filepath.Base(exe) // e.g. live
	// It WILL NOT OVERRIDE an env variable that already exists
	dotEnvErr := godotenv.Load()
	if dotEnvErr != nil {
		logrus.Warnln(dotEnvErr)
	}
	logrus.Println("Running in", c.program, "mode.")
	if c.program == "source.test" {
		return c
	}

	/////////
	c.enable300 = true
	if strings.ToUpper(os.Getenv("ENABLE_300")) == "FALSE" {
		c.enable300 = false
	}
	c.enableRedis = true
	if strings.ToUpper(os.Getenv("ENABLE_REDIS")) == "FALSE" {
		c.enableRedis = false
	}
	if strings.ToUpper(os.Getenv("ENABLE_CLICKHOUSE")) == "TRUE" {
		c.enableClickHouse = true
	}
	c.enableSql = true
	if strings.ToUpper(os.Getenv("ENABLE_SQL")) == "FALSE" {
		c.enableSql = false
	}
	/////////
	if market := strings.ToUpper(os.Getenv("MARKET")); market == "" || market == "SPOT" {
		// Spot market configs
		c.maxWeight = 5900
		c.fp = "spot_perms.bin"
		if c.program == "live" {
			os.MkdirAll("/var/log/binancego/live", os.ModePerm)
			c.logFile = "/var/log/binancego/live/spot.log"
		} else {
			os.MkdirAll("/var/log/binancego/history", os.ModePerm)
			c.logFile = "/var/log/binancego/history/spot.log"
		}
		c.market = types.SPOT_MARKET
		c.wsBaseUrl = types.SPOT_WS_BASE_URL
		c.logSymbol = "BTCUSDT"
	} else if market == "NOBITEX" {
		c.maxWeight = 600
		c.fp = "nobitex_perms.bin"
		if c.program == "live" {
			os.MkdirAll("/var/log/binancego/live", os.ModePerm)
			c.logFile = "/var/log/binancego/live/nobitex.log"
		} else {
			os.MkdirAll("/var/log/binancego/history", os.ModePerm)
			c.logFile = "/var/log/binancego/history/nobitex.log"
		}
		c.market = types.NOBITEX_MARKET
		c.wsBaseUrl = types.NOBITEX_WS_BASE_URL
		c.logSymbol = "BTCIRT"

	} else if market == "BITYCLE" {
		c.maxWeight = 600
		c.fp = "nobitex_perms.bin"
		if c.program == "live" {
			os.MkdirAll("/var/log/binancego/live", os.ModePerm)
			c.logFile = "/var/log/binancego/live/bitycle.log"
		} else {
			os.MkdirAll("/var/log/binancego/history", os.ModePerm)
			c.logFile = "/var/log/binancego/history/bitycle.log"
		}
		c.market = types.BITYCLE_MARKET
		c.wsBaseUrl = types.BITYCLE_WS_BASE_URL
		c.logSymbol = "BTCIRT"
	} else if market == "FOREX" {
		c.maxWeight = 300
		c.fp = "forex_perms.bin"
		if c.program == "live" {
			os.MkdirAll("/var/log/binancego/live", os.ModePerm)
			c.logFile = "/var/log/binancego/live/forex.log"
		} else {
			os.MkdirAll("/var/log/binancego/history", os.ModePerm)
			c.logFile = "/var/log/binancego/history/forex.log"
		}
		c.market = types.FOREX_MARKET
		c.wsBaseUrl = types.FOREX_WS_BASE_URL
		c.logSymbol = "BTCUSDT"
	} else {
		// Futures market configs
		c.maxWeight = 2200
		c.fp = "futures_perms.bin"
		if c.program == "live" {
			os.MkdirAll("/var/log/binancego/live", os.ModePerm)
			c.logFile = "/var/log/binancego/live/futures.log"
		} else {
			os.MkdirAll("/var/log/binancego/history", os.ModePerm)
			c.logFile = "/var/log/binancego/history/futures.log"
		}
		c.market = types.FUTURES_MARKET
		c.wsBaseUrl = types.FUTURES_WS_BASE_URL
		c.logSymbol = "OANDA_XAUUSD"
	}
	if logSymbol := strings.ToUpper(os.Getenv("LOG_SYMBOL")); logSymbol != "" {
		c.logSymbol = logSymbol
	}

	if logInterval := os.Getenv("LOG_INTERVAL"); logInterval != "" {
		c.logInterval = types.TIMEFRAME_INTERVAL_MAP[c.market][types.Timeframe(logInterval)]
	} else {
		c.logInterval = types.M1
	}
	fmt.Println("Logging symbol", c.logSymbol, "interval", c.logInterval)
	if str := strings.ToUpper(os.Getenv("SQL_MODE")); str == "" || str == "COPY" {
		c.sqlMode = types.SQL_COPY_MODE
	} else {
		c.sqlMode = types.SQL_INSERT_MODE
	}
	if str := os.Getenv("SQL_URL"); str != "" {
		c.sqlUrl = str
	} else {
		c.sqlUrl = "postgres://localhost:5432/postgres"
	}
	if sqlAttempts, err := strconv.Atoi(os.Getenv("SQL_ATTEMPTS")); err == nil {
		c.sqlAttempts = sqlAttempts
	} else {
		c.sqlAttempts = 5
	}
	symbolsSqlUrl := os.Getenv("SYMBOLS_SQL_URL")
	if symbolsSqlUrl == "" {
		symbolsSqlUrl = c.sqlUrl
	}
	// if alertGetUrl := os.Getenv("ALERT_GET_URL"); alertGetUrl != "" {
	// 	c.warmUpAlert = warmupAlertFn(alertGetUrl)
	// } else {
	// 	c.warmUpAlert = warmupAlertFn("https://back.tradecheck.co/api/doctor/price-alerts/unsend")
	// }
	// if alertImpactUrl := os.Getenv("ALERT_SEND_URL"); alertImpactUrl != "" {
	// 	c.alertImpactUrl = alertImpactUrl
	// } else {
	// 	c.alertImpactUrl = "https://back.tradecheck.co/api/doctor/price-alerts/impact/%v"
	// }

	if testSymbol := os.Getenv("TEST_SYMBOL"); testSymbol != "" {
		c.symbolCache = &types.SymbolsCache{
			Symbols: map[types.Symbol]types.Oldest{types.Symbol(testSymbol): types.Oldest("")},
			Loaded:  time.Now(),
		}
	} else {
		cache := SetupCache(c.market, symbolsSqlUrl)
		c.symbolCache = cache
	}
	// copied := make(map[Symbol]Oldest, 10)
	// copied[Symbol("CAPITALCOM_EU50")] = Oldest("")
	// sliced := slices.Collect(maps.Keys(cache.Symbols))
	// for range 10 {
	// 	copied[sliced[rand.IntN(len(sliced))]] = Oldest("")
	// }
	// c.symbolCache = &types.SymbolsCache{
	// 	Symbols: map[Symbol]Oldest{Symbol("BOMEUSDC_FUTURES"): Oldest("")}, // binance
	// 	// Symbols: map[Symbol]Oldest{Symbol("ADAJPY"): Oldest("")}, // binance
	// 	// Symbols: map[Symbol]Oldest{Symbol("OANDA_XAUUSD"): Oldest("")}, // forex
	// 	// Symbols: map[Symbol]Oldest{Symbol("CRYPTOCAP_TOTAL"): Oldest("")}, // forex
	// 	Symbols: map[Symbol]Oldest{Symbol("USDTIRT"): Oldest("")}, // nobitex
	// 	// Symbols: copied,
	// 	Loaded: time.Now(),
	// }

	c.lenSymbols = len(c.symbolCache.Symbols)
	///////// Log config
	if level, err := logrus.ParseLevel(strings.ToLower(os.Getenv("LOG_LEVEL"))); err == nil {
		c.logLevel = level
	} else {
		c.logLevel = logrus.ErrorLevel
	}
	c.enabledFileHook = strings.ToLower(os.Getenv("FILE_HOOK")) == "true"

	///////// *************************
	//					Redis config											Redis config														Redis config											Redis config
	///////// *************************
	if publisherWroker, err := strconv.Atoi(os.Getenv("PUBLISHER_WORKER")); err == nil {
		c.publisherWroker = publisherWroker
	} else {
		c.publisherWroker = 8
	}
	if publisherMaxBatchSize, err := strconv.Atoi(os.Getenv("PUBLISHER_MAX_BATCH_SIZE")); err == nil {
		c.publisherMaxBatchSize = publisherMaxBatchSize
	} else {
		c.publisherMaxBatchSize = 256
	}
	if publisherMinBatchSize, err := strconv.Atoi(os.Getenv("PUBLISHER_MIN_BATCH_SIZE")); err == nil {
		c.publisherMinBatchSize = publisherMinBatchSize
	} else {
		c.publisherMinBatchSize = 8
	}
	if cacherMaxBatchSize, err := strconv.Atoi(os.Getenv("CACHER_MAX_BATCH_SIZE")); err == nil {
		c.cacherMaxBatchSize = cacherMaxBatchSize
	} else {
		c.cacherMaxBatchSize = 256
	}
	if cacherMinBatchSize, err := strconv.Atoi(os.Getenv("CACHER_MIN_BATCH_SIZE")); err == nil {
		c.cacherMinBatchSize = cacherMinBatchSize
	} else {
		c.cacherMinBatchSize = 8
	}
	if publisherChannelSize, err := strconv.Atoi(os.Getenv("PUBLISHER_CHANNEL_SIZE")); err == nil {
		c.publisherChannelSize = publisherChannelSize
	} else {
		c.publisherChannelSize = 1024
	}
	if publisherPoolSize, err := strconv.Atoi(os.Getenv("PUBLISHER_POOL_SIZE")); err == nil {
		c.publisherPoolSize = publisherPoolSize
	} else {
		c.publisherPoolSize = 128
	}
	///////// *************************
	//				SQL config										SQL config													SQL config										SQL config
	///////// *************************
	if sqlTimeout, err := strconv.Atoi(os.Getenv("SQL_TIMEOUT")); err == nil {
		c.sqlTimeout = sqlTimeout
	} else {
		c.sqlTimeout = 30
	}
	///////// *************************
	//				Emitter config										Emitter config													Emitter config										Emitter config
	///////// *************************
	if maxEmitterLength, err := strconv.Atoi(os.Getenv("MAX_EMITTER_LENGTH")); err == nil {
		c.maxEmitterLength = maxEmitterLength
	} else {
		c.maxEmitterLength = 47970
	}
	if emitterRowBufferSize, err := strconv.Atoi(os.Getenv("EMITTER_BUFFER_SIZE")); err == nil {
		c.emitterRowBufferSize = emitterRowBufferSize
	} else {
		c.emitterRowBufferSize = 1024
	}
	if maxDbConnections, err := strconv.Atoi(os.Getenv("MAX_DB_CONN")); err == nil {
		c.maxDbConnections = maxDbConnections
	} else {
		c.maxDbConnections = 10
	}
	///////// *************************
	//				Manager config										Manager config													Manager config										Manager config
	///////// *************************
	if httpRetries, err := strconv.Atoi(os.Getenv("HTTP_RETRIES")); err == nil {
		c.httpRetries = httpRetries
	} else {
		c.httpRetries = 10
	}
	if maxCuncurrentDownload, err := strconv.Atoi(os.Getenv("MAX_CONCURRENT_DOWNLOAD")); err == nil {
		c.maxCuncurrentDownload = maxCuncurrentDownload
	} else {
		c.maxCuncurrentDownload = 10
	}
	if str := os.Getenv("SYNC_URL"); str != "" {
		if strings.Contains(str, "{port}") {
			var port string
			switch c.market {
			case types.FOREX_MARKET:
				port = "3091"
			case types.NOBITEX_MARKET, types.BITYCLE_MARKET:
				port = "3093"
			case types.FUTURES_MARKET:
				port = "3092"
			case types.SPOT_MARKET:
				port = "3094"
			}
			str = strings.ReplaceAll(str, "{port}", port)
		}
		c.syncUrl = str
	} else {
		c.syncUrl = "http://localhost:3061/sync"
	}
	///////// *************************
	//				Subscription config										Subscription config													Subscription config										Subscription config
	///////// *************************
	if rbs, err := strconv.Atoi(os.Getenv("RAW_BUFFER_SIZE")); err == nil {
		c.rawBufferSize = rbs
	} else {
		c.rawBufferSize = 1024
	}
	if sag, err := strconv.Atoi(os.Getenv("SUBSCRIPTION_AGG_GOROUTINES")); err == nil {
		c.subscriptionAggGoroutines = sag
	} else {
		c.subscriptionAggGoroutines = 50
	}
	if rbs, err := strconv.Atoi(os.Getenv("SUBSCRIPTION_BUFFER_SIZE")); err == nil {
		c.subscriptionBufferSize = rbs
	} else {
		c.subscriptionBufferSize = 1024
	}
	if rbs, err := strconv.Atoi(os.Getenv("MAX_SYMBOL_PER_CONNECTION")); err == nil {
		c.maxSymbolPerConnection = rbs
	} else {
		c.maxSymbolPerConnection = 30
	}
	if str := os.Getenv("KLINE_BASE_URL"); str != "" {
		c.klineBaseUrl = str
	} else {
		c.klineBaseUrl = "https://api.binance.com/api/v3/klines"
	}
	if str := os.Getenv("FIRST_TOKEN"); str != "" {
		c.liveToken = str
	} else if str2 := os.Getenv("TOKEN"); str2 != "" {
		c.liveToken = str2
	}
	if str := os.Getenv("SECOND_TOKEN"); str != "" {
		c.histToken = str
	} else {
		c.histToken = c.liveToken
	}
	c.tokenLastUpdate = time.UnixMilli(0)
	///////// *************************
	//				Server config										Server config													Server config										Server config
	///////// *************************
	if str := os.Getenv("HTTP_ADDRESS"); str != "" {
		c.httpAddress = str
	} else {
		first := 1
		last := int32(45)
		if c.program == "live" {
			first += 0 // 1
		} else {
			first += 1 // 2
		}
		last += c.market // spot=0 future=1 forex=2 nobitex=3 bitycle=4
		c.httpAddress = fmt.Sprintf(":%d23%d", first, last)
	}
	if str := os.Getenv("REDIS_URL"); str != "" {
		c.liveRedisUrl = str
	} else {
		c.liveRedisUrl = "redis://127.0.0.1:6379"
	}
	if str := os.Getenv("CACHE_REDIS_URL"); str != "" {
		c.cacheRedisUrl = str
	} else {
		c.cacheRedisUrl = c.liveRedisUrl
	}
	c.server = http.NewServeMux()
	// Wrap mux with a small logging middleware
	logged := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		c.server.ServeHTTP(w, r)
		logrus.Infof("%s %s %s", r.Method, r.URL.String(), time.Since(start))
	})

	logrus.Infof("server starting on %s", c.httpAddress)
	go func() {
		if err := http.ListenAndServe(c.httpAddress, logged); err != nil {
			logrus.Error(err)
			os.Exit(1)
		}
	}()
	///////// *************************
	//				Clickhouse config										Clickhouse config													Clickhouse config										Clickhouse config
	///////// *************************
	if str := os.Getenv("S_CLICK_HOUSE_URL"); str != "" {
		c.sClickHouseUrl = str
	} else {
		if str := os.Getenv("CLICK_HOUSE_URL"); str != "" {
			c.sClickHouseUrl = str
		} else {
			if str := os.Getenv("M_CLICK_HOUSE_URL"); str != "" {
				c.sClickHouseUrl = str
			} else {
				c.sClickHouseUrl = "tcp://127.0.0.1:9000?username=default&password=default&database=default"
			}
		}
	}
	if str := os.Getenv("M_CLICK_HOUSE_URL"); str != "" {
		c.mClickHouseUrl = str
	} else {
		if str := os.Getenv("CLICK_HOUSE_URL"); str != "" {
			c.mClickHouseUrl = str
		} else {
			if str := os.Getenv("S_CLICK_HOUSE_URL"); str != "" {
				c.mClickHouseUrl = str
			} else {
				c.mClickHouseUrl = "tcp://127.0.0.1:9000?username=default&password=default&database=default"
			}
		}
	}
	c.forceClickHouse = os.Getenv("FORCE_CH") == "true"
	/////////
	/////////
	return c
}

// Main config
func (c *config) App() string                      { return c.program }
func (c *config) HttpAddress() string              { return c.httpAddress }
func (c *config) Market() int32                    { return c.market }
func (c *config) LogLevel() logrus.Level           { return c.logLevel }
func (c *config) EnabledFileHook() bool            { return c.enabledFileHook }
func (c *config) SymbolCache() *types.SymbolsCache { return c.symbolCache }
func (c *config) LogFile() string                  { return c.logFile }
func (c *config) FP() string                       { return c.fp }
func (c *config) KlineBaseUrl() string             { return c.klineBaseUrl }
func (c *config) WsBaseUrl() string                { return c.wsBaseUrl }
func (c *config) LiveRedisUrl() string             { return c.liveRedisUrl }
func (c *config) CacheRedisUrl() string            { return c.cacheRedisUrl }
func (c *config) SqlUrl() string                   { return c.sqlUrl }
func (c *config) SqlMode() types.SQL_MODE          { return c.sqlMode }
func (c *config) SqlAttempts() int                 { return c.sqlAttempts }
func (c *config) SyncUrl() string                  { return c.syncUrl }
func (c *config) SecClickHouseUrl() string         { return c.sClickHouseUrl }
func (c *config) MinClickHouseUrl() string         { return c.mClickHouseUrl }
func (c *config) ClickHouseUrlSet() map[types.Interval]string {
	return map[types.Interval]string{
		types.S1:  c.sClickHouseUrl,
		types.S5:  c.sClickHouseUrl,
		types.S15: c.sClickHouseUrl,
		types.S30: c.sClickHouseUrl,
		types.M1:  c.mClickHouseUrl,
		types.M5:  c.mClickHouseUrl,
		types.M15: c.mClickHouseUrl,
		types.M30: c.mClickHouseUrl,
		types.H1:  c.mClickHouseUrl,
		types.H4:  c.mClickHouseUrl,
		types.D1:  c.mClickHouseUrl,
		types.W1:  c.mClickHouseUrl,
		types.Mo1: c.mClickHouseUrl,
	}
}
func (c *config) EnableClickHouse() bool { return c.enableClickHouse }
func (c *config) ForceClickHouse() bool  { return c.forceClickHouse }
func (c *config) EnableRedis() bool      { return c.enableRedis }
func (c *config) EnableSql() bool        { return c.enableSql }
func (c *config) Enable300() bool        { return c.enable300 }
func (c *config) Server() *http.ServeMux { return c.server }
func (c *config) UpdateToken(loginFn func() (string, error)) {
	if time.Now().After(c.tokenLastUpdate.Add(time.Hour)) {
		token, err := loginFn()
		if err != nil {
			c.tokenMu.Lock()
			defer c.tokenMu.Unlock()
			c.liveToken = token
		} else {
			fmt.Println("Error refreshing token", err)
		}
	}
}
func (c *config) SwapTokens() {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	liveToken := c.liveToken
	histToken := c.histToken
	c.liveToken = histToken
	c.histToken = liveToken
}
func (c *config) HistoryToken() string {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	return c.histToken
}
func (c *config) LiveToken() string {
	c.tokenMu.Lock()
	defer c.tokenMu.Unlock()
	return c.liveToken
}
func (c *config) LogSymbol() string           { return c.logSymbol }
func (c *config) LogInterval() types.Interval { return c.logInterval }

// Socket config
func (c *config) RawBufferSize() int { return c.rawBufferSize }
func (c *config) MaxSymbolPerConnection() int {
	if c.market == types.FOREX_MARKET || c.market == types.NOBITEX_MARKET {
		return 1
	}
	return min(c.maxSymbolPerConnection, len(c.symbolCache.Symbols))
}

// Redis config
func (c *config) PublisherWroker() int       { return c.publisherWroker }
func (c *config) PublisherChannelSize() int  { return c.publisherChannelSize }
func (c *config) PublisherMaxBatchSize() int { return c.publisherMaxBatchSize }
func (c *config) PublisherMinBatchSize() int { return c.publisherMinBatchSize }
func (c *config) CacherMaxBatchSize() int    { return c.cacherMaxBatchSize }
func (c *config) CacherMinBatchSize() int    { return c.cacherMinBatchSize }
func (c *config) PublisherPoolSize() int     { return c.publisherPoolSize }

func (c *config) SubscriptionBufferSize() int    { return c.subscriptionBufferSize }
func (c *config) SubscriptionAggGoroutines() int { return c.subscriptionAggGoroutines }

// Manager config
// func (c *config) WarmUpAlert() types.AlertsGet { return c.warmUpAlert }
func (c *config) AlertImpactUrl() string     { return c.alertImpactUrl }
func (c *config) HttpRetries() int           { return c.httpRetries }
func (c *config) MaxDbConnections() int      { return c.maxDbConnections }
func (c *config) MaxCuncurrentDownload() int { return c.maxCuncurrentDownload }
func (c *config) MaxWeight() uint64          { return c.maxWeight }

// Emitter config
func (c *config) MaxEmitterLength() int     { return c.maxEmitterLength }
func (c *config) SqlTimeout() int           { return c.sqlTimeout }
func (c *config) EmitterRowBufferSize() int { return c.emitterRowBufferSize }
func (c *config) EmitterThreashold(interval string) int {
	if interval == "1s" {
		return 30 * c.lenSymbols
	}
	return c.lenSymbols + 100
}
