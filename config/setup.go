package config

import (
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/HamedMolavi/finance-data-stream/types"
	_ "github.com/lib/pq" // PostgreSQL driver

	"github.com/sirupsen/logrus"
)

func futureOldestMonth(symbol string) (oldest string) {
	listingURL := fmt.Sprintf(types.FUTURE_SYMBOL_MONTH_LISTING_URL, symbol)
	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Get(listingURL)
	if err != nil {
		fmt.Println("Error in request", err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Println("status error", resp.StatusCode)
		return
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading body", err)
		return
	}
	var res listBucketResult
	if err := xml.Unmarshal(body, &res); err != nil {
		fmt.Println("XML unmarshal error:", err)
		return
	}
	// regex to match YYYY-MM.zip at end of filename
	dateRe := regexp.MustCompile(`(\d{4})-(\d{2})\.zip$`)
	const INF = int64(1<<62 - 1)
	minMonthIdx := INF
	for _, c := range res.Contents {
		key := strings.TrimSpace(c.Key)
		if key == "" {
			continue
		}
		// skip CHECKSUM files
		if strings.HasSuffix(key, ".CHECKSUM") {
			continue
		}
		// want only .zip
		if !strings.HasSuffix(key, ".zip") {
			continue
		}
		// find YYYY-MM
		m := dateRe.FindStringSubmatch(key)
		if m == nil {
			continue
		}
		yr, err1 := strconv.Atoi(m[1])
		mm, err2 := strconv.Atoi(m[2])
		if err1 != nil || err2 != nil || mm < 1 || mm > 12 {
			continue
		}
		monthIndex := int64(yr)*12 + int64(mm-1)
		if monthIndex < minMonthIdx {
			minMonthIdx = monthIndex
			// construct public URL for the object
			oldest = m[1] + "-" + m[2]
		}
	}
	// if oldest still empty, nothing matched
	if oldest == "" {
		oldest = time.Now().Format("2006-01")
	}
	return
}

func spotOldestMonth(symbol string) (oldest string) {
	listingURL := fmt.Sprintf(types.SPOT_SYMBOL_MONTH_LISTING_URL, symbol)

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Get(listingURL)
	if err != nil {
		fmt.Println("Error in request", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		fmt.Println("status error", resp.StatusCode)
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading body", err)
		return
	}

	var res listBucketResult
	if err := xml.Unmarshal(body, &res); err != nil {
		fmt.Println("XML unmarshal error:", err)
		return
	}

	// regex to match YYYY-MM.zip at end of filename
	dateRe := regexp.MustCompile(`(\d{4})-(\d{2})\.zip$`)

	const INF = int64(1<<62 - 1)
	minMonthIdx := INF

	for _, c := range res.Contents {
		key := strings.TrimSpace(c.Key)
		if key == "" {
			continue
		}
		// skip CHECKSUM files
		if strings.HasSuffix(key, ".CHECKSUM") {
			continue
		}
		// want only .zip
		if !strings.HasSuffix(key, ".zip") {
			continue
		}
		// find YYYY-MM
		m := dateRe.FindStringSubmatch(key)
		if m == nil {
			continue
		}
		yr, err1 := strconv.Atoi(m[1])
		mm, err2 := strconv.Atoi(m[2])
		if err1 != nil || err2 != nil || mm < 1 || mm > 12 {
			continue
		}
		monthIndex := int64(yr)*12 + int64(mm-1)
		if monthIndex < minMonthIdx {
			minMonthIdx = monthIndex
			// construct public URL for the object
			oldest = m[1] + "-" + m[2]
		}
	}

	// if oldest still empty, nothing matched
	return
}

func fetchOldestMonths(market int32, symbols []string) map[types.Symbol]types.Oldest {
	type helper struct {
		s, o string
	}
	res := make(chan helper)
	out := make(map[types.Symbol]types.Oldest, len(symbols))
	done := make(chan struct{})
	go func() {
		for {
			select {
			case result := <-res:
				out[types.Symbol(result.s)] = types.Oldest(result.o)
			case <-done:
				close(res)
				return
			}
		}
	}()
	wg := sync.WaitGroup{}
	nobitexFlag := true
	currencies := make([]Datum, 0)
	for _, s := range symbols {
		wg.Add(1)
		switch market {
		case types.SPOT_MARKET:
			go func(symbol string) {
				oldest := spotOldestMonth(symbol)
				res <- helper{symbol, oldest}
				wg.Done()
			}(s)
		case types.FUTURES_MARKET:
			go func(symbol string) {
				// in case we got symbols from SQL they are postfixed with _FUTURES
				originalSymbol := strings.ReplaceAll(symbol, "_FUTURES", "")
				// in case we got symbols from Binance API the replacement has no effect on them
				oldest := futureOldestMonth(originalSymbol)
				res <- helper{originalSymbol + "_FUTURES", oldest}
				wg.Done()
			}(s)
		case types.NOBITEX_MARKET, types.BITYCLE_MARKET:
			for nobitexFlag {
				l := len(currencies)
				currencies = getMoreNobitexCurrencies(currencies)
				if len(currencies) == l {
					nobitexFlag = false
					break
				} else {
					l = len(currencies)
				}
			}
			oldest := ""
			index := slices.IndexFunc(currencies, func(d Datum) bool { return d.Symbol == strings.ToLower(strings.ReplaceAll(s, "IRT", "")) })
			if index != -1 && currencies[index].ListingDate.Year() != 1 {
				oldest = currencies[index].ListingDate.Format(time.RFC3339)
			}
			res <- helper{s, oldest}
			wg.Done()
		default:
			// FOREX market doesn't use this feature yet.
			res <- helper{s, ""}
			wg.Done()
		}
	}
	wg.Wait()
	done <- struct{}{}
	return out
}

func fetchAllSymbols(market int32, sqlUrl string, cachePath string) (map[types.Symbol]types.Oldest, error) {
	// First we have to read symbols from SQL
	var err error
	var db *sql.DB
	var results *sql.Rows
	var query string
	symbols := make([]string, 0)
	switch market {
	case types.FOREX_MARKET:
		query = `SELECT name FROM public.forex_symbols order by id`
	case types.SPOT_MARKET:
		query = `SELECT name FROM public.spot_symbols order by id`
	case types.FUTURES_MARKET:
		query = `SELECT name FROM public.future_symbols order by id`
	case types.NOBITEX_MARKET, types.BITYCLE_MARKET:
		query = `SELECT name FROM public.nobitex_symbols order by id`
	}

	if db, err = sql.Open("postgres", sqlUrl); err == nil {
		defer db.Close() // ensure connection closes when done
		if results, err = db.Query(query); err == nil {
			defer results.Close()
			for results.Next() {
				var name string
				if err := results.Scan(&name); err != nil {
					logrus.Warnln("reading row of sql symbols failed", err)
					break
				}
				symbols = append(symbols, name)
			}
			return fetchOldestMonths(market, symbols), nil
		}
	}

	// Second try is reading symbols from source
	symbols = make([]string, 0)
	var url string
	switch market {
	case types.NOBITEX_MARKET, types.BITYCLE_MARKET:
		url := types.NOBITEX_INFO_BASE_URL
		if req, err := http.NewRequest("GET", url, nil); err == nil {
			if res, err := http.DefaultClient.Do(req); err == nil {
				defer res.Body.Close()
				var r tempType1
				if err = json.NewDecoder(res.Body).Decode(&r); err == nil {
					for key, stat := range r.Stats {
						if !stat.IsClosed {
							name := strings.ToUpper(strings.ReplaceAll(key, "-", ""))
							symbols = append(symbols, name)
						}
					}
					return fetchOldestMonths(market, symbols), nil
				}
			}
		}
	case types.SPOT_MARKET:
		url = types.SPOT_INFO_BASE_URL
		fallthrough
	case types.FUTURES_MARKET:
		if url == "" {
			url = types.FUTURES_INFO_BASE_URL
		}
		if req, err := http.NewRequest("GET", url, nil); err == nil {
			if resp, err := http.DefaultClient.Do(req); err == nil {
				defer resp.Body.Close()
				if resp.StatusCode == 200 {
					var info exchangeInfoResp
					if err := json.NewDecoder(resp.Body).Decode(&info); err == nil {
						// Only include tradable symbols (status == "TRADING")
						for _, s := range info.Symbols {
							if strings.EqualFold(s.Status, "TRADING") {
								symbols = append(symbols, s.Symbol)
							}
						}
						return fetchOldestMonths(market, symbols), nil
					}
				}
			}
		}
	}
	// Third try is reading from local cache
	sc, err := loadCache(cachePath)
	if err != nil {
		return nil, err
	}

	return sc.Symbols, err
}

func saveCache(path string, c *types.SymbolsCache) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(c); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func loadCache(path string) (*types.SymbolsCache, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var c types.SymbolsCache
	if err := json.NewDecoder(f).Decode(&c); err != nil {
		return nil, err
	}
	return &c, nil
}

func SetupCache(market int32, sqlUrl string) *types.SymbolsCache {
	var cachePath string
	switch market {
	case types.FOREX_MARKET:
		cachePath = filepath.Join(".", types.FOREX_CACHE_FILE)
	case types.SPOT_MARKET:
		cachePath = filepath.Join(".", types.SPOT_CACHE_FILE)
	case types.FUTURES_MARKET:
		cachePath = filepath.Join(".", types.FUTURES_CACHE_FILE)
	case types.NOBITEX_MARKET, types.BITYCLE_MARKET:
		cachePath = filepath.Join(".", types.NOBITEX_CACHE_FILE)
	}
	logrus.Infoln("Fetching symbol list...")
	syms, err := fetchAllSymbols(market, sqlUrl, cachePath)
	if err != nil {
		logrus.Fatalf("failed to fetch symbols: %v", err)
	}
	sc := &types.SymbolsCache{Symbols: syms, Loaded: time.Now().UTC()}
	if err := saveCache(cachePath, sc); err != nil {
		logrus.Warnf("warning: failed to write cache: %v", err)
	} else {
		logrus.Infof("Saved %d symbols to %s", len(sc.Symbols), cachePath)
	}
	logrus.Infof("Total symbols loaded: %d", len(sc.Symbols))
	return sc
}

func warmupAlertFn(url string) (alerts types.AlertsGet) {
	fmt.Println("Reading warmup alerts")
	client := &http.Client{
		Timeout: 5 * time.Second,
	}
	// defer client.CloseIdleConnections()
	if req, err := http.NewRequest("GET", url, nil); err == nil {
		if res, err := client.Do(req); err == nil {
			defer res.Body.Close()
			if res.StatusCode != http.StatusOK {
				r, _ := io.ReadAll(res.Body)
				fmt.Println("WARN WARNING : failed to read warmup alerts", string(r))
				return
			} else if res.Body == nil {
				logrus.Warnln("Nobitex FetchLastKlines nil body!!", res)
			}
			if err = json.NewDecoder(res.Body).Decode(&alerts); err != nil {
				fmt.Println("WARN WARNING : failed to read warmup alerts", err)
			}
		} else {
			fmt.Println("WARN WARNING : failed to read warmup alerts", err)
		}
	} else {
		fmt.Println("WARN WARNING : failed to read warmup alerts", err)
	}
	return
}

/////////////////////////////////////////////////////////
//										Setup Utils
/////////////////////////////////////////////////////////

// Nobitex

type tempType1 struct {
	Status string               `json:"status"`
	Stats  map[string]tempType2 `json:"stats"`
}
type tempType2 struct {
	IsClosed bool `json:"isClosed"`
	// BestSell  string `json:"bestSell"`
	// BestBuy   string `json:"bestBuy"`
	// VolumeSrc string `json:"volumeSrc"`
	// VolumeDst string `json:"volumeDst"`
	// Latest    string `json:"latest"`
	// Mark      string `json:"mark"`
	// DayLow    string `json:"dayLow"`
	// DayHigh   string `json:"dayHigh"`
	// DayOpen   string `json:"dayOpen"`
	// DayClose  string `json:"dayClose"`
	// DayChange string `json:"dayChange"`
}

func UnmarshalNobitexCurrencies(data []byte) (NobitexCurrencies, error) {
	var r NobitexCurrencies
	err := json.Unmarshal(data, &r)
	return r, err
}

type NobitexCurrencies struct {
	Status string  `json:"status"`
	Data   []Datum `json:"data"`
	Meta   Meta    `json:"meta"`
}

type Datum struct {
	ID          int64     `json:"id"`
	DocumentID  string    `json:"documentId"`
	Symbol      string    `json:"symbol"`
	ListingDate time.Time `json:"listingDate"`
}

type Meta struct {
	Pagination Pagination `json:"pagination"`
}

type Pagination struct {
	Start int64 `json:"start"`
	Limit int64 `json:"limit"`
	Total int64 `json:"total"`
}

func getMoreNobitexCurrencies(currentCurrencies []Datum) []Datum {
	start := int64(len(currentCurrencies))
	url := "https://content.nobitex.ir/api/currencies?fields%5B1%5D=symbol&fields%5B4%5D=listingDate&pagination%5Blimit%5D=250&pagination%5Bstart%5D=" + strconv.FormatInt(start, 10)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fmt.Println(err)
		return currentCurrencies
	}
	req.Header.Add("Referer", "https://nobitex.ir/")
	req.Header.Add("User-Agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36")
	req.Header.Add("Accept", "application/json, text/plain, */*")
	req.Header.Add("sec-ch-ua", `"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"`)
	req.Header.Add("sec-ch-ua-mobile", "?0")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Println(err)
		return currentCurrencies
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		fmt.Println(err)
		return currentCurrencies
	}
	newCurrencies, err := UnmarshalNobitexCurrencies(body)
	if err != nil {
		fmt.Println(err)
		return currentCurrencies
	}
	for _, newCurrency := range newCurrencies.Data {
		currentCurrencies = append(currentCurrencies, newCurrency)
	}
	return currentCurrencies
}
