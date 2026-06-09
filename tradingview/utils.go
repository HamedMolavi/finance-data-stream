package tradingview

import (
	"errors"
	"regexp"
	"strings"

	"github.com/HamedMolavi/finance-data-stream/types"
)

func split(symbol types.Symbol, timeFrame types.Timeframe) (tvSession, tvExchange, tvSymbol string, err error) {
	tvSession = "regular"
	strSymbol := symbol.String()
	if strings.Contains(strSymbol, "_1") {
		tvSession = "us_regular"
		strSymbol = strings.ReplaceAll(strSymbol, "_1", "")
	}
	splitted := strings.Split(strSymbol, "_")
	if len(splitted) < 2 {
		err = errors.New("Wrong symbol name construction")
		return
	}
	tvExchange = splitted[0]
	if len(splitted) == 2 {
		tvSymbol = splitted[1]
	} else {
		if splitted[1] == "MINI" {
			tvExchange = strings.Join(splitted[:2], "_")
			tvSymbol = splitted[len(splitted)-1]
		} else {
			tvSymbol = strings.Join(splitted[1:], "_")
		}
	}
	if strings.HasPrefix(tvSymbol, "FF") {
		tvSymbol = tvSymbol[2:]
		if timeFrame == "D" {
			timeFrame = "1D"
		}
	}
	return
}

var regex = regexp.MustCompile(`"p"\s*:\s*\[\s*"(\w+)"`)

func findSessionID(in string) (ChartSessionID, bool) {
	result := regex.FindStringSubmatch(in)
	if len(result) > 1 {
		return ChartSessionID(result[1]), true
	}
	return "", false
}
