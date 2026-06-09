package config

type listBucketResult struct {
	Contents []struct {
		Key string `xml:"Key"`
	} `xml:"Contents"`
}

type exchangeInfoResp struct {
	Symbols []struct {
		Symbol string `json:"symbol"`
		Status string `json:"status"`
		// other fields omitted
	} `json:"symbols"`
}
