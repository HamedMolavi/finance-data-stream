package types

import (
	"fmt"
	"time"
)

////////////////////////////////////////////////////////////////////////////////////

type AlertTriggerType string

const (
	CrossAlert        AlertTriggerType = "CROSS"
	CrossUpAlert      AlertTriggerType = "CROSS_UP"
	CrossDownAlert    AlertTriggerType = "CROSS_DOWN"
	GreaterAlert      AlertTriggerType = "GREATER"
	LessAlert         AlertTriggerType = "LESS"
	ChannelEnterAlert AlertTriggerType = "CHANNEL_ENTER"
	ChannelLeaveAlert AlertTriggerType = "CHANNEL_LEAVE"
	ChannelInAlert    AlertTriggerType = "CHANNEL_IN"
	ChannelOutAlert   AlertTriggerType = "CHANNEL_OUT"
)

type AlertMode int

const (
	Cross AlertMode = iota
	In
	Out
	EnterMode // alert when going inside the channel
	LeaveMode // alert when leaving the channel
)

////////////////////////////////////////////////////////////////////////////////////

type AlertServiceType string // lowercase
type AlertServiceTypeHash struct {
	Service    AlertServiceType
	Resolution Interval
	InputsHash string
}

const (
	PriceAlert    AlertServiceType = "price"
	EmaCrossAlert AlertServiceType = "ema_cross"
	ThreeSmaAlert AlertServiceType = "three_sma"

	AwesomeOsc     AlertServiceType = "awesome_oscillator"
	AcceleratorOsc AlertServiceType = "accelerator_oscillator"

	ThreeSmaMovingAverageConvergenceDivergence
	MovingAverageConvergenceDivergence
	BalanceOfPower
	BollingerBandsPercentB
	BollingerBandsWidth
	ChaikinMoneyFlow
	ChaikinOsc
	ChaikinVolatility
	ChandeKrollStop
	ChandeMomentumOsc
	ChopZone
	ChoppinessIdx
	ConnorsRsi
	CoppockCurve
	CorrelationCoeff
	CorrelationLog
	DetrendedPriceOsc
	DonchianChannels
	DoubleExponentialMovingAverage
	EaseOfMovement
	EldersForceIdx
	EmaCross
	Envelope
	StandardError
	StandardErrorBands
	FisherTransform
	HistoricalVolatility
	HullMa
	KeltnerChannels
	KlingerOsc
	KnowSureThing
	LeastSquaresMovingAverage
	LinearRegressionCurve
	LinearRegressionSlope
	MaCross
	MaWithEmaCross
	MassIdx
	McginleyDynamic
	MedianPrice
	Momentum
	MoneyFlow
	MovingAverage
	MovingAverageChannel
	MovingAverageExponential
	MovingAverageWeighted
	MovingAverageDouble
	MovingAverageTriple
	MovingAverageAdaptive
	MovingAverageHamming
	MovingAverageMultiple
	MajorityRule
	NetVolume
	ParabolicSar
	PriceChannel
	PriceOsc
	RateOfChange
	RelativeVigorIdx
	RelativeVolatilityIdx
	SmiErgodicIndicatorOsc
	SmoothedMovingAverage
	StandardDeviation
	Stochastic
	StochasticRsi
	Trix
	TripleEma
	TrueStrengthIndicator
	TrendStrengthIdx
	Typicalprice
	UltimateOsc
	VolatilityCloseToClose
	VolatilityZeroTrendCloseToClose
	VolatilityOHLC
	VolatilityIdx
	Vwma
	VolumeOsc
	VortexIndicator
	WillamsPercentR
	WilliamsAlligator
	WilliamsFractals
	GuppyMultipleMovingAverage
	Zigzag
	Volume
	Modifiedmovingaverage
	Ac
	Squeezemomentumindicator
	Tdi
	Adxanddi
	Fibonaccibollingerbands
	Sslhybrid
	Supportandresistancelevelswithbreaks
	Matrixseries
	TuxEmaScalper
	Halftrend
	Rsiupdated
	Chandelierexit
	Adxdihistogram
	Orderblock
	Pivotbasedtrailingmaximaminima
	Boomhunterpro
	Compositeindexdivergenceindicator
	Elliottwaveoscillator
	QqeMod
	Megarsi
	Sslchannel
	Ichimokuronin
	Stc
	Heikinashirsioscillator
	Trendlineswithbreaks
	Nogapscandles
	Divergenceformanyindicators
	Stiffness
	Trendregularityadaptivemovingaverage
	Metatradermacd
	Macdupdated
	Dtoscillator
	Lsmaupdated
	Chartsessions
	Vslrt
	Superichiluxalgo
	Ichimokucrossandswitch
	Aminoftmodified
	Linearregressioncandle
	Coraltrendlazybear
	Vwapold
	Nadarayawatsonenvelope
	Blackflagfts
	Hullsuite
	RelativeStrengthIdx
	BollingerBands
	AverageTrueRange
	DirectionalMovementIdx
	IchimokuCloud
	CommodityChannelIdx
	Supertrend
	AdvanceDecline
	ArnaudLegouxMovingAverage
	Aroon
	Averageprice
	AverageDirectionalIdx
	Alphatrend
	Wavetrendoscillator
	Trendtraderstrategy
	Bullsvbears
	IchimokucloudQualityline
	Macci
	Ichimoku2c
	Macdichimoku
	Laguerrersi
	Zlsma
	Basingcandles
	AtrStopLossFinder
	AiEngulfingcandle
	DailyHighLow
	Highandlowlevels
	GdMomentum
	Marketfacilitationindex
	Superorderblock
	Smartmoneyconcepts
	Marketsessions
	Orderblockdetector
	Andeanoscillator
	Impulsemacd
	Zerolagmacdenhancedversion
	Littiming
	Rangefilterandlablesdw
	Lnltrendsystem
)

func (s AlertServiceType) Hash(data IndicatorData) AlertServiceTypeHash {
	switch s {
	case PriceAlert:
		return AlertServiceTypeHash{PriceAlert, M1, ""}
	case EmaCrossAlert:
		if data.IndicatorOutputIndex < 2 {
			return AlertServiceTypeHash{EmaCrossAlert, data.Resolution, fmt.Sprintf("%d", data.IndicatorInputs[data.IndicatorOutputIndex])}
		}
		return AlertServiceTypeHash{EmaCrossAlert, data.Resolution, fmt.Sprintf("%d,%d", data.IndicatorInputs[0], data.IndicatorInputs[1])}
	case ThreeSmaAlert:
		return AlertServiceTypeHash{ThreeSmaAlert, data.Resolution, fmt.Sprintf("%d", data.IndicatorInputs[data.IndicatorOutputIndex])}
	}
	return AlertServiceTypeHash{}
}
func (h AlertServiceTypeHash) UnHash() AlertServiceType {
	return h.Service
}

type IndicatorData struct {
	Resolution           Interval `json:"resolution"`
	IndicatorInputs      []int    `json:"indicatorInputs"`    // Inputs are given in array with preserving order
	IndicatorOutputIndex int      `json:"indicatorOutputIdx"` // indicator output may be array like, this selects the output to be sent to comparison channel
}

////////////////////////////////////////////////////////////////////////////////////

type HistoryState int
type TargetUniqueParam int64

const (
	NOT_CLEAR HistoryState = iota
	UNDER_LOW
	BETWEEN // also includes being ON_LOW or ON_UP
	ABOVE_UP
)

type Target struct {
	Id            TargetUniqueParam
	Upper         float64
	Lower         float64
	Usage         int
	HistoryState  HistoryState
	Mode          AlertMode
	Service       AlertServiceType
	IndicatorData IndicatorData
}

func (this *Target) String() string {
	return fmt.Sprintf("( Moed:%s Upper:%.0f Lower:%.0f History:%s)",
		[]string{"Cross", "In", "Out", "Enter", "Leave"}[this.Mode],
		this.Upper, this.Lower,
		[]string{"noclear", "under", "between", "above"}[this.HistoryState])
}

type TargetsMap map[TargetUniqueParam]*Target // { url: Target }

type SymbolAlerts struct {
	Last    float64
	Targets TargetsMap
}

func (this *SymbolAlerts) String() string {
	return fmt.Sprintf("%.0f - %v", this.Last, this.Targets)
}

////////////////////////////////////////////////////////////////////////////////////

type FanoutChannelSet struct {
	KlineCh        chan interface{}
	AddTargetCh    chan *Target
	UpdateTargetCh chan *Target
	DelTargetCh    chan TargetUniqueParam
	CopyCh         chan chan TargetsMap
	DoneCh         chan struct{}
}

type AlertsChannelSet struct {
	In             chan<- *Kline
	Out            chan float64
	AddTargetCh    chan *Target
	UpdateTargetCh chan *Target
	DelTargetCh    chan TargetUniqueParam
	CopyCh         chan chan TargetsMap
}

type AlertsGet struct {
	Data []AlertGetBody
}

type AlertGetBody struct {
	ID             int64       `json:"id"`              // 10,
	UserID         int64       `json:"user_id"`         // 1,
	Symbol         string      `json:"symbol"`          // "CBOT_ZL1!",
	Market         string      `json:"market"`          // "cme futures",
	Type           string      `json:"type"`            // "1",
	Params         string      `json:"params"`          // "{\"threshold\":49.45}" OR "{\"upper\":\"1\",\"lower\":\"0\"}"
	Status         int64       `json:"status"`          // 0,
	Triggered      int64       `json:"Triggered"`       // 0,
	Setting        string      `json:"setting"`         // "{\"repeat\":\"0\",\"title\":\"\",\"description\":\"\",\"trigger\":\"Only Once\",\"expireDate\":null}",
	TelegramStatus int64       `json:"telegram_status"` // 0,
	SMSStatus      int64       `json:"sms_status"`      // 0,
	WebStatus      int64       `json:"web_status"`      // 0,
	ShouldTelegram int64       `json:"should_telegram"` // 0,
	ShouldSMS      int64       `json:"should_sms"`      // 0,
	ShouldWeb      int64       `json:"should_web"`      // 1,
	SentTime       interface{} `json:"sent_time"`       // null,
	CreatedAt      time.Time   `json:"created_at"`      // "2025-12-27T09:06:57.000000Z",
	UpdatedAt      time.Time   `json:"updated_at"`      // "2025-12-27T09:13:30.000000Z",
	PhoneNumber    string      `json:"phone_number"`    // "09351171196",
	TelegramID     interface{} `json:"telegram_id"`     // null,
	PriceScale     *string     `json:"price_scale"`     // null
}
