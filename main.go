package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buger/jsonparser"
	"github.com/gorilla/websocket"
)

var (
	mu         sync.Mutex
	marketsMap = make(map[string]*market)
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  10240,
	WriteBufferSize: 10240,
}

var topCoins []string

var settings struct {
	CmcAPIKey           string `json:"CmcAPIKey"`
	Tf                  string `json:"Tf"`
	BroadcastNoduleAddr string `json:"BroadcastNoduleAddr"`
	RestServerEndpoint  string `json:"RestServerEndpoint"`
	RestServerPort      string `json:"RestServerPort"`
	UpdateTimeout       int    `json:"UpdateTimeout"`
	TopAmount           int    `json:"TopAmount"`
	QuoteAsset          string `json:"QuoteAsset"`
	QuoteAssetOr        string `json:"QuoteAssetOr"`
	MarketTypePriority1 string `json:"MarketTypePriority1"`
	MarketTypePriority2 string `json:"MarketTypePriority2"`
}

var subscribeList []string

type market struct {
	subsString, short_name, exchange, description string
	ch, chp, lp, o, h, l, v                       float64
	top                                           int
}

var message = make(chan string)
var broadcastReadError = make(chan bool)

func checkInSubscribeList(subsString string) bool {
	for _, coin := range subscribeList {
		if coin == subsString {
			return true
		}
	}
	return false
}

// convert from ticker to subscribe string
func getSubsName(ticker string) string {
	tickerArr := strings.Split(ticker, ":")
	exchangeArr := strings.Split(tickerArr[0], "_")
	exchange := strings.ToLower(exchangeArr[0])
	marketType := strings.ToLower(exchangeArr[1])
	symbol := tickerArr[1]

	if exchange == "ftx" {
		marketType = "all"
	}
	if exchange == "binance" {
		marketType = "linear"
	}

	subscribeStr := fmt.Sprintf("%s_%s,symbol=%s,tf=%s", exchange, marketType, symbol, settings.Tf)
	return subscribeStr
}

// watchlist init data rest server module
func urlParser(w http.ResponseWriter, r *http.Request) {

	// incoming requests examples:
	var byTicker bool
	var tickers []string

	// url parse
	tickersRaw := r.FormValue("tickers")
	if tickersRaw != "all" {
		tickers = strings.Split(tickersRaw, ",")
		if len(tickers) != 0 {
			byTicker = true
		}
	}

	var jsonOutput strings.Builder
	jsonOutput.WriteString("{")
	comma2 := ""

	// for each ticker iterate
	if byTicker {
		for i := 0; i < len(tickers); i++ {

			mu.Lock()
			_, ok := marketsMap[tickers[i]]
			if ok {
				jsonHeader := fmt.Sprintf("%s\"%d\":{\"s\":\"ok\",\"n\":\""+tickers[i]+"\",\"v\":{", comma2, i)
				jsonOutput.WriteString(jsonHeader)

				dataString := fmt.Sprintf("\"ch\":%f, \"chp\":%f, \"short_name\":\"%s\", \"exchange\":\"%s\", \"description\":\"%s\", \"lp\":%f, \"open_price\":%f, \"high_price\":%f, \"low_price\":%f, \"volume\":%f", marketsMap[tickers[i]].ch, marketsMap[tickers[i]].chp, marketsMap[tickers[i]].short_name, marketsMap[tickers[i]].exchange, marketsMap[tickers[i]].description, marketsMap[tickers[i]].lp, marketsMap[tickers[i]].o, marketsMap[tickers[i]].h, marketsMap[tickers[i]].l, marketsMap[tickers[i]].v)
				jsonOutput.WriteString(dataString)
			} else {
				jsonHeader := fmt.Sprintf("%s\"%d\":{\"s\":\"error\",\"n\":\""+tickers[i]+"\",\"v\":{", comma2, i)
				jsonOutput.WriteString(jsonHeader)
			}

			jsonOutput.WriteString("}}")
			if comma2 == "" {
				comma2 = ","
			}
			mu.Unlock()

		}
	} else {
		for k, v := range marketsMap {

			mu.Lock()
			jsonHeader := fmt.Sprintf("%s\"%d\":{\"s\":\"ok\",\"n\":\""+k+"\",\"v\":{", comma2, v.top)
			jsonOutput.WriteString(jsonHeader)

			dataString := fmt.Sprintf("\"ch\":%f, \"chp\":%f, \"short_name\":\"%s\", \"exchange\":\"%s\", \"description\":\"%s\", \"lp\":%f, \"open_price\":%f, \"high_price\":%f, \"low_price\":%f, \"volume\":%f", v.ch, v.chp, v.short_name, v.exchange, v.description, v.lp, v.o, v.h, v.l, v.v)
			jsonOutput.WriteString(dataString)

			jsonOutput.WriteString("}}")
			if comma2 == "" {
				comma2 = ","
			}
			mu.Unlock()

		}
	}

	// send response to client
	jsonOutput.WriteString("}")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Write([]byte(jsonOutput.String()))

}

func getTopTickers() {

	var topCoins2 []string

	newMarketsMap := make(map[string]int)

	fairTopNum := 0

	// get data from cmc
	fmt.Println("Get top assets from CMC")
	client := &http.Client{}
	req, err := http.NewRequest("GET", "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest", nil)
	if err != nil {
		log.Print(err)
		fmt.Println("Get top assets from CMC - FAILED")
		os.Exit(1)
	}

	q := url.Values{}
	q.Add("start", "1")
	topAmountStr := fmt.Sprint(settings.TopAmount + 10)
	q.Add("limit", topAmountStr)
	q.Add("convert", "USD")

	req.Header.Set("Accepts", "application/json")
	req.Header.Add("X-CMC_PRO_API_KEY", settings.CmcAPIKey)
	req.URL.RawQuery = q.Encode()

	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error sending request to server")
		os.Exit(1)
	}
	fmt.Println("CMC query status: ", resp.Status)
	respBody, _ := ioutil.ReadAll(resp.Body)

	coinsIterator := 0
	coinCheck := true

	for coinCheck {

		bypassThisCoin := false
		stringedIter := fmt.Sprintf("[%d]", coinsIterator)
		var symbol, _ = jsonparser.GetString(respBody, "data", stringedIter, "symbol")

		// by tag filter
		tagIterator := 0
		tagCheck := true

		for tagCheck {
			stringedTagIter := fmt.Sprintf("[%d]", tagIterator)
			var tag, _ = jsonparser.GetString(respBody, "data", stringedIter, "tags", stringedTagIter)
			if tag != "" {

				if tag == "stablecoin" || tag == "wrapped-tokens" {
					bypassThisCoin = true
				}
				tagIterator++

			} else {
				tagCheck = false
			}

		}

		if symbol != "" {
			if !bypassThisCoin {
				topCoins2 = append(topCoins2, symbol)
			}
			coinsIterator++
		} else {
			coinCheck = false
		}
		if len(topCoins2)-1 == settings.TopAmount {
			coinCheck = false
		}
	}

	fmt.Println("Get top assets from CMC - SUCCESS")
	fmt.Println(topCoins2)

	if len(topCoins2) != 0 {
		topCoins = topCoins2
	}

	fmt.Println("Get top tickers from targethit server ... ")
	// get data from our server
	resp2, err := http.Get("https://targethit.com/api/markets")

	if err != nil {
		log.Fatal(err)
		fmt.Println("Get top tickers from targethit server - FAILED")
	}

	defer resp2.Body.Close()

	body, err := ioutil.ReadAll(resp2.Body)

	if err != nil {
		log.Fatal(err)
	}

	// find market name loop
	for y, thisCoinName := range topCoins {

		thisTicker := ""
		thisExchange := ""
		thisDescription := ""
		thisShortName := ""

		marketTypePriority := settings.MarketTypePriority1

		// define Iterator
		iter := 0
		symbolCheck := true

		for symbolCheck {
			stringedIter2 := fmt.Sprintf("[%d]", iter)
			var getSymbolName, _ = jsonparser.GetString(body, stringedIter2, "base_asset")
			var getQuoteAsset, _ = jsonparser.GetString(body, stringedIter2, "quote_asset")
			var getAssetType, _ = jsonparser.GetString(body, stringedIter2, "type")

			if getSymbolName != "" {

				if getSymbolName == thisCoinName {

					if getQuoteAsset == settings.QuoteAsset || getQuoteAsset == settings.QuoteAssetOr {

						if getAssetType == marketTypePriority {
							var getTicker, _ = jsonparser.GetString(body, stringedIter2, "ticker")
							var getExchange, _ = jsonparser.GetString(body, stringedIter2, "exchange")
							var getDescription, _ = jsonparser.GetString(body, stringedIter2, "description")

							thisTicker = getTicker
							thisExchange = getExchange
							thisDescription = getDescription
							thisShortName = getSymbolName

						}
					}
				}
				iter++
			} else {
				// stop iterate
				symbolCheck = false
				if thisTicker == "" {
					if marketTypePriority == settings.MarketTypePriority1 {
						marketTypePriority = settings.MarketTypePriority2
						symbolCheck = true
						iter = 0
					}
					continue
				}

				// define new map with tickers
				newMarketsMap[thisTicker] = y

				// rewrite init data to map
				mu.Lock()

				_, ok := marketsMap[thisTicker]
				// if ticker finded - update top position only
				if ok {
					marketsMap[thisTicker].top = fairTopNum
				} else {
					marketsMap[thisTicker] = new(market)

					marketsMap[thisTicker].exchange = thisExchange
					marketsMap[thisTicker].description = thisDescription
					marketsMap[thisTicker].subsString = getSubsName(thisTicker)
					marketsMap[thisTicker].short_name = thisShortName
					marketsMap[thisTicker].top = fairTopNum

					fmt.Println("added to watchlist:", thisTicker)
				}

				mu.Unlock()

				fairTopNum++

				// debug
				// fmt.Println(newMarketsMap)
			}
		}

	}

	// delete from map old ticker
	mu.Lock()
	for k, _ := range marketsMap {
		_, ok := newMarketsMap[k]
		if !ok {
			fmt.Println("removed from watchlist:", k)
			delete(marketsMap, k)
		}
	}
	mu.Unlock()

	fmt.Println("Get top tickers from targethit server - SUCCESS")

	// debug
	// fmt.Println(marketsMap)

	// first subscribe to all tickers or subscribe to recently added
	go subscribeModule()

}

func subscribeModule() {
	mu.Lock()
	// subscribe to all markets
	for k, v := range marketsMap {

		if !checkInSubscribeList(k) {
			subsStringForsend := fmt.Sprintf("s?%s", v.subsString)
			message <- subsStringForsend
			subscribeList = append(subscribeList, k)
		}
	}
	mu.Unlock()
	//fmt.Println(subscribeList)
}

func connectToBroadcastModule() {
	// CONNECT TO BROADCAST MODULE
	for {
		writeErr := false
		fmt.Println("Trying to connect - BROADCAST MODULE")
		connToBroadcast, _, err := websocket.DefaultDialer.Dial(settings.BroadcastNoduleAddr, nil)
		if err != nil {
			fmt.Println("Connection to BROADCAST MODULE fails, is being re-connection")
			time.Sleep(1 * time.Second)
			continue
		} else {
			fmt.Println("Connection to BROADCAST MODULE success")
			go subscribeModule()
		}
		go func() {
			for {
				// set read dead time
				connToBroadcast.SetReadDeadline(time.Now().Add(time.Duration(10) * time.Minute))

				// watch for new messages
				_, msg, readErr := connToBroadcast.ReadMessage()
				if readErr != nil {
					fmt.Println("Error during message reading:", readErr)
					broadcastReadError <- true
					return
				}

				// parse incoming
				dataArr := strings.Split(string(msg), " c=")
				dataPath := dataArr[0]
				dataRaw := dataArr[1]

				tickDataArr := strings.Split(dataRaw, `"`)
				ohlcvArr := strings.Split(tickDataArr[1], ` `)

				//tickTime, _ := strconv.ParseInt(tickDataArr[2], 10, 64)
				tickOpen, _ := strconv.ParseFloat(ohlcvArr[0], 64)
				tickHigh, _ := strconv.ParseFloat(ohlcvArr[1], 64)
				tickLow, _ := strconv.ParseFloat(ohlcvArr[2], 64)
				tickClose, _ := strconv.ParseFloat(ohlcvArr[3], 64)
				tickVol, _ := strconv.ParseFloat(ohlcvArr[4], 64)
				priceChanged := tickClose - tickOpen

				// iterate all
				mu.Lock()
				for k, v := range marketsMap {
					if v.subsString == dataPath {

						marketsMap[k].ch = priceChanged
						marketsMap[k].chp = priceChanged / tickClose * 100
						marketsMap[k].lp = tickClose
						marketsMap[k].o = tickOpen
						marketsMap[k].h = tickHigh
						marketsMap[k].l = tickLow
						marketsMap[k].v = tickVol
					}
				}
				mu.Unlock()
			}
		}()
		for {

			select {
			case messageData := <-message:
				err = connToBroadcast.WriteMessage(1, []byte(messageData))
				if err != nil {
					fmt.Println("Error during message writing:", err)
					writeErr = true
					time.Sleep(1 * time.Second)
				}
			case readError := <-broadcastReadError:
				fmt.Println("Error during message reading - CONNECTION RESTART")
				if readError {
					fmt.Println("readError = true - restart")
					writeErr = true
				}
				time.Sleep(1 * time.Second)
			}

			if writeErr {
				subscribeList = subscribeList[:0]
				break
			}
		}
	}
}

func main() {

	// load settings from file
	configFile, err := os.Open("config.json")
	if err != nil {
		fmt.Println("config file read error!")
	}

	jsonParser := json.NewDecoder(configFile)
	if err = jsonParser.Decode(&settings); err != nil {
		fmt.Println("config file parse error!")
	}

	go connectToBroadcastModule()

	// get top coins list from cmc init
	go getTopTickers()

	// update top tickers periodically
	go func() {
		updater := time.NewTicker(time.Duration(settings.UpdateTimeout) * time.Minute)
		defer updater.Stop()
		for range updater.C {
			go getTopTickers()
		}
	}()

	// start rest listener service
	http.HandleFunc(settings.RestServerEndpoint, urlParser)
	log.Fatal(http.ListenAndServe(settings.RestServerPort, nil))

}
