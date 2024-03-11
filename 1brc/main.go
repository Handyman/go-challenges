package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type StationData = map[string][]int32
type StationResults = map[string]string

var stationData StationData
var stationResults StationResults
var wg sync.WaitGroup
var mutex sync.Mutex

func main() {
	// Start the timer
	start := time.Now()

	stationData = make(StationData)
	stationResults = make(StationResults)

	processFileStart := time.Now()
	processFile()
	profileFileElapsed := time.Since(processFileStart)

	processStationsStart := time.Now()
	processStations()
	processStationsElapsed := time.Since(processStationsStart)

	processSortStart := time.Now()
	sortAndOutput()
	processSortElapsed := time.Since(processSortStart)

	// Calculate the elapsed time
	elapsed := time.Since(start)
	// Output the script time
	fmt.Printf("File processing time: %v\n", profileFileElapsed)
	fmt.Printf("Station processing time: %v\n", processStationsElapsed)
	fmt.Printf("Sort processing time: %v\n", processSortElapsed)
	fmt.Printf("Script execution time: %v\n", elapsed)
}

func processFile() {
	file, err := os.Open("github/measurements.txt")
	if err != nil {
		panic(err)
	}

	defer file.Close()

	reader := bufio.NewReader(file)

	for {
		line, _, err := reader.ReadLine()

		if err == io.EOF {
			break
		}
		wg.Add(1)
		go processLine(string(line))
	}
	wg.Wait()
}

func processLine(line string) {
	fields := strings.Split(line, ";")

	floatPart, _ := strconv.ParseFloat(fields[1], 32)
	temperature := int32(floatPart * 10)

	wg.Add(1)
	go appendTemperature(fields[0], temperature)

	wg.Done()
}

func appendTemperature(station string, temperature int32) {

	// Lock the mutex before writing to the map
	mutex.Lock()

	stationData[station] = append(stationData[station], temperature)

	mutex.Unlock()
	wg.Done()
}

func processStations() {
	for station, temperatures := range stationData {
		wg.Add(1)
		go procesStation(station, temperatures)
	}
	wg.Wait()
}

func procesStation(station string, temperatures []int32) {

	var minTemp int32
	var maxTemp int32
	var mean int32
	var temperatureSum int32
	first := true
	dataPoints := int32(len(temperatures))
	temperatureSum = 0

	for _, temp := range temperatures {
		if first == true {
			maxTemp = temp
			minTemp = temp
			first = false
		} else if temp > maxTemp {
			maxTemp = temp
		} else if temp < minTemp {
			minTemp = temp
		}

		temperatureSum += temp
	}
	mean = temperatureSum / dataPoints

	minResult := strconv.FormatFloat(float64(minTemp)/10.0, 'f', 1, 32)
	meanResult := strconv.FormatFloat(float64(mean)/10.0, 'f', 1, 32)
	maxResult := strconv.FormatFloat(float64(maxTemp)/10.0, 'f', 1, 32)

	wg.Add(1)
	// Write in this format: <min>/<mean>/<max>
	go writeStation(station, minResult+"/"+meanResult+"/"+maxResult)

	wg.Done()
}

func writeStation(station string, results string) {
	mutex.Lock()
	stationResults[station] = results
	mutex.Unlock()
	wg.Done()
}

func sortAndOutput() {
	keys := make([]string, 0, len(stationResults))
	for k := range stationResults {
		keys = append(keys, k)
	}

	// Sort the keys
	sort.Strings(keys)

	// Iterate over the sorted keys and print the values
	// example:
	//{Abha=-23.0/18.0/59.2, Abidjan=-16.2/26.0/67.3, Abéché=-10.0/29.4/69.0, Accra=-10.1/26.4/66.4, Addis Ababa=-23.7/16.0/67.0, Adelaide=-27.8/17.3/58.5, ...}
	for _, k := range keys {
		fmt.Printf("%s=%v\n", k, stationResults[k])
	}
}
