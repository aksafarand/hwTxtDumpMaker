package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	logStd "log"
	"math"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"

	"github.com/aksafarand/ftpdownloader/configs"

	_ "github.com/alexbrainman/odbc"
	"github.com/gen2brain/go-unarr"
	"gopkg.in/dutchcoders/goftp.v1"
)

var downloadFailed int
var allTables map[string]*configs.Table

func copyNationalResultToFolder(src, dest, techName string) ([]string, error) {
	var resultCopy []string
	_, err := os.Stat(dest)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(dest, 0666); err != nil {
			return nil, err
		}
	}
	files, err := IOReadDir(src)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		fmt.Println(file)
		// copy and rename National Dump
		if strings.Contains(filepath.Base(file), "National") && strings.Contains(file, techName) {
			srcFile, err := os.Open(filepath.Join(src, file))
			if err != nil {
				return nil, err
			}
			defer srcFile.Close()

			newFileName := strings.Replace(filepath.Base(file), techName+"_DUMP_HW", "HW_"+techName, -1)
			newFile, err := os.Create(filepath.Join(dest, newFileName))
			if err != nil {
				return nil, err
			}
			defer newFile.Close()
			bytesCopied, err := io.Copy(newFile, srcFile)
			if err != nil {
				return nil, err
			}
			resultCopy = append(resultCopy, fmt.Sprintf("Copied %d bytes to %s", bytesCopied, newFile.Name()))
		}

	}

	return resultCopy, nil
}

func ftpDownload(remoteServer, remoteFolder, remoteUser, remotePass, currentDate, serverName, filePrefix string, wg *sync.WaitGroup, region, national, dateNaming string) {
	defer wg.Done()
	var err error
	var ftp *goftp.FTP

	if ftp, err = goftp.Connect(remoteServer); err != nil {
		log.Errorf("Cannot Connect To: %s ServerName: %s Err: %s", remoteServer, serverName, err.Error())
		downloadFailed++
		return

	}

	defer ftp.Close()

	if err = ftp.Login(remoteUser, remotePass); err != nil {
		config := tls.Config{
			InsecureSkipVerify: true,
			ClientAuth:         tls.RequestClientCert,
		}

		if err = ftp.AuthTLS(&config); err != nil {
			log.Errorf("Cannot Login To: %s ServerName: %s Err: %s", remoteServer, serverName, err.Error())
			downloadFailed++
			return
		}
		if err = ftp.Login(remoteUser, remotePass); err != nil {
			log.Errorf("Cannot Login To: %s ServerName: %s Err: %s", remoteServer, serverName, err.Error())
			downloadFailed++
			return
		}
	}

	if err = ftp.Cwd(remoteFolder); err != nil {
		log.Errorf("Cannot Open Folder From: %s From: %s ServerName: %s Err: %s", remoteFolder, remoteServer, serverName, err.Error())
		downloadFailed++
		return
	}

	var curpath string
	if curpath, err = ftp.Pwd(); err != nil {
		log.Errorf("Cannot Open Folder From: %s From: %s ServerName: %s Err: %s", remoteFolder, remoteServer, serverName, err.Error())

		return
	}

	var files []string
	if files, err = ftp.List(remoteFolder); err != nil {
		log.Errorf("Cannot List Files: %s From: %s ServerName: %s Err: %s", remoteFolder, remoteServer, serverName, err.Error())

		return
	}

	var path string
	var fName string
	var ext string
	var tFile int
	for _, f := range files {

		s := strings.Split(f, " ")
		for _, fs := range s {

			if strings.Contains(fs, filePrefix) && strings.Contains(fs, currentDate) {

				if strings.Contains(f, ":") {
					dSplit := strings.Split(f, ":")
					tSplit := strings.Split(dSplit[0], "  ")
					tData, _ := strconv.Atoi(tSplit[1])
					if tFile == 0 {
						tFile = tData
						path = filepath.Join(curpath, strings.TrimSpace(fs))
						ext = filepath.Ext(strings.TrimSpace(fs))
						fName = serverName + "_" + dateNaming + ext

					}
					if tFile < tData {
						tFile = tData
						path = filepath.Join(curpath, strings.TrimSpace(fs))
						ext = filepath.Ext(strings.TrimSpace(fs))
						fName = serverName + "_" + dateNaming + ext

					}
				}

			}
		}

	}

	if fName == "" {
		log.Errorf("Cannot Find Files In: %s From: %s", remoteFolder, serverName)
		downloadFailed++
		return
	}
	_, err = ftp.Retr(path, func(r io.Reader) error {

		var regionBuf, nationalBuf bytes.Buffer

		writer := io.MultiWriter(&regionBuf, &nationalBuf)

		if _, err = io.Copy(writer, r); err != nil {
			downloadFailed++
			return err
		}

		destinationRegion, err := os.Create(fmt.Sprintf("%s/%s", region, fName))
		if err != nil {
			downloadFailed++
			return err
		}
		if _, err = io.Copy(destinationRegion, &regionBuf); err != nil {
			downloadFailed++
			return err
		}

		destinationNational, err := os.Create(fmt.Sprintf("%s/%s", national, fName))
		if err != nil {
			downloadFailed++
			return err
		}
		if _, err = io.Copy(destinationNational, &nationalBuf); err != nil {
			downloadFailed++
			return err
		}

		return nil
	})
	if err != nil {
		downloadFailed++
		return
	}
	log.Printf("Download: %s From: %s To: %s", fName, serverName, region)

}

func AppInfo() string {
	return "Huawei Dump 2G/3G Maker - Kukuh Wikartomo - 2021 v2021.12 | kukuh.wikartomo@huawei.com"
}

func dataProcess(techName string, currentDate string, info chan string, skipDoubleSlash, rawOnly, keepCsv bool) string {

	var jsonFile string
	var fileName string
	if techName == "2G" {
		jsonFile = "./listbsc2g.json"
		fileName = "bsc"
	} else {
		jsonFile = "./listrnc3g.json"
		fileName = "huawei"
	}

	if _, err := os.Stat("./EMPTY.accdb"); os.IsNotExist(err) {
		log.Fatalf("No 'EMPTY.accdb' Found")
	}

	c, err := ioutil.ReadFile(jsonFile)
	if err != nil {
		panic(err)
	}
	var ftpConfigs []configs.Config
	err = json.Unmarshal(c, &ftpConfigs)
	if err != nil {
		panic(err)
	}

	for i := range ftpConfigs {
		ftpConfigs[i].FillDate(currentDate)
	}

	mapConfig := make(map[string]string)

	for _, f := range ftpConfigs {
		ipAddr := strings.Split(f.RemoteServer, ":")
		fPrefix := f.FilePrefix
		fullPrefix := fmt.Sprintf("%s%s", fPrefix, ipAddr[0])
		mapConfig[fullPrefix] = f.FtpName
	}

	if err := os.MkdirAll("result", 0666); err != nil {
		panic(err)
	}

	if err := os.MkdirAll(filepath.Join("result", currentDate), 0666); err != nil {
		panic(err)
	}

	if err := os.MkdirAll(filepath.Join("result", currentDate, techName), 0666); err != nil {
		panic(err)
	}

	if err := os.MkdirAll(filepath.Join("result", currentDate, techName, "National"), 0666); err != nil {
		panic(err)
	}

	resultNational := filepath.Join("result", currentDate, techName, "National")
	resultRegion := filepath.Join("result", currentDate, techName)

	go processDownload(techName, ftpConfigs, info, resultRegion, resultNational, currentDate)

	logInfo := <-info
	log.Info(logInfo)

	if rawOnly {
		return ""
	}

	nationalMapPart := make(map[string][]string)
	for _, c := range ftpConfigs {
		if c.Part != "0" {
			if _, ok := nationalMapPart[c.Part]; !ok {
				nationalMapPart[c.Part] = append(nationalMapPart[c.Part], c.FtpName)
			} else {
				nationalMapPart[c.Part] = append(nationalMapPart[c.Part], c.FtpName)
			}
		}

	}

	logStd.Println("Extracting Data For National")
	err = unArr(filepath.Join("result", currentDate, techName, "National"), "0", "", true, currentDate)
	if err != nil {
		log.Errorf("Cannot Extract File From: %s", "National")
	}

	regionMap := make(map[string]int)
	for _, c := range ftpConfigs {
		if _, ok := regionMap[c.Region]; !ok {
			regionMap[c.Region] = 0
		}
	}

	// extract region only
	for r := range regionMap {
		logStd.Printf("Extracting Data For %s\n", r)
		err = unArr(filepath.Join("result", currentDate, techName, r), "0", "", false, currentDate)
		if err != nil {
			log.Errorf("Cannot Extract File From: %s", r)
		}
	}

	t := listAccessLocation(filepath.Join("result", currentDate, techName))

	var accessTemplate []byte

	if t != nil {
		accessTemplate, err = ioutil.ReadFile(`./EMPTY.accdb`)
		if err != nil {
			log.Println(err)
			return ""
		}

	}

	// creating accdb for region
	parentDir, _ := os.Getwd()
	var accFolder string
	for k, _ := range t {
		if !strings.Contains(k, "National") {
			accFolder = k

			err := ioutil.WriteFile(filepath.Join(parentDir, "result", currentDate, techName, accFolder, (techName+"_HW_"+accFolder+"_"+currentDate+".accdb")), accessTemplate, 0755)
			if err != nil {
				log.Error("Error creating", filepath.Join(parentDir, "result", currentDate, techName, accFolder, (techName+"_HW_"+accFolder+"_"+currentDate+".accdb")))
				return ""
			}
		}
	}

	if len(nationalMapPart) == 0 {
		err := ioutil.WriteFile(filepath.Join(parentDir, "result", currentDate, techName, "National", (techName+"_DUMP_HW_"+"National"+"_"+currentDate+".accdb")), accessTemplate, 0755)
		if err != nil {
			log.Error("Error creating", filepath.Join(parentDir, "result", currentDate, techName, "National", (techName+"_DUMP_HW_"+"National"+"_"+currentDate+".accdb")))
			return ""
		}
	}

	for part, _ := range nationalMapPart {
		err := ioutil.WriteFile(filepath.Join(parentDir, "result", currentDate, techName, "National", (techName+"_DUMP_HW_"+"National_"+part+"_"+currentDate+".accdb")), accessTemplate, 0755)
		if err != nil {
			log.Error("Error creating", filepath.Join(parentDir, "result", currentDate, techName, "National", (techName+"_DUMP_HW_"+"National_"+part+"_"+currentDate+".accdb")))
			return ""
		}
	}

	// Checking Created National Folder
	files, err := ioutil.ReadDir(filepath.Join(parentDir, "result", currentDate, techName, "National"))
	if err != nil {
		log.Errorf("Failed to List Folder: %s", filepath.Join(parentDir, "result", currentDate, techName, "National"))
	}
	nationalSplit := false
	for _, f := range files {
		if f.IsDir() {
			nationalSplit = true
		}
	}

	if nationalSplit {
		listDeleted := find(filepath.Join(parentDir, "result", currentDate, techName, "National"), ".txt")
		for _, l := range listDeleted {
			if filepath.Base(filepath.Dir(l)) == "National" {
				err := os.Remove(l)
				if err != nil {
					log.Errorf("Failed to delete redundant files: %s", l)
				}
			}

		}
		err := os.Remove(filepath.Join(parentDir, "result", currentDate, techName, "National", techName+"_HW_National"+"_"+currentDate+".accdb"))
		if err != nil {
			log.Errorf("Failed to delete redundant files: %s", filepath.Join(parentDir, "result", currentDate, techName, "National", techName+"_HW_National"+"_"+currentDate+".accdb"))
		}
	}

	t = listAccessLocation(filepath.Join("result", currentDate, techName))

	var wg sync.WaitGroup
	// var part int
	// if len(nationalMapPart) > 0 {
	// 	part = len(nationalMapPart) - 1
	// }
	// fmt.Println("part:", part, t)
	wg.Add(len(t))

	for k, v := range t {
		if err := os.MkdirAll(filepath.Join("result", currentDate, techName, k, "_dumpresult"), 0666); err != nil {
			panic(err)
		}
		if strings.Contains(k, "National") {

			go MainProcess(v, filepath.Join(parentDir, "result", currentDate, techName, k, "_dumpresult"), skipDoubleSlash, fileName, true, keepCsv, filepath.Join(parentDir, "result", currentDate, techName, k, (techName+"_DUMP_HW_"+k+"_"+currentDate+".accdb")), false, &wg, nationalMapPart, currentDate, ftpConfigs, mapConfig)
		} else {

			go MainProcess(v, filepath.Join(parentDir, "result", currentDate, techName, k, "_dumpresult"), skipDoubleSlash, fileName, true, keepCsv, filepath.Join(parentDir, "result", currentDate, techName, k, (techName+"_HW_"+k+"_"+currentDate+".accdb")), false, &wg, nationalMapPart, currentDate, ftpConfigs, mapConfig)
		}

	}

	wg.Wait()
	logStd.Println("Parsing Raw Data Done")
	// Access Export

	var wg2 sync.WaitGroup
	wg2.Add(len(t))
	for k, v := range t {

		if strings.Contains(k, "National") {

			GoAccess(v, filepath.Join(parentDir, "result", currentDate, techName, k, "_dumpresult"), skipDoubleSlash, fileName, true, keepCsv, filepath.Join(parentDir, "result", currentDate, techName, k, (techName+"_DUMP_HW_"+k+"_"+currentDate+".accdb")), false, nationalMapPart, currentDate, ftpConfigs, mapConfig, &wg2)
		} else {

			GoAccess(v, filepath.Join(parentDir, "result", currentDate, techName, k, "_dumpresult"), skipDoubleSlash, fileName, true, keepCsv, filepath.Join(parentDir, "result", currentDate, techName, k, (techName+"_HW_"+k+"_"+currentDate+".accdb")), false, nationalMapPart, currentDate, ftpConfigs, mapConfig, &wg2)
		}

	}

	wg2.Wait()
	logStd.Println("Creating Access Done")
	// for k, v := range t {
	// 	if err := os.MkdirAll(filepath.Join("result", currentDate, techName, k, "_dumpresult"), 0666); err != nil {
	// 		panic(err)
	// 	}
	// 	if strings.Contains(k, "National") {
	// 		MainProcess(v, filepath.Join(parentDir, "result", currentDate, techName, k, "_dumpresult"), skipDoubleSlash, fileName, true, keepCsv, filepath.Join(parentDir, "result", currentDate, techName, k, (techName+"_DUMP_HW_"+k+"_"+currentDate+".accdb")), false, &wg, nationalMapPart, currentDate, ftpConfigs, mapConfig)
	// 	}

	// }

	for k, v := range t {
		for _, s := range find(v, ".txt") {
			if nationalSplit && strings.Contains(filepath.Base(v), "National") {
				if !keepCsv {
					err := os.RemoveAll(v)
					if err != nil {
						log.Errorf("Failed To Remove Temp File: %s", s)
					}
				}
			} else {
				err := os.Remove(s)
				if err != nil {
					log.Errorf("Failed To Remove Temp File: %s", s)
				}
			}
		}
		if !keepCsv {
			if err := os.RemoveAll(filepath.Join(parentDir, "result", currentDate, techName, k, "_dumpresult")); err != nil {
				log.Errorf("Error Delete Temp Dir: %s", filepath.Join(parentDir, "result", currentDate, techName, k, "_dumpresult"))

			}
		}
	}

	if !keepCsv {
		logStd.Println("Removing Temp Files Done")
	}

	for _, v := range t {
		for _, s := range find(v, ".accdb") {

			if err := zipSource(s, fmt.Sprintf(`%s.zip`, strings.TrimSuffix(s, path.Ext(s)))); err != nil {
				log.Errorf("Failed To Zip File: %s", s)
			} else {
				err := os.Remove(s)
				if err != nil {
					log.Errorf("Failed To Remove Original File: %s", s)
				}
			}

		}
	}

	if nationalSplit {
		for _, s := range find(filepath.Join(parentDir, "result", currentDate, techName, "National"), ".accdb") {
			if err := zipSource(s, fmt.Sprintf(`%s.zip`, strings.TrimSuffix(s, path.Ext(s)))); err != nil {
				log.Errorf("Failed To Zip File: %s", s)
			} else {
				err := os.Remove(s)
				if err != nil {
					log.Errorf("Failed To Remove Original File: %s", s)
				}
			}

		}
	}
	return filepath.Join(parentDir, resultNational)
}

func main() {
	flagTech := flag.String("tech", "", "Technology 2g/3g")
	flagSkippedComment := flag.Bool("skip-comment", true, "Skipped // Lines")
	flagGetDate := flag.String("date", "", "Get Specific Date in yyyymmdd")
	flagRawOnly := flag.Bool("raw", false, "Get Raw Only")
	flagKeepCSV := flag.Bool("keep-csv", false, "Keep Generated CSV for checking")
	flagCopyToFolder := flag.String("copy-to", "", "Copy National Dump Result to Folder")
	flag.Parse()
	techName := strings.TrimSpace(strings.ToUpper(*flagTech))
	skipDoubleSlash := *flagSkippedComment
	rawOnly := *flagRawOnly
	getDate := *flagGetDate
	keepCSV := *flagKeepCSV
	copyToFolder := *flagCopyToFolder

	if techName == "" {
		logStd.Fatalf("Technology not defined")
	}

	var currentDate string
	techName = strings.ToUpper(strings.TrimSpace(techName))

	timeStart := time.Now()
	logStd.Println(AppInfo())
	if getDate == "" {
		currentDate = time.Now().Format("20060102")
	} else {
		currentDate = getDate
	}

	if techName == "2G" {
		f, err := os.OpenFile(currentDate+"_"+techName+"_LOG.txt", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			fmt.Printf("error opening file: %v", err)
		}

		defer f.Close()
		log.SetFormatter(&log.JSONFormatter{})
		log.SetOutput(f)
		log.Info(AppInfo())
		info2g := make(chan string)

		logStd.Println("Starting 2G For", currentDate)
		resultNationalFolder := dataProcess("2G", currentDate, info2g, skipDoubleSlash, rawOnly, keepCSV)
		if copyToFolder != "" {
			res, err := copyNationalResultToFolder(resultNationalFolder, filepath.Join(copyToFolder, currentDate), "2G")
			if err != nil {
				log.Errorf("Cannot Copy To Destination Folder %s", err.Error())
			}
			for _, r := range res {
				log.Infof("Copy result %s", r)
			}

		}
		log.Info("Done in: ", time.Since(timeStart))
	} else {
		f, err := os.OpenFile(currentDate+"_"+techName+"_LOG.txt", os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			fmt.Printf("error opening file: %v", err)
		}

		defer f.Close()
		log.SetFormatter(&log.JSONFormatter{})
		log.SetOutput(f)
		log.Info(AppInfo())
		info3g := make(chan string)

		logStd.Println("Starting 3G For", currentDate)
		resultNationalFolder := dataProcess("3G", currentDate, info3g, skipDoubleSlash, rawOnly, keepCSV)
		if copyToFolder != "" {
			res, err := copyNationalResultToFolder(resultNationalFolder, filepath.Join(copyToFolder, currentDate), "3G")
			if err != nil {
				log.Errorf("Cannot Copy To Destination Folder %s", err.Error())
			}
			for _, r := range res {
				log.Infof("Copy result %s", r)
			}

		}
		log.Info("Done in: ", time.Since(timeStart))
	}

	logStd.Println("Done in:", time.Since(timeStart))
	logStd.Println("More Details See Logfile:", currentDate+"_"+techName+"_LOG.txt")

}

func processDownload(techName string, ftpConfigs []configs.Config, info chan string, resultRegion, resultNational, currentDate string) {

	wgDone := make(chan bool)
	var wg sync.WaitGroup
	wg.Add(len(ftpConfigs))
	startTime := time.Now()

	for _, f := range ftpConfigs {
		if err := os.MkdirAll(filepath.Join(resultRegion, f.Region), 0666); err != nil {
			panic(err)
		}
	}

	totalF := 0
	for _, f := range ftpConfigs {
		go ftpDownload(f.RemoteServer, f.RemoteFolder, f.RemoteUser, f.RemotePass, f.DateFind, f.FtpName, f.FilePrefix, &wg, filepath.Join(resultRegion, f.Region), resultNational, currentDate)
		totalF++

	}

	go func() {
		wg.Wait()
		close(wgDone)
	}()

	select {
	case <-wgDone:
		info <- fmt.Sprintf(techName+" - %v out of %v Files Downloaded In - %s", (totalF - downloadFailed), totalF, time.Since(startTime))
		break
		// case err := <-fatalErrors:
		// 	close(fatalErrors)
		// 	info <- err.Error()
	}

}

func unArr(location string, part string, ftpName string, isNational bool, currentDate string) error {

	if isNational {
		parentDir, _ := os.Getwd()
		err := filepath.Walk(location,
			func(files string, info os.FileInfo, err error) error {
				if err != nil {
					log.Error(err)
				}

				if path.Ext(info.Name()) == ".zip" {
					if strings.Contains(filepath.Dir(files), "National") && strings.TrimSuffix(info.Name(), path.Ext(info.Name())) == ftpName+"_"+currentDate && part != "0" {
						a, err := unarr.NewArchive(filepath.Join(parentDir, filepath.Dir(files), info.Name()))
						if err != nil {
							return fmt.Errorf("Cannot Extract: %s", filepath.Join(parentDir, filepath.Dir(files), info.Name()))
						}
						if err := os.MkdirAll(filepath.Join(parentDir, filepath.Dir(files), "National_"+part), 0666); err != nil {
							panic(err)
						}
						a.Extract(filepath.Join(parentDir, filepath.Dir(files), "National_"+part))

					}
					if strings.Contains(filepath.Dir(files), "National") && part == "0" {
						a, err := unarr.NewArchive(filepath.Join(parentDir, filepath.Dir(files), info.Name()))
						if err != nil {
							return fmt.Errorf("Cannot Extract: %s", filepath.Join(parentDir, filepath.Dir(files), info.Name()))
						}

						a.Extract(filepath.Join(parentDir, filepath.Dir(files)))

					}

				}
				return nil
			})
		if err != nil {
			return err
		}
	}

	if !isNational {
		parentDir, _ := os.Getwd()
		err := filepath.Walk(location,
			func(files string, info os.FileInfo, err error) error {
				if err != nil {
					log.Error(err)
				}

				if path.Ext(info.Name()) == ".zip" {

					a, err := unarr.NewArchive(filepath.Join(parentDir, filepath.Dir(files), info.Name()))
					if err != nil {
						return fmt.Errorf("Cannot Extract: %s", filepath.Join(parentDir, filepath.Dir(files), info.Name()))
					}
					a.Extract(filepath.Join(parentDir, filepath.Dir(files)))

				}
				return nil
			})
		if err != nil {
			return err
		}
	}
	return nil
}

func listAccessLocation(location string) map[string]string {
	accessDestination := make(map[string]string)
	parentDir, _ := os.Getwd()
	err := filepath.Walk(location,
		func(files string, info os.FileInfo, err error) error {
			if err != nil {
				log.Error(err)
			}
			if path.Ext(info.Name()) == ".txt" {
				if _, ok := accessDestination[filepath.Base(filepath.Dir(files))]; !ok {
					accessDestination[filepath.Base(filepath.Dir(files))] = filepath.Join(parentDir, filepath.Dir(files))
				}
			}
			return nil
		})
	if err != nil {
		return nil
	}
	return accessDestination
}

func MainProcess(sourceDir string, resultDir string, skipDoubleSlash bool, techNeName string, isAccess, keepCSV bool, dbName string, isLogOut bool, wg *sync.WaitGroup, nationalPart map[string][]string, currentDate string, ftpConfigs []configs.Config, mapConfig map[string]string) {
	defer wg.Done()
	tables := make(map[string]*configs.Table)
	files, err := ioutil.ReadDir(sourceDir)
	if err != nil {
		log.Fatal(err)
	}
	// var storeNeName string
	for _, file := range files {

		if path.Ext(file.Name()) == ".txt" {
			// Split File Name to Value from mapConfig --> CFGMML-RNC1091-10.5.99.18
			nameSplit := strings.Split(file.Name(), "-")
			checkName := strings.TrimSpace(fmt.Sprintf("%s-%s-%s", nameSplit[0], nameSplit[1], nameSplit[2]))
			if _, ok := mapConfig[checkName]; !ok {
				log.Errorf("Fail to get NeName from Config from file %s", file.Name())
				return
			}
			neName := mapConfig[checkName]

			fullName := filepath.Join(sourceDir, file.Name())
			log.Infof("Processing: %s", fullName)
			f, err := os.Open(fullName)
			if err != nil {
				log.Errorf("Error Open File: %s", fullName)
				return
			}
			// neName := ""

			scanner := bufio.NewScanner(f)
			it := 1
			for scanner.Scan() {
				it++
				if it < 10 {
					continue
				}
				line := scanner.Text()
				if strings.TrimSpace(line) == "" {
					continue
				}

				if line[:2] == "//" && skipDoubleSlash {
					continue
				}

				if !skipDoubleSlash {
					line = strings.ReplaceAll(scanner.Text(), "//", "")
				}

				arrStr := strings.Split(line, ":")
				if len(arrStr) < 2 {
					continue
				}

				tblName := strings.TrimSpace(arrStr[0])
				if len(tblName) < 1 {
					continue
				}

				if _, ok := tables[tblName]; !ok {
					table, err := MakeNewTable(tblName, resultDir)
					if err != nil {
						panic(err)
					}

					tables[tblName] = table
				}

				table := tables[tblName]
				arrStr[1] = strings.ReplaceAll(arrStr[1], ";", "")
				keyVals := strings.Split(arrStr[1], ",")
				isSubKey := false
				row := make([]string, len(table.Header))
				for _, kv := range keyVals {
					keyVal := strings.Split(kv, "=")
					key := strings.TrimSpace(keyVal[0])
					val := ""

					// Check if Val is Concatenated Param
					// HSPAPLUSSWITCH=
					// 64QAM-1
					// &MIMO-0
					// &E_FACH-0
					// &DTX_DRX-0
					// &HS_SCCH_LESS_OPERATION-0
					// &DL_L2ENHANCED-1
					// &64QAM_MIMO-0
					// &UL_16QAM-1
					// &DC_HSDPA-0
					// &UL_L2ENHANCED-1
					// &EDPCCH_BOOSTING-0
					// &DCMIMO_HSDPA-0
					// &E_DRX-0
					// &DC_HSUPA-0
					// &HSDPA_4C_MIMO-0
					// &HSDPA_4C-0
					// &DBMIMO_HSDPA-0
					// &DB_HSDPA-0
					// &HSDPA_SFDC-0
					// &HSDPA_DF3C-0
					// &INTERNBDB_HSDPA-0
					// &FDPCH_CAPABILITY_INVALID-0,

					if len(keyVal) > 1 {
						val = keyVal[1]

						// Check if thereis Sub Value with & But Not Col Remark & But Not Col CELLNAME Due to i.e."604360_CL&T_073_3G-1"
						if strings.Contains(val, "&") && key != "REMARK" && !strings.Contains(key, "CELLNAME") {
							isSubKey = true

							cKeyVal := strings.Split(val, "&")
							for _, cV := range cKeyVal {
								subKeyVal := strings.Split(cV, "-")
								if len(subKeyVal) > 1 {

									subKey := fmt.Sprintf("%s_%v", key, strings.TrimSpace(subKeyVal[0]))
									subVal := fmt.Sprintf("%v", subKeyVal[1])

									if idx, ok := table.HeaderMap[subKey]; !ok {
										table.HeaderMap[subKey] = int64(len(table.Header))
										table.Header = append(table.Header, subKey)

										row = append(row, subVal)

									} else {
										row[idx] = subVal
									}

								}

							}

						} else {
							isSubKey = false
						}

						if len(val) > 2 && val[:2] == "H'" {
							output, err := strconv.ParseInt(hexaNumberToInteger(val[2:]), 16, 64)
							if err != nil {
								logStd.Println(err)
							}
							val = fmt.Sprintf("%v", output)
						}
						// SFXX to Access Get Converted into $ Currency -- Store as Text
						if len(val) > 2 && val[:2] == "SF" {
							val = fmt.Sprintf("%q", val)
						}

					}

					if !isSubKey {
						if idx, ok := table.HeaderMap[key]; !ok {
							table.HeaderMap[key] = int64(len(table.Header))
							table.Header = append(table.Header, key)

							row = append(row, val)

						} else {
							row[idx] = val
						}
					}

				}

				content := append([]byte(neName), []byte(strings.Join(row, ",")+"\n")...)
				if _, err := table.File.Write(content); err != nil {
					panic(err)
				}

			}

		}

	}
	for _, table := range tables {
		defer table.File.Close()
		content, err := os.ReadFile(table.Fpath)
		if err != nil {
			log.Errorf("Error Reading File: %s", table.Fpath)
			return
		}

		buffer := new(bytes.Buffer)
		buffer.Write([]byte(strings.Join(table.Header, ",") + "\n"))
		buffer.Write(content)

		if err := os.WriteFile(table.Fpath, buffer.Bytes(), 0666); err != nil {
			log.Errorf("Error Writing File: %s", table.Fpath)
			return
		}

	}

	// TODO: UNTIL THIS PART --> CSV ALREADY COMBINED --> LOOP TABLESS
	for _, table := range tables {

		if len(table.Header) > 255 {
			f, err := os.Open(table.Fpath)
			if err != nil {
				log.Errorf("Error Opening File: %s", table.Fpath)
				return
			}

			defer f.Close()
			csvReader := csv.NewReader(f)
			csvReader.FieldsPerRecord = -1
			csvReader.LazyQuotes = true
			line, err := csvReader.ReadAll()
			if err != nil {
				log.Errorf("Error ReadAll File: %s || %s", err.Error(), f.Name())
			}

			noSplitFile := int(math.Ceil(float64(len(table.Header)) / 255))
			for n := 1; n <= noSplitFile; n++ {
				table.ListFile = append(table.ListFile, fmt.Sprintf("%s_%v.csv", table.Name, n))
				table.TableName = append(table.TableName, fmt.Sprintf("%s %v", table.Name, n))
			}

			dir := filepath.Dir(table.Fpath)

			minC := 1
			maxC := 254
			// TEMPORARY --> UNTIL NOW ONLY THIS MEAS GROUP FOR CELL LEVEL -> GET NE NAME AND CELLID FOR EACH SPLIT
			if strings.Contains(table.Name, "UCELLCOALGOENHPARA") {
				minC = 2
			}
			for i, nf := range table.ListFile {
				csvFile, err := os.Create(filepath.Join(dir, nf))
				if err != nil {
					log.Fatalf("failed creating file: %s", err)
				}
				defer csvFile.Close()

				csvwriter := csv.NewWriter(csvFile)
				defer csvwriter.Flush()
				var data [][]string
				if i == len(table.ListFile)-1 {

					for _, r := range line {
						// TEMPORARY --> UNTIL NOW ONLY THIS MEAS GROUP FOR CELL LEVEL -> GET NE NAME AND CELLID FOR EACH SPLIT
						if strings.Contains(table.Name, "UCELLCOALGOENHPARA") {
							row := append([]string{r[0], r[1]}, r[minC:]...)
							data = append(data, row)
						} else {
							row := append([]string{r[0]}, r[minC:]...)
							data = append(data, row)
						}
					}
				} else {
					for _, r := range line {

						if (len(r)) < maxC {
							maxC = len(r)
						}
						if strings.Contains(table.Name, "UCELLCOALGOENHPARA") {
							row := append([]string{r[0], r[1]}, r[minC:maxC]...)
							data = append(data, row)
						} else {
							row := append([]string{r[0]}, r[minC:maxC]...)
							data = append(data, row)
						}
					}
				}

				csvwriter.WriteAll(data)

				minC = maxC
				maxC = maxC + 254
			}

		} else {
			table.ListFile = append(table.ListFile, fmt.Sprintf("%s.csv", table.Name))
			table.TableName = append(table.TableName, table.Name)
		}
	}

	allTables = tables

}

func MakeNewTable(name string, resultDir string) (*configs.Table, error) {
	fpath := filepath.Join(resultDir, name+".csv")
	f, err := os.Create(fpath)
	if err != nil {
		return nil, err
	}

	return &configs.Table{
		Name:   name,
		Fpath:  fpath,
		Header: []string{"NE NAME"},
		HeaderMap: map[string]int64{
			"NE NAME": 0,
		},
		Buffer: new(bytes.Buffer),
		File:   f,
	}, nil
}

func ExportAccess(tables map[string]*configs.Table, dbName string, resultDir string, isLogOut bool, listNE []string) {
	// fmt.Println(tables)

	pvd := fmt.Sprintf(`DRIVER=Microsoft Access Driver (*.mdb, *.accdb);UID=admin;DBQ=%s;`, dbName)
	db, err := sqlx.Open("odbc", pvd)
	if err != nil && isLogOut {
		log.Errorf("open db %s err %s", dbName, err.Error())
		return
	}
	log.Infof("Processing Access File: %s", dbName)
	defer db.Close()
	if listNE != nil { // for national
		for _, table := range tables {
			checkSplit := len(table.ListFile)

			for t := 0; t < checkSplit; t++ {
				qry := fmt.Sprintf(`SELECT file.* INTO [%s] FROM [Text;FMT=Delimited(,);HDR=YES;DATABASE=%s].[%s] as file where file.[NE NAME] IN (%s)`, table.TableName[t], resultDir, table.ListFile[t], "'"+strings.Join(listNE, "', '")+"'")

				if isLogOut {
					log.Info(qry)
				}
				tx, err := db.Exec(qry)
				if err != nil && isLogOut {
					log.Warnf("Error Inserting %s Retry With Text Data Type", table.TableName[t])
					createTableCol := []string{}

					for _, s := range table.Header {
						createTableCol = append(createTableCol, fmt.Sprintf(`[%s] longtext`, s))
					}

					newQry := fmt.Sprintf(`CREATE TABLE [%s] (%s)`, table.TableName[t], strings.Join(createTableCol, ","))
					_, _ = db.Exec(newQry)
					if isLogOut {
						log.Info(qry)
					}
					if err != nil && isLogOut {
						log.Warnf("Error Creating Table %s Maybe Already Exists, Trying to Insert Values", table.TableName[t])
					}
					qry := fmt.Sprintf(`INSERT INTO [%s] SELECT * FROM [Text;FMT=Delimited(,);HDR=YES;DATABASE=%s].[%s] as file WHERE file.[NE NAME] IN (%s)`, table.TableName[t], resultDir, table.ListFile[t], "'"+strings.Join(listNE, "', '")+"'")
					tx, err = db.Exec(qry)
					if isLogOut {
						log.Info(qry)
					}
					if err != nil && isLogOut {
						log.Errorf("Error Inserting %s - Skipping", table.TableName[t])
						continue
					}
					if isLogOut {
						rowsInserted, _ := tx.RowsAffected()
						log.Infof(`Inserted %s row(s) to [%s]`, strconv.FormatInt(rowsInserted, 10), table.TableName[t])
					}

					continue

				}
				if isLogOut {
					rowsInserted, _ := tx.RowsAffected()
					log.Infof(`Inserted %s row(s) to [%s]`, strconv.FormatInt(rowsInserted, 10), table.TableName[t])
				}
			}

		}
	} else { // For Region
		for _, table := range tables {
			checkSplit := len(table.ListFile)

			for t := 0; t < checkSplit; t++ {

				qry := fmt.Sprintf(`SELECT file.* INTO [%s] FROM [Text;FMT=Delimited(,);HDR=YES;DATABASE=%s].[%s] as file`, table.TableName[t], resultDir, table.ListFile[t])

				if isLogOut {
					log.Info(qry)
				}
				tx, err := db.Exec(qry)
				if err != nil && isLogOut {
					log.Warnf("Error Inserting %s Retry With Text Data Type - %s", table.TableName[t], err.Error())
					createTableCol := []string{}

					for _, s := range table.Header {
						createTableCol = append(createTableCol, fmt.Sprintf(`[%s] longtext`, s))
					}

					newQry := fmt.Sprintf(`CREATE TABLE [%s] (%s)`, table.TableName[t], strings.Join(createTableCol, ","))
					_, _ = db.Exec(newQry)
					if isLogOut {
						log.Info(qry)
					}
					if err != nil && isLogOut {
						log.Warnf("Error Creating Table %s Maybe Already Exists, Trying to Insert Values", table.TableName[t])
					}
					qry := fmt.Sprintf(`INSERT INTO [%s] SELECT * FROM [Text;FMT=Delimited(,);HDR=YES;DATABASE=%s].[%s] as file`, table.TableName[t], resultDir, table.ListFile[t])
					tx, err = db.Exec(qry)
					if isLogOut {
						log.Info(qry)
					}
					if err != nil && isLogOut {
						log.Errorf("Error Inserting %s - Skipping", table.TableName[t])
						continue
					}
					if isLogOut {
						rowsInserted, _ := tx.RowsAffected()
						log.Infof(`Inserted %s row(s) to [%s]`, strconv.FormatInt(rowsInserted, 10), table.TableName[t])
					}

					continue

				}
				if isLogOut {
					rowsInserted, _ := tx.RowsAffected()
					log.Infof(`Inserted %s row(s) to [%s]`, strconv.FormatInt(rowsInserted, 10), table.TableName[t])
				}
			}

		}
	}
}

func find(root, ext string) []string {
	var a []string
	filepath.WalkDir(root, func(s string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if filepath.Ext(d.Name()) == ext {
			a = append(a, s)
		}
		return nil
	})
	return a
}

func IOReadDir(root string) ([]string, error) {
	var files []string
	fileInfo, err := ioutil.ReadDir(root)
	if err != nil {
		return files, err
	}
	for _, file := range fileInfo {
		files = append(files, file.Name())
	}
	return files, nil
}

// https://gosamples.dev/zip-file/
func zipSource(source, target string) error {
	// 1. Create a ZIP file and zip.Writer
	f, err := os.Create(target)
	if err != nil {
		return err
	}
	defer f.Close()

	writer := zip.NewWriter(f)
	defer writer.Close()

	// 2. Go through all the files of the source
	return filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 3. Create a local file header
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}

		// set compression
		header.Method = zip.Deflate

		// 4. Set relative path of a file as the header name
		header.Name, err = filepath.Rel(filepath.Dir(source), path)
		if err != nil {
			return err
		}
		if info.IsDir() {
			header.Name += "/"
		}

		// 5. Create writer for the file header and save content of the file
		headerWriter, err := writer.CreateHeader(header)
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()

		_, err = io.Copy(headerWriter, f)
		return err
	})
}

func hexaNumberToInteger(hexaString string) string {
	// replace 0x or 0X with empty String
	numberStr := strings.Replace(hexaString, "0x", "", -1)
	numberStr = strings.Replace(numberStr, "0X", "", -1)
	return numberStr
}

func GoAccess(sourceDir string, resultDir string, skipDoubleSlash bool, techNeName string, isAccess, keepCSV bool, dbName string, isLogOut bool, nationalPart map[string][]string, currentDate string, ftpConfigs []configs.Config, mapConfig map[string]string, wg2 *sync.WaitGroup) {
	defer wg2.Done()
	dbName2 := filepath.Base(dbName)[:len(filepath.Base(dbName))-len(filepath.Ext(filepath.Base(dbName)))-len(currentDate)]

	// access region
	if isAccess && !strings.Contains(dbName2, "National") {
		logStd.Printf("Populating: %s\n", dbName)
		ExportAccess(allTables, dbName, resultDir, false, nil)
	}

	// access national all
	if isAccess && strings.Contains(dbName2, "National") && len(nationalPart) == 0 {
		logStd.Printf("Populating: %s\n", dbName)
		ExportAccess(allTables, dbName, resultDir, isLogOut, nil)
	}

	// access national part
	if isAccess && strings.Contains(dbName2, "National") && len(nationalPart) != 0 {
		for part, listNe := range nationalPart {
			dbNamePart := filepath.Join(filepath.Dir(dbName), dbName2+part+"_"+currentDate+".accdb")
			logStd.Printf("Populating: %s\n", dbNamePart)

			ExportAccess(allTables, dbNamePart, resultDir, isLogOut, listNe)
		}
	}

}
