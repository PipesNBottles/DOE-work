package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/PuerkitoBio/goquery"
)

func main() {
	for i := 0; i < 10; i++ {
		go f(10)
	}
	var input string
	fmt.Scan(&input)
	fmt.Println(input)
}

func f(n int) {
	for i := 0; i < n; i++ {
		fmt.Print(i, " ")
		amt := time.Duration(rand.Intn(250))
		time.Sleep(amt * time.Millisecond)
	}
	fmt.Println()
}

func test() {
	BaseURL := "https://www.dnfsb.gov/documents/reports"
	response, err := http.Get(BaseURL)
	checkError(err)
	defer response.Body.Close()

	document, err := goquery.NewDocumentFromReader(response.Body)
	checkError(err)
	document.Find("a").Each(processElement)

}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}

func processElement(index int, element *goquery.Selection) {
	href, exists := element.Attr("href")
	if exists {
		fmt.Println(href)
	}
}
