package main

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"golang.org/x/net/html"
)

func main() {
	BaseURL := "https://www.dnfsb.gov/documents/reports"
	response, err := http.Get(BaseURL)
	checkError(err)
	defer response.Body.Close()

	token := html.NewTokenizer(response.Body)

	for {
		tt := token.Next()

		switch {
		case tt == html.ErrorToken:
			break

		case tt == html.StartTagToken:
			newToken := token.Token()
			if newToken.Data == "a" {
				for _, a := range newToken.Attr {
					if a.Key == "href" {
						fmt.Println(a.Val)
					}
				}
			}
		}
	}

}

func f(n int) {
	for i := 0; i < n; i++ {
		fmt.Print(i, " ")
		amt := time.Duration(rand.Intn(250))
		time.Sleep(amt * time.Millisecond)
	}
	fmt.Println()
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}
