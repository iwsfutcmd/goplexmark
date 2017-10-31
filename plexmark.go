package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"time"

	_ "github.com/lib/pq"
	"github.com/pkg/profile"
)

type Expr struct {
	Text  string
	Score int
}

type PreppedExpr struct {
	PreppedText []rune
	Score       int
}

type Chain struct {
	Model Model
}

type Model map[[stateSize]rune]map[rune]int

const (
	begin = '\u0002'
	end   = '\u0003'
)

const stateSize = 4

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
func accumulate(input []int) (output []int) {
	currTotal := 0
	for _, n := range input {
		currTotal += n
		output = append(output, currTotal)
	}
	return
}

func prepString(s string) []rune {
	runeString := []rune(s)
	items := make([]rune, 0, stateSize+len(runeString)+1)
	for i := 0; i < stateSize; i++ {
		items = append(items, begin)
	}
	items = append(items, runeString...)
	return append(items, end)
}

func buildModel(corpus *sql.Rows) Model {
	model := make(Model)
	for corpus.Next() {
		var text string
		var score int
		err := corpus.Scan(&text, &score)
		checkErr(err)
		items := prepString(text)
		for i := 0; i < len(items)-stateSize; i++ {
			var state [stateSize]rune
			copy(state[:], items[i:i+stateSize])
			follow := items[i+stateSize]
			_, ok := model[state]
			if !ok {
				model[state] = make(map[rune]int)
			}
			model[state][follow] += score
		}
	}
	return model
}

func prepExprs(exprs *sql.Rows, c chan PreppedExpr) {
	// for _, expr := range exprs {
	for exprs.Next() {
		var text string
		var score int
		err := exprs.Scan(&text, &score)
		checkErr(err)
		runeString := []rune(text)
		items := make([]rune, 0, stateSize+len(runeString)+1)
		for i := 0; i < stateSize; i++ {
			items = append(items, begin)
		}
		items = append(items, runeString...)
		c <- PreppedExpr{append(items, end), score}
	}
	close(c)
}

func buildModelConc(corpus *sql.Rows) Model {
	model := make(Model)
	c := make(chan PreppedExpr)
	go prepExprs(corpus, c)
	for preppedExpr := range c {
		for i := 0; i < len(preppedExpr.PreppedText)-stateSize; i++ {
			var state [stateSize]rune
			copy(state[:], preppedExpr.PreppedText[i:i+stateSize])
			follow := preppedExpr.PreppedText[i+stateSize]
			_, ok := model[state]
			if !ok {
				model[state] = make(map[rune]int)
			}
			model[state][follow] += preppedExpr.Score
		}
	}
	return model
}

func move(chain Chain, state [stateSize]rune) rune {
	var choices []rune
	var weights []int
	for choice, weight := range chain.Model[state] {
		choices = append(choices, choice)
		weights = append(weights, weight)
	}
	cumdist := accumulate(weights)
	r := rand.Intn(cumdist[len(cumdist)-1])
	for i, n := range cumdist {
		if r < n {
			return choices[i]
		}
	}
	return 0
}

func walk(chain Chain) string {
	var output []rune
	var char rune
	var state [stateSize]rune
	for i := 0; i < stateSize; i++ {
		state[i] = begin
	}
	for char != end {
		char = move(chain, state)
		output = append(output, char)
		for i := 1; i < stateSize; i++ {
			state[i-1] = state[i]
		}
		state[stateSize-1] = char
	}
	return string(output[:len(output)-1])
}

func pullExprFromDB(uid string) *sql.Rows {
	dbinfo := fmt.Sprintf("user=%s password=%s dbname=%s sslmode=disable", DB_USER, DB_PASSWORD, DB_NAME)
	db, err := sql.Open("postgres", dbinfo)
	checkErr(err)
	const query = `
		SELECT txt, score
		FROM exprx
		WHERE langvar = uid_langvar($1)
		`
	rows, err := db.Query(query, uid)
	checkErr(err)
	return rows
}

func main() {
	defer profile.Start().Stop()
	rand.Seed(int64(time.Now().Nanosecond()))
	var start time.Time
	var elapsed time.Duration
	exprs := pullExprFromDB(os.Args[1])
	var chain Chain
	start = time.Now()
	chain.Model = buildModelConc(exprs)
	elapsed = time.Since(start)
	fmt.Printf("conc model building took %s\n", elapsed)
	exprs = pullExprFromDB(os.Args[1])
	start = time.Now()
	chain.Model = buildModel(exprs)
	elapsed = time.Since(start)
	fmt.Printf("model building took %s\n", elapsed)
	start = time.Now()
	for i := 0; i < 10; i++ {
		fmt.Println(walk(chain))
	}
	elapsed = time.Since(start)
	fmt.Printf("generating 10 runs took %s\n", elapsed)

}
