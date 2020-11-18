package main

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// Есть источник данных, это может (быть база данных, АПИ м пр.) генерирующий последовательность данных  c определенной
// частотой "Ч" операций/секунду, в данном примере, это функция producer, выдающая последовательно надор натуральных
// чисел от 1 до limit.
//
// Есть приемник данных, проводящий сложные манипуляции с входящими данными и к примеру сохраняющий результат в другую
// базу данных, умеющий обрабатывать входящие данные с частотой "Ч"/N операциций в секунду, в данном примере это функция
// processor, вычисляющая квадраты входящего значения, где с помощью паузы выполнения эмулируется длительное выполнений
// операции.
//
// Для эффективного выполнения задачи, требуется согласовать источник данных и примник данных, путем параллельной
// обработки в потребителе, с ограничение степени параллелизма обработки в размере concurrencySize. Таким образом
// потребитель при получении данных запускает не более чем concurrencySize обработчиков,
//
//                               processor
//		producer -> consumer ->  processor -> terminator (выводит на экран результат, в наеш случае, суммы квадратов входящих наруальных чисел)
//                               ...
//                               processor
//
// При возникновении ошибки обработки, требуется отменить все последующие расчеты, и вернуть ошибку

const (
	//limit           = 1000
	limit           = 10
	concurrencySize = 5
)

func producer(limit int) <-chan int {
	ch := make(chan int, limit)

	go func(ch chan<- int, limit int) {
		for i := 0; i < limit; i++ {
			_, _ = fmt.Fprintf(os.Stdout, "[producer] write value = %d into ch \n", i+1)

			ch <- i+1
		}

		close(ch)
	}(ch, limit)

	return ch
}

func processor(i int) (int, error) {
	_, _ = fmt.Fprintf(os.Stdout, "[processor] process value = %d \n", i)

	if i == 10 {
		return 0, errors.New("i hate 5")
	}

	time.Sleep(5 * time.Second)

	return i * i, nil
}

func terminator(results <-chan int) {
	for v := range results {
		_, _ = fmt.Fprintf(os.Stdout, "[terminator] print value = %d \n", v)
	}
}

func consumer(in <-chan int) error {
	var (
		out   = make(chan int, limit)
		w     = make(chan int, concurrencySize)

		wg = sync.WaitGroup{}	// Для ожидания завершения горутин-воркеров
	)
	wg.Add(concurrencySize)

	go terminator(out)

	for i := 0; i < concurrencySize; i++ {
		go func(in <-chan int, out chan<- int, wg *sync.WaitGroup) {
			for v := range w {
				res, err := processor(v)
				if err != nil {
					panic(err)
				}

				out <- res
			}

			wg.Done()
		}(w, out, &wg)
	}

	for v := range in {
		_, _ = fmt.Fprintf(os.Stdout, "[consumer] consume value = %d \n", v)
		w <- v
	}

	close(w)

	wg.Wait()

	close(out)

	return nil
}

func main() {
	err := consumer(producer(limit))
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
	}
}