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
	limit           = 1000
	//limit           = 9
	//limit           = 10
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

func consumer(in <-chan int) (err error) {
	var (
		out   = make(chan int, limit)
		w     = make(chan int, concurrencySize)
		errCh = make(chan error, 1)

		wg = sync.WaitGroup{}	// Для ожидания завершения горутин-воркеров
	)
	wg.Add(concurrencySize)

	defer func(ch chan error, err *error) {
		// Не совсем нравится такой вариант, потому что, на мой взгляд, могут быть проблемы, когда processor() будет
		// возвращать разные ошибки и в этоге consumer() может вернуть не ту ошибку. Но такой "хак" необходим, чтобы
		// обработать случай, когда limit=10, потому что там хоть и возникает ошибка, но for-select, который проходит
		// по каналу видит, что всё нормально
		close(ch)

		if e, ok := <-ch; ok {
			*err = e
		}
	}(errCh, &err)

	defer close(out)
	defer wg.Wait()
	defer close(w)

	go terminator(out)

	for i := 0; i < concurrencySize; i++ {
		go func(in <-chan int, out chan<- int, errCh chan<- error, wg *sync.WaitGroup, i int) {
			_, _ = fmt.Fprintf(os.Stdout, "[worker #%d] starting \n", i)

			defer wg.Done()
			defer fmt.Fprintf(os.Stdout, "[worker #%d] stopping \n", i)

			for v := range in {
				res, err := processor(v)
				if err != nil {
					_, _ = fmt.Fprintf(os.Stdout, "[worker #%d] got error %v \n", i, err)
					errCh <- err
					return
				}

				out <- res
			}
		}(w, out, errCh, &wg, i+1)
	}

	for {
		select {
		case v, ok := <-in:
			if !ok {
				return nil
			}

			_, _ = fmt.Fprintf(os.Stdout, "[consumer] consume value = %d \n", v)
			w <- v
		case err = <-errCh:
			_, _ = fmt.Fprintf(os.Stdout, "[consumer] got error %v \n", err)
			return err
		}
	}
}

func main() {
	if err := consumer(producer(limit)); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "[main] got error: %v \n", err)
	}
}