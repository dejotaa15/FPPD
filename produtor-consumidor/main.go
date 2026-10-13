// Produtor/Consumidor com buffer limitado, em duas versões:
// channel com capacidade K e buffer circular protegido por semáforos.
//
// Uso:
//
//	go run ./produtor-consumidor -p 4 -c 4 -k 10 -itens 1000 -versao semaforo
package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"time"
)

type config struct {
	p, c, k int
	itens   int // itens por produtor
	versao  string
	tprod   time.Duration
	tcons   time.Duration
	timeout time.Duration
	amostra time.Duration
	verbose bool
	csv     bool
}

// Estatísticas individuais: cada goroutine escreve só na sua e o main lê
// depois do Wait, então não precisam de proteção.
type estProdutor struct {
	produzidos int
	bloqueado  time.Duration // tempo total dentro de Inserir
}

type estConsumidor struct {
	recebidos []int         // todos os itens retirados, para a verificação final
	esperando time.Duration // tempo total dentro de Retirar até chegar um item
	timeouts  int
}

type estMonitor struct {
	amostras int
	soma     int
	maximo   int
}

func main() {
	var cfg config
	flag.IntVar(&cfg.p, "p", 4, "número de produtores (P)")
	flag.IntVar(&cfg.c, "c", 4, "número de consumidores (C)")
	flag.IntVar(&cfg.k, "k", 10, "capacidade do buffer (K)")
	flag.IntVar(&cfg.itens, "itens", 1000, "itens produzidos por cada produtor")
	flag.StringVar(&cfg.versao, "versao", "channel", "channel | semaforo")
	flag.DurationVar(&cfg.tprod, "tprod", 0, "tempo máximo para produzir um item (sorteado entre 0 e o valor)")
	flag.DurationVar(&cfg.tcons, "tcons", 0, "tempo máximo para consumir um item (sorteado entre 0 e o valor)")
	flag.DurationVar(&cfg.timeout, "timeout", 50*time.Millisecond, "quanto o consumidor espera por um item antes do comportamento alternativo")
	flag.DurationVar(&cfg.amostra, "amostra", 200*time.Microsecond, "intervalo de amostragem da ocupação do buffer")
	flag.BoolVar(&cfg.verbose, "v", false, "mostra uma mensagem a cada timeout de consumidor")
	flag.BoolVar(&cfg.csv, "csv", false, "imprime só uma linha CSV com o resumo")
	flag.Parse()

	if cfg.p < 1 || cfg.c < 1 || cfg.k < 1 || cfg.itens < 1 {
		fmt.Fprintln(os.Stderr, "erro: p, c, k e itens devem ser >= 1")
		os.Exit(2)
	}

	var buf Buffer
	switch cfg.versao {
	case "channel":
		buf = novoBufferChannel(cfg.k)
	case "semaforo":
		buf = novoBufferSemaforo(cfg.k)
	default:
		fmt.Fprintf(os.Stderr, "erro: versão desconhecida: %q (use channel ou semaforo)\n", cfg.versao)
		os.Exit(2)
	}

	produtores := make([]estProdutor, cfg.p)
	consumidores := make([]estConsumidor, cfg.c)
	var mon estMonitor

	pararMonitor := make(chan struct{})
	monitorParou := make(chan struct{})
	go monitor(buf, cfg.amostra, &mon, pararMonitor, monitorParou)

	var wgProd, wgCons sync.WaitGroup
	inicio := time.Now()
	for id := 0; id < cfg.c; id++ {
		wgCons.Add(1)
		go consumidor(id, cfg, buf, &consumidores[id], &wgCons)
	}
	for id := 0; id < cfg.p; id++ {
		wgProd.Add(1)
		go produtor(id, cfg, buf, &produtores[id], &wgProd)
	}

	// Encerramento ordenado:
	wgProd.Wait() // 1. todos os produtores terminam
	buf.Fechar()  // 2. avisa que não vem mais nada
	wgCons.Wait() // 3. consumidores drenam o que restou e saem
	duracao := time.Since(inicio)
	close(pararMonitor) // 4. para o monitor
	<-monitorParou

	erros := verificar(cfg, produtores, consumidores)
	if cfg.csv {
		imprimirCSV(cfg, produtores, consumidores, mon, duracao, len(erros) == 0)
	} else {
		imprimirRelatorio(cfg, produtores, consumidores, mon, duracao, erros)
	}
	if len(erros) > 0 {
		os.Exit(1)
	}
}

// produtor gera cfg.itens itens com identificadores únicos no programa todo.
func produtor(id int, cfg config, buf Buffer, est *estProdutor, wg *sync.WaitGroup) {
	defer wg.Done()
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

	for seq := 0; seq < cfg.itens; seq++ {
		dormir(rng, cfg.tprod) // produzindo
		item := id*cfg.itens + seq

		antes := time.Now()
		buf.Inserir(item)
		est.bloqueado += time.Since(antes)
		est.produzidos++
	}
}

// consumidor retira itens até o buffer ser encerrado. Se nada chega dentro do
// timeout, executa o comportamento alternativo (registra a ociosidade) e
// volta a tentar.
func consumidor(id int, cfg config, buf Buffer, est *estConsumidor, wg *sync.WaitGroup) {
	defer wg.Done()
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(1000+id)))

	antes := time.Now()
	for {
		item, r := buf.Retirar(cfg.timeout)
		switch r {
		case itemRecebido:
			est.esperando += time.Since(antes)
			est.recebidos = append(est.recebidos, item)
			dormir(rng, cfg.tcons) // consumindo
			antes = time.Now()
		case tempoEsgotado:
			est.timeouts++
			if cfg.verbose {
				fmt.Printf("consumidor %d: nenhum item em %v, segue ocioso (timeout nº %d)\n", id, cfg.timeout, est.timeouts)
			}
		case bufferEncerrado:
			return
		}
	}
}

// monitor amostra a ocupação do buffer em intervalos fixos até ser parado.
func monitor(buf Buffer, intervalo time.Duration, est *estMonitor, parar <-chan struct{}, parou chan<- struct{}) {
	defer close(parou)
	relogio := time.NewTicker(intervalo)
	defer relogio.Stop()

	for {
		select {
		case <-relogio.C:
			ocupacao := buf.Ocupacao()
			est.amostras++
			est.soma += ocupacao
			est.maximo = max(est.maximo, ocupacao)
		case <-parar:
			return
		}
	}
}

// verificar confere que nada foi perdido nem duplicado: o total consumido é
// igual ao produzido e cada identificador aparece exatamente uma vez.
func verificar(cfg config, produtores []estProdutor, consumidores []estConsumidor) []string {
	var erros []string

	produzidos := 0
	for _, p := range produtores {
		produzidos += p.produzidos
	}

	vezes := make([]int, cfg.p*cfg.itens)
	consumidos := 0
	for _, c := range consumidores {
		consumidos += len(c.recebidos)
		for _, item := range c.recebidos {
			if item < 0 || item >= len(vezes) {
				erros = append(erros, fmt.Sprintf("item inválido consumido: %d", item))
				continue
			}
			vezes[item]++
		}
	}

	if produzidos != consumidos {
		erros = append(erros, fmt.Sprintf("produzidos (%d) != consumidos (%d)", produzidos, consumidos))
	}
	perdidos, duplicados := 0, 0
	for _, v := range vezes {
		switch {
		case v == 0:
			perdidos++
		case v > 1:
			duplicados++
		}
	}
	if perdidos > 0 || duplicados > 0 {
		erros = append(erros, fmt.Sprintf("%d itens perdidos e %d duplicados", perdidos, duplicados))
	}
	return erros
}

func dormir(rng *rand.Rand, maximo time.Duration) {
	if maximo > 0 {
		time.Sleep(time.Duration(rng.Int63n(int64(maximo))))
	}
}

type resumo struct {
	total            int
	vazao            float64 // itens por segundo
	ocupacaoMedia    float64
	bloqueioProdutor float64 // µs por inserção
	esperaConsumidor float64 // µs por retirada
	timeouts         int
	consMin, consMax int
}

func resumir(produtores []estProdutor, consumidores []estConsumidor, mon estMonitor, duracao time.Duration) resumo {
	var r resumo
	var bloqueado, esperando time.Duration
	for _, p := range produtores {
		r.total += p.produzidos
		bloqueado += p.bloqueado
	}
	r.consMin = len(consumidores[0].recebidos)
	for _, c := range consumidores {
		esperando += c.esperando
		r.timeouts += c.timeouts
		r.consMin = min(r.consMin, len(c.recebidos))
		r.consMax = max(r.consMax, len(c.recebidos))
	}
	r.vazao = float64(r.total) / duracao.Seconds()
	if mon.amostras > 0 {
		r.ocupacaoMedia = float64(mon.soma) / float64(mon.amostras)
	}
	r.bloqueioProdutor = float64(bloqueado) / float64(time.Microsecond) / float64(r.total)
	r.esperaConsumidor = float64(esperando) / float64(time.Microsecond) / float64(r.total)
	return r
}

func imprimirRelatorio(cfg config, produtores []estProdutor, consumidores []estConsumidor, mon estMonitor, duracao time.Duration, erros []string) {
	r := resumir(produtores, consumidores, mon, duracao)

	fmt.Printf("\nVersão: %s | P=%d | C=%d | K=%d | %d itens por produtor\n", cfg.versao, cfg.p, cfg.c, cfg.k, cfg.itens)
	fmt.Println("consumidor  itens consumidos  timeouts")
	for id, c := range consumidores {
		fmt.Printf("%10d  %16d  %8d\n", id, len(c.recebidos), c.timeouts)
	}
	fmt.Printf("\ntempo total: %v | throughput: %.0f itens/s\n", duracao.Round(time.Microsecond), r.vazao)
	fmt.Printf("ocupação média do buffer: %.2f de %d (%.0f%%) | máxima: %d | %d amostras\n",
		r.ocupacaoMedia, cfg.k, 100*r.ocupacaoMedia/float64(cfg.k), mon.maximo, mon.amostras)
	fmt.Printf("tempo médio em Inserir: %.2f µs | tempo médio em Retirar: %.2f µs | timeouts: %d\n",
		r.bloqueioProdutor, r.esperaConsumidor, r.timeouts)

	if len(erros) == 0 {
		fmt.Printf("verificação: OK, %d produzidos = %d consumidos, nenhum item perdido ou duplicado\n", r.total, r.total)
	} else {
		for _, e := range erros {
			fmt.Println("verificação: FALHOU:", e)
		}
	}
	fmt.Printf("goroutines ativas ao final: %d\n", runtime.NumGoroutine())
}

func imprimirCSV(cfg config, produtores []estProdutor, consumidores []estConsumidor, mon estMonitor, duracao time.Duration, ok bool) {
	r := resumir(produtores, consumidores, mon, duracao)
	fmt.Printf("%s,%d,%d,%d,%d,%.2f,%.0f,%.2f,%d,%.2f,%.2f,%d,%d,%d,%t\n",
		cfg.versao, cfg.p, cfg.c, cfg.k, r.total,
		float64(duracao)/float64(time.Millisecond), r.vazao,
		r.ocupacaoMedia, mon.maximo, r.bloqueioProdutor, r.esperaConsumidor,
		r.timeouts, r.consMin, r.consMax, ok)
}
