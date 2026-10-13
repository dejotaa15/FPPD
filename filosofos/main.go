// Jantar dos Filósofos (Dijkstra) com garfos representados por channels.
//
// Uso:
//
//	go run ./filosofos -n 5 -r 100 -estrategia hierarquia
//
// Estratégias: base (entra em deadlock), hierarquia, limite, garcom.
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
	n, r       int
	estrategia string
	disputa    bool
	pensar     time.Duration
	comer      time.Duration
	pausa      time.Duration
	vigia      time.Duration
	csv        bool
}

// estatistica guarda os dados de um filósofo. Cada filósofo escreve apenas na
// sua e o main só lê depois que todos terminaram, então não há disputa.
type estatistica struct {
	refeicoes   int
	esperaTotal time.Duration
	esperaMax   time.Duration
}

func main() {
	var cfg config
	flag.IntVar(&cfg.n, "n", 5, "número de filósofos (N)")
	flag.IntVar(&cfg.r, "r", 100, "número de iterações (R)")
	flag.StringVar(&cfg.estrategia, "estrategia", "base", "base | hierarquia | limite | garcom")
	flag.BoolVar(&cfg.disputa, "disputa", false, "as N*R refeições são disputadas entre todos, em vez de R para cada um")
	flag.DurationVar(&cfg.pensar, "pensar", time.Millisecond, "tempo máximo pensando (sorteado entre 0 e o valor)")
	flag.DurationVar(&cfg.comer, "comer", time.Millisecond, "tempo máximo comendo (sorteado entre 0 e o valor)")
	flag.DurationVar(&cfg.pausa, "pausa", time.Millisecond, "pausa entre pegar o primeiro e o segundo garfo")
	flag.DurationVar(&cfg.vigia, "vigia", 2*time.Second, "tempo sem nenhuma refeição para declarar deadlock")
	flag.BoolVar(&cfg.csv, "csv", false, "imprime só uma linha CSV com o resumo")
	flag.Parse()

	if cfg.n < 2 || cfg.r < 1 {
		fmt.Fprintln(os.Stderr, "erro: é preciso n >= 2 e r >= 1")
		os.Exit(2)
	}

	m := novaMesa(cfg.n, cfg.pausa)
	m.restantes.Store(int64(cfg.n) * int64(cfg.r))

	estr, pararEstrategia, err := novaEstrategia(cfg.estrategia, m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "erro:", err)
		os.Exit(2)
	}

	stats := make([]estatistica, cfg.n)
	var wg sync.WaitGroup
	inicio := time.Now()
	for id := 0; id < cfg.n; id++ {
		wg.Add(1)
		go filosofo(id, cfg, m, estr, &stats[id], &wg)
	}

	deadlock := aguardar(&wg, m, cfg)
	duracao := time.Since(inicio)
	pararEstrategia()

	if cfg.csv {
		imprimirCSV(cfg, stats, duracao, deadlock)
	} else {
		imprimirRelatorio(cfg, stats, duracao, deadlock)
	}
}

// filosofo alterna entre pensar e comer até cumprir suas iterações.
func filosofo(id int, cfg config, m *mesa, estr estrategia, est *estatistica, wg *sync.WaitGroup) {
	defer wg.Done()
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

	for {
		if cfg.disputa {
			// Reserva uma das N*R refeições; quando acabam, vai embora.
			if m.restantes.Add(-1) < 0 {
				return
			}
		} else if est.refeicoes == cfg.r {
			return
		}

		dormir(rng, cfg.pensar) // pensando

		comFome := time.Now()
		if !estr.adquirir(id) {
			return // simulação abortada: deadlock
		}
		espera := time.Since(comFome)
		est.esperaTotal += espera
		if espera > est.esperaMax {
			est.esperaMax = espera
		}

		dormir(rng, cfg.comer) // comendo
		est.refeicoes++
		m.total.Add(1)

		estr.liberar(id)
	}
}

// aguardar espera todos os filósofos terminarem e, ao mesmo tempo, vigia o
// progresso: se nenhuma refeição acontecer durante um intervalo inteiro do
// vigia, declara deadlock, mostra o ciclo de espera e aborta a simulação para
// que as goroutines bloqueadas sejam liberadas. Devolve true se houve deadlock.
func aguardar(wg *sync.WaitGroup, m *mesa, cfg config) bool {
	terminou := make(chan struct{})
	go func() {
		wg.Wait()
		close(terminou)
	}()

	relogio := time.NewTicker(cfg.vigia)
	defer relogio.Stop()

	ultimo := int64(-1)
	for {
		select {
		case <-terminou:
			return false
		case <-relogio.C:
			atual := m.total.Load()
			if atual != ultimo {
				ultimo = atual
				continue
			}
			if !cfg.csv {
				imprimirDeadlock(m, cfg)
			}
			close(m.abortar)
			<-terminou
			return true
		}
	}
}

func imprimirDeadlock(m *mesa, cfg config) {
	fmt.Printf("\n*** DEADLOCK: nenhuma refeição nos últimos %v ***\n", cfg.vigia)
	for id := range m.segura {
		fmt.Printf("  filósofo %d: segura %s, espera %s\n",
			id, nomeGarfo(m.segura[id].Load()), nomeGarfo(m.espera[id].Load()))
	}
	fmt.Println("  abortando: os filósofos devolvem os garfos e encerram")
}

func nomeGarfo(g int32) string {
	if g == nenhum {
		return "nenhum garfo"
	}
	return fmt.Sprintf("o garfo %d", g)
}

func dormir(rng *rand.Rand, maximo time.Duration) {
	if maximo > 0 {
		time.Sleep(time.Duration(rng.Int63n(int64(maximo))))
	}
}

// resumo agrega as estatísticas individuais.
type resumo struct {
	refTotal, refMin, refMax int
	jain                     float64       // índice de fairness de Jain sobre as refeições
	esperaMedia              time.Duration // média global (por refeição)
	menorMedia, maiorMedia   time.Duration // menor e maior média individual
	esperaMax                time.Duration
}

func resumir(stats []estatistica) resumo {
	r := resumo{refMin: stats[0].refeicoes, menorMedia: -1}
	var esperaTotal time.Duration
	var soma, somaQuadrados float64
	for _, s := range stats {
		r.refTotal += s.refeicoes
		r.refMin = min(r.refMin, s.refeicoes)
		r.refMax = max(r.refMax, s.refeicoes)
		r.esperaMax = max(r.esperaMax, s.esperaMax)
		esperaTotal += s.esperaTotal
		soma += float64(s.refeicoes)
		somaQuadrados += float64(s.refeicoes) * float64(s.refeicoes)

		media := mediaDe(s)
		if r.menorMedia < 0 || media < r.menorMedia {
			r.menorMedia = media
		}
		r.maiorMedia = max(r.maiorMedia, media)
	}
	if r.refTotal > 0 {
		r.esperaMedia = esperaTotal / time.Duration(r.refTotal)
		r.jain = soma * soma / (float64(len(stats)) * somaQuadrados)
	}
	return r
}

func mediaDe(s estatistica) time.Duration {
	if s.refeicoes == 0 {
		return 0
	}
	return s.esperaTotal / time.Duration(s.refeicoes)
}

func micros(d time.Duration) float64 { return float64(d) / float64(time.Microsecond) }

func imprimirRelatorio(cfg config, stats []estatistica, duracao time.Duration, deadlock bool) {
	modo := fmt.Sprintf("%d refeições por filósofo", cfg.r)
	if cfg.disputa {
		modo = fmt.Sprintf("%d refeições em disputa", cfg.n*cfg.r)
	}
	fmt.Printf("\nEstratégia: %s | N=%d | R=%d (%s)\n", cfg.estrategia, cfg.n, cfg.r, modo)
	fmt.Println("filósofo  refeições  espera média (µs)  espera máx (µs)")
	for id, s := range stats {
		fmt.Printf("%8d  %9d  %17.1f  %15.1f\n", id, s.refeicoes, micros(mediaDe(s)), micros(s.esperaMax))
	}

	r := resumir(stats)
	fmt.Printf("\ntotal de refeições: %d | tempo total: %v\n", r.refTotal, duracao.Round(time.Millisecond))
	fmt.Printf("refeições (mín/máx): %d/%d | índice de Jain: %.4f\n", r.refMin, r.refMax, r.jain)
	fmt.Printf("espera média global: %.1f µs | médias individuais (mín/máx): %.1f/%.1f µs | pior espera: %.1f µs\n",
		micros(r.esperaMedia), micros(r.menorMedia), micros(r.maiorMedia), micros(r.esperaMax))
	if deadlock {
		fmt.Println("resultado: DEADLOCK detectado; simulação abortada e encerrada de forma ordenada")
	} else {
		fmt.Println("resultado: todas as iterações concluídas sem deadlock")
	}
	fmt.Printf("goroutines ativas ao final: %d\n", runtime.NumGoroutine())
}

func imprimirCSV(cfg config, stats []estatistica, duracao time.Duration, deadlock bool) {
	r := resumir(stats)
	fmt.Printf("%s,%d,%d,%t,%t,%.1f,%d,%d,%.4f,%.1f,%.1f,%.1f,%.1f\n",
		cfg.estrategia, cfg.n, cfg.r, cfg.disputa, deadlock,
		float64(duracao)/float64(time.Millisecond),
		r.refMin, r.refMax, r.jain,
		micros(r.esperaMedia), micros(r.menorMedia), micros(r.maiorMedia), micros(r.esperaMax))
}
