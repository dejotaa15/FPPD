package main

import "time"

// semaforo é um semáforo de contagem construído sobre um channel: cada ficha
// no channel vale uma unidade do contador.
type semaforo chan struct{}

func novoSemaforo(maximo, inicial int) semaforo {
	s := make(semaforo, maximo)
	for i := 0; i < inicial; i++ {
		s <- struct{}{}
	}
	return s
}

// P (wait/down): decrementa; bloqueia se o contador for zero.
func (s semaforo) P() { <-s }

// V (signal/up): incrementa e acorda quem estiver bloqueado em P.
func (s semaforo) V() { s <- struct{}{} }

// bufferSemaforo é o buffer circular clássico:
//
//	naoCheio  conta as posições livres  (começa em K)
//	naoVazio  conta os itens disponíveis (começa em 0)
//	mutexEntrada / mutexSaida protegem os índices de escrita e de leitura
type bufferSemaforo struct {
	dados   []int
	entrada int // próxima posição de escrita
	saida   int // próxima posição de leitura

	naoCheio     semaforo
	naoVazio     semaforo
	mutexEntrada semaforo
	mutexSaida   semaforo

	encerrado chan struct{}
}

func novoBufferSemaforo(k int) *bufferSemaforo {
	return &bufferSemaforo{
		dados:        make([]int, k),
		naoCheio:     novoSemaforo(k, k),
		naoVazio:     novoSemaforo(k, 0),
		mutexEntrada: novoSemaforo(1, 1),
		mutexSaida:   novoSemaforo(1, 1),
		encerrado:    make(chan struct{}),
	}
}

func (b *bufferSemaforo) Inserir(item int) {
	b.naoCheio.P() // espera haver posição livre

	// Sem este mutex dois produtores poderiam ler o mesmo valor de entrada
	// e escrever na mesma posição, perdendo um item.
	b.mutexEntrada.P()
	b.dados[b.entrada] = item
	b.entrada = (b.entrada + 1) % len(b.dados)
	b.mutexEntrada.V()

	b.naoVazio.V() // avisa que há um item a mais
}

func (b *bufferSemaforo) Retirar(limite time.Duration) (int, resultado) {
	// Como o semáforo é um channel, o P em naoVazio pode participar de um
	// select junto com o timeout e com o aviso de encerramento.
	select {
	case <-b.naoVazio:
	case <-b.encerrado:
		// Os produtores já terminaram, mas ainda pode haver itens: tenta um
		// P sem bloquear. Se não houver ficha, o buffer está vazio de vez.
		select {
		case <-b.naoVazio:
		default:
			return 0, bufferEncerrado
		}
	case <-time.After(limite):
		return 0, tempoEsgotado
	}

	// Sem este mutex dois consumidores poderiam retirar o mesmo item.
	b.mutexSaida.P()
	item := b.dados[b.saida]
	b.saida = (b.saida + 1) % len(b.dados)
	b.mutexSaida.V()

	b.naoCheio.V() // avisa que há uma posição livre a mais
	return item, itemRecebido
}

func (b *bufferSemaforo) Fechar() { close(b.encerrado) }

// Ocupacao: posições em uso = K menos as posições livres contadas por
// naoCheio. Uma posição conta como ocupada desde que o produtor a reserva
// até o consumidor liberá-la.
func (b *bufferSemaforo) Ocupacao() int { return len(b.dados) - len(b.naoCheio) }
