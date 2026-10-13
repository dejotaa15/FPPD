package main

import "time"

// resultado diz o que aconteceu em uma tentativa de retirada.
type resultado int

const (
	itemRecebido    resultado = iota
	tempoEsgotado             // nenhum item chegou dentro do limite
	bufferEncerrado           // produtores terminaram e o buffer está vazio
)

// Buffer é o buffer limitado visto pelos produtores e consumidores.
// As duas versões (channel e semáforos) implementam a mesma interface, então
// o resto do programa é idêntico para ambas.
type Buffer interface {
	// Inserir bloqueia enquanto o buffer estiver cheio.
	Inserir(item int)
	// Retirar bloqueia enquanto o buffer estiver vazio, por no máximo limite.
	Retirar(limite time.Duration) (int, resultado)
	// Fechar avisa que não haverá mais inserções. Só pode ser chamado depois
	// que todos os produtores terminaram.
	Fechar()
	// Ocupacao devolve quantos itens estão no buffer neste instante.
	Ocupacao() int
}

// bufferChannel: o buffer é o próprio channel com capacidade K. O bloqueio
// de produtor (cheio) e de consumidor (vazio) é a semântica nativa do Go.
type bufferChannel struct {
	itens chan int
}

func novoBufferChannel(k int) *bufferChannel {
	return &bufferChannel{itens: make(chan int, k)}
}

func (b *bufferChannel) Inserir(item int) {
	b.itens <- item
}

func (b *bufferChannel) Retirar(limite time.Duration) (int, resultado) {
	select {
	case item, aberto := <-b.itens:
		if !aberto {
			// Channel fechado só devolve !aberto depois de entregar tudo o
			// que estava no buffer, então nenhum item é perdido.
			return 0, bufferEncerrado
		}
		return item, itemRecebido
	case <-time.After(limite):
		return 0, tempoEsgotado
	}
}

func (b *bufferChannel) Fechar() { close(b.itens) }

func (b *bufferChannel) Ocupacao() int { return len(b.itens) }
