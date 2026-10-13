package main

import "fmt"

// estrategia define como um filósofo obtém e devolve seus dois garfos.
type estrategia interface {
	// adquirir bloqueia até o filósofo ter os dois garfos.
	// Devolve false se a simulação foi abortada durante a espera.
	adquirir(id int) bool
	liberar(id int)
}

// novaEstrategia devolve a estratégia escolhida e uma função que encerra
// eventuais goroutines auxiliares dela (só o garçom tem uma).
func novaEstrategia(nome string, m *mesa) (estrategia, func(), error) {
	nada := func() {}
	switch nome {
	case "base":
		return &base{m}, nada, nil
	case "hierarquia":
		return &hierarquia{m}, nada, nil
	case "limite":
		return &limite{mesa: m, lugares: make(chan struct{}, len(m.garfos)-1)}, nada, nil
	case "garcom":
		g := novoGarcom(m)
		return g, g.parar, nil
	}
	return nil, nil, fmt.Errorf("estratégia desconhecida: %q (use base, hierarquia, limite ou garcom)", nome)
}

// base: todos pegam primeiro o garfo da esquerda e depois o da direita.
// Se os N filósofos pegarem o da esquerda ao mesmo tempo, forma-se um ciclo
// de espera e ninguém mais avança (deadlock).
type base struct{ *mesa }

func (b *base) adquirir(id int) bool {
	return b.pegarDois(id, b.esquerda(id), b.direita(id))
}

func (b *base) liberar(id int) { b.largarDois(id) }

// hierarquia: os garfos têm uma ordem global (o índice) e todo filósofo pega
// primeiro o de menor índice. Só o último filósofo muda de comportamento
// (pega o garfo 0 antes do N-1), o que quebra a simetria e impede o ciclo.
type hierarquia struct{ *mesa }

func (h *hierarquia) adquirir(id int) bool {
	e, d := h.esquerda(id), h.direita(id)
	return h.pegarDois(id, min(e, d), max(e, d))
}

func (h *hierarquia) liberar(id int) { h.largarDois(id) }

// limite: no máximo N-1 filósofos disputam garfos ao mesmo tempo. O channel
// lugares funciona como um semáforo de contagem com N-1 vagas. Com N garfos
// para N-1 filósofos, pelo menos um deles sempre consegue os dois.
type limite struct {
	*mesa
	lugares chan struct{}
}

func (l *limite) adquirir(id int) bool {
	select {
	case l.lugares <- struct{}{}:
	case <-l.abortar:
		return false
	}
	if !l.pegarDois(id, l.esquerda(id), l.direita(id)) {
		<-l.lugares
		return false
	}
	return true
}

func (l *limite) liberar(id int) {
	l.largarDois(id)
	<-l.lugares
}
