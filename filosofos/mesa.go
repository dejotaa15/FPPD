package main

import (
	"sync/atomic"
	"time"
)

// nenhum indica "nenhum garfo" nos vetores segura/espera da mesa.
const nenhum = -1

// garfo é um channel de capacidade 1 que guarda uma única ficha.
// Ficha no channel = garfo sobre a mesa. Pegar o garfo é receber a ficha
// (bloqueia se outro filósofo estiver com ela) e largar é devolvê-la.
type garfo chan struct{}

func novoGarfo() garfo {
	g := make(garfo, 1)
	g <- struct{}{}
	return g
}

// pegar bloqueia até o garfo estar livre. Devolve false se a simulação foi
// abortada (deadlock detectado pelo vigia) enquanto o filósofo esperava.
func (g garfo) pegar(abortar <-chan struct{}) bool {
	select {
	case <-g:
		return true
	case <-abortar:
		return false
	}
}

func (g garfo) largar() {
	g <- struct{}{}
}

// mesa reúne o estado compartilhado por todos os filósofos.
type mesa struct {
	garfos  []garfo
	pausa   time.Duration // intervalo entre pegar o primeiro e o segundo garfo
	abortar chan struct{} // fechado pelo vigia quando detecta deadlock

	total     atomic.Int64 // refeições concluídas (lido pelo vigia)
	restantes atomic.Int64 // refeições ainda disponíveis no modo -disputa

	// Qual garfo cada filósofo segura / espera. Usado só para o diagnóstico
	// de deadlock, por isso é atômico: o vigia lê enquanto os filósofos escrevem.
	segura []atomic.Int32
	espera []atomic.Int32
}

func novaMesa(n int, pausa time.Duration) *mesa {
	m := &mesa{
		garfos:  make([]garfo, n),
		pausa:   pausa,
		abortar: make(chan struct{}),
		segura:  make([]atomic.Int32, n),
		espera:  make([]atomic.Int32, n),
	}
	for i := range m.garfos {
		m.garfos[i] = novoGarfo()
		m.segura[i].Store(nenhum)
		m.espera[i].Store(nenhum)
	}
	return m
}

func (m *mesa) esquerda(id int) int { return id }
func (m *mesa) direita(id int) int  { return (id + 1) % len(m.garfos) }

// pegarDois pega os garfos na ordem primeiro -> segundo. A pausa entre os dois
// alarga a janela em que o filósofo segura um garfo e espera o outro, o que
// torna o deadlock da versão base reprodutível. Ela vale para todas as
// estratégias, para que a comparação seja feita nas mesmas condições.
func (m *mesa) pegarDois(id, primeiro, segundo int) bool {
	m.espera[id].Store(int32(primeiro))
	if !m.garfos[primeiro].pegar(m.abortar) {
		return false
	}
	m.segura[id].Store(int32(primeiro))
	m.espera[id].Store(int32(segundo))

	time.Sleep(m.pausa)

	if !m.garfos[segundo].pegar(m.abortar) {
		m.garfos[primeiro].largar()
		m.segura[id].Store(nenhum)
		return false
	}
	m.espera[id].Store(nenhum)
	return true
}

func (m *mesa) largarDois(id int) {
	m.garfos[m.esquerda(id)].largar()
	m.garfos[m.direita(id)].largar()
	m.segura[id].Store(nenhum)
}
