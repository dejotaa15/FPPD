package main

// pedido é a mensagem que um filósofo com fome envia ao garçom.
// O garçom autoriza fechando o channel ok.
type pedido struct {
	id int
	ok chan struct{}
}

// garcom: arbitragem centralizada. Um único processo (goroutine) decide quem
// pode pegar garfos. Ele só autoriza um filósofo quando os DOIS garfos dele
// estão livres, então ninguém fica segurando um garfo enquanto espera o outro.
type garcom struct {
	*mesa
	pedidos    chan pedido
	devolucoes chan int
	fim        chan struct{}
	encerrado  chan struct{}
}

func novoGarcom(m *mesa) *garcom {
	g := &garcom{
		mesa:       m,
		pedidos:    make(chan pedido),
		devolucoes: make(chan int),
		fim:        make(chan struct{}),
		encerrado:  make(chan struct{}),
	}
	go g.atender()
	return g
}

// atender é o laço do processo garçom. Todo o estado (livre, fila) pertence
// só a esta goroutine: os filósofos falam com ela exclusivamente por channels,
// então não é preciso mutex.
func (g *garcom) atender() {
	defer close(g.encerrado)

	livre := make([]bool, len(g.garfos))
	for i := range livre {
		livre[i] = true
	}
	var fila []pedido // pedidos ainda não atendidos, em ordem de chegada

	for {
		select {
		case p := <-g.pedidos:
			fila = append(fila, p)
		case id := <-g.devolucoes:
			livre[g.esquerda(id)], livre[g.direita(id)] = true, true
		case <-g.fim:
			return
		}

		// Percorre a fila em ordem de chegada e autoriza quem tem os dois
		// garfos livres. O pedido mais antigo que não puder ser atendido
		// RESERVA seus garfos: ninguém mais novo pode usá-los. É isso que
		// impede dois vizinhos de se revezarem e deixarem o do meio em
		// starvation. Só o mais antigo reserva, para não bloquear em cadeia
		// filósofos que poderiam comer em paralelo.
		reservaE, reservaD := nenhum, nenhum
		pendentes := fila[:0]
		for _, p := range fila {
			e, d := g.esquerda(p.id), g.direita(p.id)
			reservado := e == reservaE || e == reservaD || d == reservaE || d == reservaD
			if livre[e] && livre[d] && !reservado {
				livre[e], livre[d] = false, false
				close(p.ok)
				continue
			}
			if reservaE == nenhum {
				reservaE, reservaD = e, d
			}
			pendentes = append(pendentes, p)
		}
		fila = pendentes
	}
}

func (g *garcom) adquirir(id int) bool {
	p := pedido{id: id, ok: make(chan struct{})}
	select {
	case g.pedidos <- p:
	case <-g.abortar:
		return false
	}
	select {
	case <-p.ok:
	case <-g.abortar:
		return false
	}
	// Com a autorização, os dois garfos estão garantidamente sobre a mesa.
	return g.pegarDois(id, g.esquerda(id), g.direita(id))
}

// liberar devolve primeiro os garfos e só depois avisa o garçom, para que o
// próximo autorizado já os encontre nos channels.
func (g *garcom) liberar(id int) {
	g.largarDois(id)
	g.devolucoes <- id
}

func (g *garcom) parar() {
	close(g.fim)
	<-g.encerrado
}
