package h2conn

import "fmt"

type MultiplexedConn struct {
	sess *session
}

func NewMultiplexedConn(proxyAddr string) (*MultiplexedConn, error) {
	s, err := newSession(proxyAddr)
	if err != nil {
		return nil, err
	}
	return &MultiplexedConn{sess: s}, nil
}

func (m *MultiplexedConn) Dial(targetAddr string) (*Conn, error) {
	if m.sess == nil {
		return nil, fmt.Errorf("multiplexed connection closed")
	}
	st, err := m.sess.dialStream(targetAddr)
	if err != nil {
		return nil, err
	}
	return &Conn{st: st, ownsSession: false}, nil
}

func (m *MultiplexedConn) Close() error {
	if m.sess == nil {
		return nil
	}
	err := m.sess.close()
	m.sess = nil
	return err
}
