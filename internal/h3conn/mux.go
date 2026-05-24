package h3conn

import "fmt"

type MultiplexedConn struct {
	sess *session
}

func NewMultiplexedConn(proxyAddr string) (*MultiplexedConn, error) {
	s, err := dialQUIC(proxyAddr)
	if err != nil {
		return nil, err
	}
	return &MultiplexedConn{sess: s}, nil
}

func (m *MultiplexedConn) Dial(targetAddr string) (*Conn, error) {
	if m.sess == nil {
		return nil, fmt.Errorf("multiplexed connection closed")
	}
	return m.sess.dialStream(targetAddr)
}

func (m *MultiplexedConn) Close() error {
	if m.sess == nil {
		return nil
	}
	err := m.sess.close()
	m.sess = nil
	return err
}
