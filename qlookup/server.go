package qlookup

import (
	"sync"
)

type LookupKey struct {
	Category string
	Key      string
	Subkey   string
}

func (k LookupKey) IsMatch(category, key, subkey string) bool {
	if k.Category != category {
		return false
	}

	if key != "*" && key != k.Key {
		return false
	}

	if subkey != "*" && subkey != k.Subkey {
		return false
	}

	return true
}

type PeerInfo struct {
	lastUpdate       int64
	id               string
	RemoreAddress    string
	Hostname         string
	BroadcastAddress string
	TcpPort          int32
	HttpPort         int32
	Version          string
}

type Producer struct {
	peerInfo *PeerInfo
}

type LookupServer struct {
	mutex sync.Mutex

	LookupMap map[LookupKey]map[string]*Producer
}

func NewLookupServer() *LookupServer {
	return &LookupServer{
		LookupMap: make(map[LookupKey]map[string]*Producer),
	}
}

func NeedFilter(key, subkey string) bool {
	return key == "*" || subkey == "*"
}

func (s *LookupServer) AddLookup(key LookupKey) {
	s.mutex.Lock()

	_, ok := s.LookupMap[key]
	if !ok {
		s.LookupMap[key] = make(map[string]*Producer)
	}

	s.mutex.Unlock()
}

func (s *LookupServer) AddProducer(key LookupKey, p *Producer) {
	s.mutex.Lock()

	_, ok := s.LookupMap[key]
	if !ok {
		s.LookupMap[key] = make(map[string]*Producer)
	}

	ps := s.LookupMap[key]
	_, found := ps[p.peerInfo.id]
	if !found {
		ps[p.peerInfo.id] = p
	}

	s.mutex.Unlock()
}

func (s *LookupServer) RemoveProducer(key LookupKey, id string) {
	s.mutex.Lock()

	for {
		ps, ok := s.LookupMap[key]
		if !ok {
			break
		}

		delete(ps, id)
		break
	}

	s.mutex.Unlock()
}

func (s *LookupServer) RemoveLookup(key LookupKey) {
	s.mutex.Lock()

	delete(s.LookupMap, key)

	s.mutex.Unlock()
}

func (s *LookupServer) FindLookupKey(category, key, subkey string) []LookupKey {
	ret := make([]LookupKey, 0)

	s.mutex.Lock()

	if !NeedFilter(key, subkey) {
		k := LookupKey{category, key, subkey}
		if _, ok := s.LookupMap[k]; ok {
			ret = append(ret, k)
		}
	} else {
		for k := range s.LookupMap {
			if !k.IsMatch(category, key, subkey) {
				continue
			}

			ret = append(ret, k)
		}
	}

	s.mutex.Unlock()

	return ret
}

func (s *LookupServer) FindProduces(category, key, subkey string) []*Producer {
	ret := make([]*Producer, 0)

	s.mutex.Lock()

	if !NeedFilter(key, subkey) {
		k := LookupKey{category, key, subkey}
		ps, _ := s.LookupMap[k]
		for _, p := range ps {
			ret = append(ret, p)
		}
	} else {
		set := make(map[string]struct{})
		for k, ps := range s.LookupMap {
			if !k.IsMatch(category, key, subkey) {
				continue
			}

			for _, p := range ps {
				_, ok := set[p.peerInfo.id]
				if ok {
					continue
				}

				set[p.peerInfo.id] = struct{}{}
				ret = append(ret, p)
			}
		}
	}

	s.mutex.Unlock()

	return ret
}

func (s *LookupServer) keys(id string) []LookupKey {
	ret := make([]LookupKey, 0)

	s.mutex.Lock()

	for k, ps := range s.LookupMap {
		if _, ok := ps[id]; ok {
			ret = append(ret, k)
		}
	}

	s.mutex.Unlock()

	return ret
}

func FilterLookup(keys []LookupKey, category, key, subkey string) []LookupKey {
	ret := make([]LookupKey, 0)

	for _, k := range keys {
		if k.IsMatch(category, key, subkey) {
			ret = append(ret, k)
		}
	}

	return ret
}

func LookupKeys(keys []LookupKey) []string {
	ret := make([]string, len(keys))
	for i, k := range keys {
		ret[i] = k.Key
	}

	return ret
}

func LookupSubkeys(keys []LookupKey) []string {
	ret := make([]string, len(keys))

	for i, k := range keys {
		ret[i] = k.Subkey
	}

	return ret
}

func ProducerPeerInfo(p []*Producer) []*PeerInfo {
	ret := make([]*PeerInfo, len(p))
	for i, k := range p {
		ret[i] = k.peerInfo
	}

	return ret
}
