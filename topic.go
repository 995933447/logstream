package logstream

func NewTopicSet() *TopicSet {
	return &TopicSet{
		set: map[string]struct{}{},
	}
}

type TopicSet struct {
	set map[string]struct{}
}

func (s *TopicSet) reset(topics []string) {
	newSet := map[string]struct{}{}
	for _, topic := range topics {
		newSet[topic] = struct{}{}
	}
	s.set = newSet
}

func (s *TopicSet) exist(topic string) bool {
	_, ok := s.set[topic]
	return ok
}

func (s *TopicSet) save(topic string) {
	_, ok := s.set[topic]
	if ok {
		return
	}
	s.set[topic] = struct{}{}
}

func (s *TopicSet) del(topic string) {
	delete(s.set, topic)
}

func (s *TopicSet) size() int {
	return len(s.set)
}

func (s *TopicSet) walk(fn func(topic string) (bool, error)) error {
	for topic := range s.set {
		keep, err := fn(topic)
		if err != nil {
			return err
		}

		if !keep {
			break
		}
	}
	return nil
}

func (s *TopicSet) list() []string {
	list := make([]string, s.size())
	var idx int
	for topic := range s.set {
		list[idx] = topic
		idx++
	}
	return list
}
