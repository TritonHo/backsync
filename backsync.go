package backsync

import (
	"github.com/go-redis/redis"
)

type Manager struct {
	redisClient *redis.Client
	setKeyName  string
	listKeyName string
}

func New(r *redis.Client, setKeyName, listKeyName string) *Manager {
	return &Manager{
		redisClient: r,
		setKeyName:  setKeyName,
		listKeyName: listKeyName,
	}
}

func (m *Manager) Add(item string) error {
	script := `if redis.call('SADD', KEYS[1], KEYS[3]) == 1 then redis.call('RPUSH', KEYS[2], KEYS[3]) end`
	return m.redisClient.Eval(script, []string{m.setKeyName, m.listKeyName, item}).Err()
}

func (m *Manager) Top(n int) (item []string, err error) {
	result, err := m.redisClient.LRange(m.listKeyName, 0, int64(n-1)).Result()

	return result, err
}

func (m *Manager) Delete(items []string) error {
	script := `
for i = 3, #KEYS do
	if redis.call('LINDEX', KEYS[2], 0) == KEYS[i] then
		redis.call('LPOP', KEYS[2])
		redis.call('SREM', KEYS[1],  KEYS[i])
	end
end
return 0
`

	params := append([]string{m.setKeyName, m.listKeyName}, items...)
	return m.redisClient.Eval(script, params).Err()
}
