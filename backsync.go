package backsync

import (
	"sort"
	"strconv"
	"time"

	"github.com/go-redis/redis"
)

type Manager struct {
	redisClient *redis.Client
	//storing the items
	itemSetName string
	//storing the key-value pairs of items-->createTime
	updateTimeHashName string

	//storing the items under backsync to mainDb, the values is the start syncTime
	inuseTimeHashName string

	//if the backsync time over the timeLimit, then the item will be pickup by another worker
	timeLimit time.Duration
}

func New(r *redis.Client, itemSetName, updateTimeHashName, inuseTimeHashName string, timeLimit time.Duration) *Manager {
	return &Manager{
		redisClient:        r,
		itemSetName:        itemSetName,
		updateTimeHashName: updateTimeHashName,
		inuseTimeHashName:  inuseTimeHashName,

		timeLimit: timeLimit,
	}
}

func (m *Manager) Add(item string) error {
	ts := time.Now().Unix()
	script := `
redis.call('SADD', KEYS[1], KEYS[3])
redis.call('HSET', KEYS[2], KEYS[3], KEYS[4]) 
return 0`
	return m.redisClient.Eval(script, []string{m.itemSetName, m.updateTimeHashName, item, strconv.FormatInt(ts, 10)}).Err()
}

type Candidate struct {
	name string
	ts   int64
}

type CandidateByScore []Candidate

func (a CandidateByScore) Len() int           { return len(a) }
func (a CandidateByScore) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a CandidateByScore) Less(i, j int) bool { return a[i].ts < a[j].ts }

func toInt64(val interface{}) int64 {
	if s, ok1 := val.(string); ok1 {
		if temp, err := strconv.ParseInt(s, 10, 64); err == nil {
			return temp
		}
	}
	return 0
}

/*
	to probe for 2*n candidate, and return the atmost least-update n items
*/
func (m *Manager) Top(n int) (items []string, err error) {
	candidateNames, err0 := m.redisClient.SRandMemberN(m.itemSetName, int64(n*2)).Result()
	if err0 != nil {
		return nil, err0
	}

	if len(candidateNames) == 0 {
		//no records, thus no need to continue
		return []string{}, nil
	}

	inUseTimes, err1 := m.redisClient.HMGet(m.inuseTimeHashName, candidateNames...).Result()
	if err1 != nil {
		return nil, err1
	}

	//get back the candidates updateTime
	updateTimes, err2 := m.redisClient.HMGet(m.updateTimeHashName, candidateNames...).Result()
	if err1 != nil {
		return nil, err2
	}
	//sort the candidates, and remove the inuse candidates
	candidates := []Candidate{}
	limitTs := time.Now().Add(-1 * m.timeLimit).Unix()
	for i, _ := range candidateNames {
		if inuseTs := toInt64(inUseTimes[i]); inuseTs == 0 || inuseTs < limitTs {
			//only include the item not pickup already by some backsync worker
			candidate := Candidate{
				name: candidateNames[i],
				ts:   toInt64(updateTimes[i]),
			}
			candidates = append(candidates, candidate)
		}
	}
	sort.Sort(CandidateByScore(candidates))

	//remove the excess candidates
	result := []string{}
	for _, c := range candidates {
		result = append(result, c.name)
	}
	if len(result) > n {
		result = result[:n]
	}

	//update the inuseHash
	if len(result) > 0 {
		temp := map[string]interface{}{}
		nowTs := strconv.FormatInt(time.Now().Unix(), 10)
		for _, itemName := range result {
			temp[itemName] = nowTs
		}
		if err := m.redisClient.HMSet(m.inuseTimeHashName, temp).Err(); err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (m *Manager) Delete(items []string) error {
	script := `
for i = 4, #KEYS do
	local updateTs = redis.call('HGET', KEYS[2], KEYS[i])
	local inuseTs = redis.call('HGET', KEYS[3], KEYS[i])
	if (updateTs) and (inuseTs) and tonumber(inuseTs) >= tonumber(updateTs) then
		redis.call('HDEL', KEYS[2], KEYS[i])
		redis.call('SREM', KEYS[1], KEYS[i])
	end
	redis.call('HDEL', KEYS[3], KEYS[i])
end
return 0
`

	params := append([]string{m.itemSetName, m.updateTimeHashName, m.inuseTimeHashName}, items...)
	return m.redisClient.Eval(script, params).Err()
}
