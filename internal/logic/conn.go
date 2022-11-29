package logic

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Terry-Mao/goim/api/protocol"
	"github.com/Terry-Mao/goim/internal/logic/model"
	log "github.com/golang/glog"
	"github.com/google/uuid"
)

// Connect connected a conn.
func (l *Logic) Connect(c context.Context, server, cookie string, token []byte) (mid int64, key, roomID string, accepts []int32, hb int64, err error) {
	var params struct {
		Mid      int64   `json:"mid"`      // 用户在业务中的ID (int64)
		Key      string  `json:"key"`      // 客户端标识别,如果为空则自动生成UUID
		RoomID   string  `json:"room_id"`  // 客户端加入房间
		Platform string  `json:"platform"` // 客户端所在平台
		Accepts  []int32 `json:"accepts"`  // 监听房间
	}
	if err = json.Unmarshal(token, &params); err != nil {
		log.Errorf("json.Unmarshal(%s) error(%v)", token, err)
		return
	}
	mid = params.Mid
	roomID = params.RoomID
	accepts = params.Accepts
	hb = int64(l.c.Node.Heartbeat) * int64(l.c.Node.HeartbeatMax)
	if key = params.Key; key == "" {
		key = uuid.New().String()
	}
	// 信息存放在redis里面，方便后面查找
	if err = l.dao.AddMapping(c, mid, key, server); err != nil {
		log.Errorf("l.dao.AddMapping(%d,%s,%s) error(%v)", mid, key, server, err)
		fmt.Printf("l.dao.AddMapping(%d,%s,%s) error(%v)", mid, key, server, err)
	}
	log.Infof("conn connected key:%s server:%s mid:%d token:%s", key, server, mid, token)
	fmt.Printf("conn connected key:%s server:%s mid:%d token:%s", key, server, mid, token)
	return
}

// Disconnect disconnect a conn.
func (l *Logic) Disconnect(c context.Context, mid int64, key, server string) (has bool, err error) {
	if has, err = l.dao.DelMapping(c, mid, key, server); err != nil {
		log.Errorf("l.dao.DelMapping(%d,%s) error(%v)", mid, key, server)
		return
	}
	log.Infof("conn disconnected key:%s server:%s mid:%d", key, server, mid)
	return
}

// Heartbeat heartbeat a conn.
func (l *Logic) Heartbeat(c context.Context, mid int64, key, server string) (err error) {
	has, err := l.dao.ExpireMapping(c, mid, key)
	if err != nil {
		log.Errorf("l.dao.ExpireMapping(%d,%s,%s) error(%v)", mid, key, server, err)
		return
	}
	if !has {
		if err = l.dao.AddMapping(c, mid, key, server); err != nil {
			log.Errorf("l.dao.AddMapping(%d,%s,%s) error(%v)", mid, key, server, err)
			return
		}
	}
	log.Infof("conn heartbeat key:%s server:%s mid:%d", key, server, mid)
	return
}

// RenewOnline renew a server online.
func (l *Logic) RenewOnline(c context.Context, server string, roomCount map[string]int32) (map[string]int32, error) {
	online := &model.Online{
		Server:    server,
		RoomCount: roomCount,
		Updated:   time.Now().Unix(),
	}
	if err := l.dao.AddServerOnline(context.Background(), server, online); err != nil {
		return nil, err
	}
	return l.roomCount, nil
}

// Receive receive a message.
func (l *Logic) Receive(c context.Context, mid int64, proto *protocol.Proto) (err error) {
	log.Infof("receive mid:%d message:%+v", mid, proto)
	return
}
