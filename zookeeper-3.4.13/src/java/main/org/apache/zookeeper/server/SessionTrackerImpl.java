/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.common.Time;

/**
 * This is a full featured SessionTracker. It tracks session in grouped by tick
 * interval. It always rounds up the tick interval to provide a sort of grace
 * period. Sessions are thus expired in batches made up of sessions that expire
 * in a given interval.
 */
public class SessionTrackerImpl extends ZooKeeperCriticalThread implements SessionTracker {
    private static final Logger LOG = LoggerFactory.getLogger(SessionTrackerImpl.class);

    HashMap<Long, SessionImpl> sessionsById = new HashMap<Long, SessionImpl>();

    HashMap<Long, SessionSet> sessionSets = new HashMap<Long, SessionSet>();

    ConcurrentHashMap<Long, Integer> sessionsWithTimeout;
    long nextSessionId = 0;
    long nextExpirationTime;

    int expirationInterval;

    public static class SessionImpl implements Session {
        SessionImpl(long sessionId, int timeout, long expireTime) {
            this.sessionId = sessionId;
            this.timeout = timeout;
            // 该属性记录的是当前session所在的会话桶id
            // 该值为0，表示当前session不在任何桶中
            this.tickTime = expireTime;
            isClosing = false;
        }

        final long sessionId;
        final int timeout;
        long tickTime;
        boolean isClosing;

        Object owner;

        public long getSessionId() { return sessionId; }
        public int getTimeout() { return timeout; }
        public boolean isClosing() { return isClosing; }
    }

    public static long initializeNextSession(long id) {
        long nextSid = 0;
        nextSid = (Time.currentElapsedTime() << 24) >>> 8;
        nextSid =  nextSid | (id <<56);
        return nextSid;
    }

    static class SessionSet {
        HashSet<SessionImpl> sessions = new HashSet<SessionImpl>();
    }

    SessionExpirer expirer;

    /**
     * 计算指定时间所在会话桶，返回值为桶id，即会话桶的最大时间边界值
     */
    private long roundToInterval(long time) {
        // We give a one interval grace period
        // 这里主要使用的就是 整型除以整型其结果仍为整型 的特性
        return (time / expirationInterval + 1) * expirationInterval;
    }

    public SessionTrackerImpl(SessionExpirer expirer,
            ConcurrentHashMap<Long, Integer> sessionsWithTimeout, int tickTime,
            long sid, ZooKeeperServerListener listener)
    {
        super("SessionTracker", listener);
        this.expirer = expirer;
        // 初始化过期间隔，即会话桶大小
        this.expirationInterval = tickTime;
        this.sessionsWithTimeout = sessionsWithTimeout;
        // 计算当前时间所在的会话桶
        nextExpirationTime = roundToInterval(Time.currentElapsedTime());
        this.nextSessionId = initializeNextSession(sid);
        // sessionsWithTimeout这个map的key为sessionId，value为timeout
        // 遍历来自于磁盘的session
        for (Entry<Long, Integer> e : sessionsWithTimeout.entrySet()) {
            // 该方法完成三项任务：
            // 1、创建一个session
            // 2、将session放入一个缓存map
            // 3、将这个session放入到相应的会话桶
            addSession(e.getKey(), e.getValue());
        }
    }

    volatile boolean running = true;

    volatile long currentTime;

    synchronized public void dumpSessions(PrintWriter pwriter) {
        pwriter.print("Session Sets (");
        pwriter.print(sessionSets.size());
        pwriter.println("):");
        ArrayList<Long> keys = new ArrayList<Long>(sessionSets.keySet());
        Collections.sort(keys);
        for (long time : keys) {
            pwriter.print(sessionSets.get(time).sessions.size());
            pwriter.print(" expire at ");
            pwriter.print(new Date(time));
            pwriter.println(":");
            for (SessionImpl s : sessionSets.get(time).sessions) {
                pwriter.print("\t0x");
                pwriter.println(Long.toHexString(s.sessionId));
            }
        }
    }

    @Override
    synchronized public String toString() {
        StringWriter sw = new StringWriter();
        PrintWriter pwriter = new PrintWriter(sw);
        dumpSessions(pwriter);
        pwriter.flush();
        pwriter.close();
        return sw.toString();
    }

    @Override
    synchronized public void run() {
        try {
            // 只要当前server处于运行状态，则循环不会结束
            while (running) {
                // 获取当前时间
                currentTime = Time.currentElapsedTime();
                // nextExpirationTime 其在当前sessionTracker创建时被初始化为了创建时间点所在的会话桶
                // 若nextExpirationTime大于当前时间，则说明nextExpirationTime这个桶尚未过期
                // 需要做的就是等待，等待者过期时间
                if (nextExpirationTime > currentTime) {
                    this.wait(nextExpirationTime - currentTime);
                    continue;
                }
                // 代码等到这里，说明nextExpirationTime <= currentTime，即nextExpirationTime桶过期了
                // 过期了，就需要将这个过期桶从会话桶中清除，并将过期桶中的会话干掉
                SessionSet set;
                // 从会话桶集合中清除掉nextExpirationTime桶
                set = sessionSets.remove(nextExpirationTime);
                if (set != null) {
                    // 将过期通中的会话干掉
                    for (SessionImpl s : set.sessions) {
                        // 修改关闭状态
                        setSessionClosing(s.sessionId);
                        // 关闭会话
                        expirer.expire(s);
                    }
                }
                // 计算出下一个要处理的桶
                nextExpirationTime += expirationInterval;
            }
        } catch (InterruptedException e) {
            handleException(this.getName(), e);
        }
        LOG.info("SessionTrackerImpl exited loop!");
    }

    /**
     * 当客户端与当前server进行一次交互时，就会触发该方法的执行
     * 其作用：
     * 1、换桶
     * 2、判断指定session是否被关闭或不存在
     */
    synchronized public boolean touchSession(long sessionId, int timeout) {
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG,
                                     ZooTrace.CLIENT_PING_TRACE_MASK,
                                     "SessionTrackerImpl --- Touch session: 0x"
                    + Long.toHexString(sessionId) + " with timeout " + timeout);
        }
        // 获取当前session
        SessionImpl s = sessionsById.get(sessionId);
        // Return false, if the session doesn't exists or marked as closing
        // 若当前session不存在，或被关闭了，则直接返回false
        if (s == null || s.isClosing()) {
            return false;
        }
        // 算桶：计算当前会话在本次交互后的空闲超时时间所在的会话桶
        long expireTime = roundToInterval(Time.currentElapsedTime() + timeout);
        /*
            tickTime 记录着当前会话所在的会话桶，expireTime  超时时间所在的会话桶
            它们的关系可能是：
            s.tickTime < expireTime：说明当前会话需要换桶
            s.tickTime == expireTime：说明超时时间所在桶与当前会话所在桶相同，不需要换桶
            s.tickTime > expireTime：不存在该可能性，因为timeout不可能为负数
         */
        if (s.tickTime >= expireTime) {
            // Nothing needs to be done
            return true;
        }
        // 代码能走到这里，说明s.tickTime < expireTime成立，即需要换桶
        // sessionSets：会话桶集合，其为一个map；key为会话桶id，即会话桶的最大时间边界值，value为会话桶
        SessionSet set = sessionSets.get(s.tickTime);
        if (set != null) {
            // 获取当前会话所在的桶，并从桶中删除
            set.sessions.remove(s);
        }
        // 更新当前会话所在桶，根据新会话桶id获取新的会话桶
        s.tickTime = expireTime;
        set = sessionSets.get(s.tickTime);
        // 若新会话桶不存在，则创建一个，再将新桶放入到会话桶集合
        if (set == null) {
            set = new SessionSet();
            sessionSets.put(expireTime, set);
        }
        // 将会话放入新桶
        set.sessions.add(s);
        return true;
    }

    synchronized public void setSessionClosing(long sessionId) {
        if (LOG.isTraceEnabled()) {
            LOG.info("Session closing: 0x" + Long.toHexString(sessionId));
        }
        SessionImpl s = sessionsById.get(sessionId);
        if (s == null) {
            return;
        }
        s.isClosing = true;
    }

    synchronized public void removeSession(long sessionId) {
        SessionImpl s = sessionsById.remove(sessionId);
        sessionsWithTimeout.remove(sessionId);
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                    "SessionTrackerImpl --- Removing session 0x"
                    + Long.toHexString(sessionId));
        }
        if (s != null) {
            SessionSet set = sessionSets.get(s.tickTime);
            // Session expiration has been removing the sessions   
            if(set != null){
                set.sessions.remove(s);
            }
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");

        running = false;
        if (LOG.isTraceEnabled()) {
            ZooTrace.logTraceMessage(LOG, ZooTrace.getTextTraceLevel(),
                                     "Shutdown SessionTrackerImpl!");
        }
    }


    synchronized public long createSession(int sessionTimeout) {
        // 完成三项工作
        addSession(nextSessionId, sessionTimeout);
        return nextSessionId++;
    }

    synchronized public void addSession(long id, int sessionTimeout) {
        sessionsWithTimeout.put(id, sessionTimeout);
        if (sessionsById.get(id) == null) {
            // 创建一个session
            SessionImpl s = new SessionImpl(id, sessionTimeout, 0);
            // 放入缓存map
            sessionsById.put(id, s);
            if (LOG.isTraceEnabled()) {
                ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                        "SessionTrackerImpl --- Adding session 0x"
                        + Long.toHexString(id) + " " + sessionTimeout);
            }
        } else {
            if (LOG.isTraceEnabled()) {
                ZooTrace.logTraceMessage(LOG, ZooTrace.SESSION_TRACE_MASK,
                        "SessionTrackerImpl --- Existing session 0x"
                        + Long.toHexString(id) + " " + sessionTimeout);
            }
        }
        // 将创建的session放入相应的会话桶
        touchSession(id, sessionTimeout);
    }

    synchronized public void checkSession(long sessionId, Object owner) throws KeeperException.SessionExpiredException, KeeperException.SessionMovedException {
        SessionImpl session = sessionsById.get(sessionId);
        if (session == null || session.isClosing()) {
            throw new KeeperException.SessionExpiredException();
        }
        if (session.owner == null) {
            session.owner = owner;
        } else if (session.owner != owner) {
            throw new KeeperException.SessionMovedException();
        }
    }

    synchronized public void setOwner(long id, Object owner) throws SessionExpiredException {
        SessionImpl session = sessionsById.get(id);
        if (session == null || session.isClosing()) {
            throw new KeeperException.SessionExpiredException();
        }
        session.owner = owner;
    }
}
