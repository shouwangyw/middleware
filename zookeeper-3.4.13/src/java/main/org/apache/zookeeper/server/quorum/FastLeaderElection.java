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


package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.jmx.MBeanRegistry;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.quorum.QuorumCnxManager.Message;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.QuorumPeer.QuorumServer;
import org.apache.zookeeper.server.quorum.QuorumPeer.ServerState;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation of leader election using TCP. It uses an object of the class
 * QuorumCnxManager to manage connections. Otherwise, the algorithm[ˈælɡəˌrɪðəm]
 * is push-based as with the other UDP implementations.
 * 翻译：使用TCP实现了Leader的选举。它使用QuorumCnxManager类的对象进行连接管理
 * (与其它Server间的连接管理)。否则(即若不使用QuorumCnxManager对象的话)，将使用
 * UDP的基于推送的算法实现。
 *
 * There are a few parameters that can be tuned to change its behavior. First,
 * finalizeWait determines the amount of time to wait until deciding upon a leader.
 * This is part of the leader election algorithm.
 * 翻译：有几个参数可以用来改变它(选举)的行为。第一，finalizeWait(这是一个代码中的常量)
 * 决定了选举出一个Leader的时间，这是Leader选举算法的一部分。
 */
public class FastLeaderElection implements Election {
    private static final Logger LOG = LoggerFactory.getLogger(FastLeaderElection.class);

    /**
     * Determine how much time a process has to wait
     * once(一经，一旦) it believes(到达) that it has reached the end of
     * leader election.
     * 翻译：(该常量)决定一个(选举)过程需要等待的选举时间。
     * 一经到达，它将结束Leader选举。
     */
    // 当前Server发出“通知”后需要等待其它Server向其发送它们的“通知”，
    // 这个常量就是等待的超时时限，注意，这是这个超时时限的初始值
    final static int finalizeWait = 200;


    /**
     * Upper bound on the amount of time between two consecutive(连续的)
     * notification checks. This impacts(影响) the amount of time to get
     * the system up again after long partitions(分割). Currently 60 seconds.
     * 翻译：(该常量指定了)两个连续的notification检查间的时间间隔上限。
     * 它影响了系统在经历了长时间分割后再次重启的时间。目前60秒。
     */
    // 该常量是前面常量所述的超时时限的最大值
    final static int maxNotificationInterval = 60000;

    /**
     * Connection manager. Fast leader election uses TCP for
     * communication between peers, and QuorumCnxManager manages
     * such connections.
     * 翻译：连接管理者。FastLeaderElection(选举算法)使用TCP(管理)
     * 两个同辈Server的通信，并且QuorumCnxManager还管理着这些连接。
     */
    QuorumCnxManager manager;


    /**
     * Notifications are messages that let other peers know that
     * a given peer has changed its vote, either because it has
     * joined leader election or because it learned of(领先于)
     * another peer with higher zxid or same zxid and higher
     * server id
     * 翻译：Notifications是一个让其它Server知道当前Server已经改变
     * 了投票的通知消息，(为什么它要改变投票呢？)要么是因为它参与了
     * Leader选举(新一轮的投票，首先投向自己)，要么是它具有更大的
     * zxid，或者zxid相同但ServerId更大。
     */
    static public class Notification {
        /*
         * Format version, introduced in 3.4.6
         */
        public final static int CURRENTVERSION = 0x1;
        int version;
        /*
         * Proposed leader：当前选票所推荐的Leader的sid
         */
        long leader;
        /*
         * zxid of the proposed leader：当前选票所推荐的Leader的最大zxid
         */
        long zxid;
        /*
         * Epoch：本轮选举的epoch
         */
        long electionEpoch;
        /*
         * current state of sender：当前通知发送者的状态
         */
        QuorumPeer.ServerState state;
        /*
         * Address of sender：当前通知发送者的sid
         */
        long sid;
        /*
         * epoch of the proposed leader：当前选票所推荐Leader的epoch
         */
        long peerEpoch;

        @Override
        public String toString() {
            return Long.toHexString(version) + " (message format version), "
                    + leader + " (n.leader), 0x"
                    + Long.toHexString(zxid) + " (n.zxid), 0x"
                    + Long.toHexString(electionEpoch) + " (n.round), " + state
                    + " (n.state), " + sid + " (n.sid), 0x"
                    + Long.toHexString(peerEpoch) + " (n.peerEpoch) ";
        }
    }
    
    static ByteBuffer buildMsg(int state,
            long leader,
            long zxid,
            long electionEpoch,
            long epoch) {
        byte requestBytes[] = new byte[40];
        ByteBuffer requestBuffer = ByteBuffer.wrap(requestBytes);

        /*
         * Building notification packet to send 
         */

        requestBuffer.clear();
        requestBuffer.putInt(state);
        requestBuffer.putLong(leader);
        requestBuffer.putLong(zxid);
        requestBuffer.putLong(electionEpoch);
        requestBuffer.putLong(epoch);
        requestBuffer.putInt(Notification.CURRENTVERSION);
        
        return requestBuffer;
    }

    /**
     * Messages that a peer wants to send to other peers.
     * These messages can be both Notifications and Acks
     * of reception of notification.
     */
    static public class ToSend {
        static enum mType {crequest, challenge, notification, ack}

        ToSend(mType type,
                long leader,
                long zxid,
                long electionEpoch,
                ServerState state,
                long sid,
                long peerEpoch) {

            this.leader = leader;
            this.zxid = zxid;
            this.electionEpoch = electionEpoch;
            this.state = state;
            this.sid = sid;
            this.peerEpoch = peerEpoch;
        }

        /*
         * Proposed leader in the case of notification
         */
        long leader;

        /*
         * id contains the tag for acks, and zxid for notifications
         */
        long zxid;

        /*
         * Epoch
         */
        long electionEpoch;

        /*
         * Current state;
         */
        QuorumPeer.ServerState state;

        /*
         * Address of recipient
         */
        long sid;
        
        /*
         * Leader epoch
         */
        long peerEpoch;
    }

    LinkedBlockingQueue<ToSend> sendqueue;
    LinkedBlockingQueue<Notification> recvqueue;

    /**
     * Multi-threaded implementation of message handler. Messenger
     * implements two sub-classes: WorkReceiver and  WorkSender. The
     * functionality of each is obvious from the name. Each of these
     * spawns a new thread.
     */

    protected class Messenger {

        /**
         * Receives messages from instance of QuorumCnxManager on
         * method run(), and processes such messages.
         */

        class WorkerReceiver extends ZooKeeperThread {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerReceiver(QuorumCnxManager manager) {
                super("WorkerReceiver");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {

                Message response;
                while (!stop) {
                    // Sleeps on receive
                    try{
                        response = manager.pollRecvQueue(3000, TimeUnit.MILLISECONDS);
                        if(response == null) continue;

                        /*
                         * If it is from an observer, respond right away.
                         * Note that the following predicate assumes that
                         * if a server is not a follower, then it must be
                         * an observer. If we ever have any other type of
                         * learner in the future, we'll have to change the
                         * way we check for observers.
                         */
                        if(!validVoter(response.sid)){
                            Vote current = self.getCurrentVote();
                            ToSend notmsg = new ToSend(ToSend.mType.notification,
                                    current.getId(),
                                    current.getZxid(),
                                    logicalclock.get(),
                                    self.getPeerState(),
                                    response.sid,
                                    current.getPeerEpoch());

                            sendqueue.offer(notmsg);
                        } else {
                            // Receive new message
                            if (LOG.isDebugEnabled()) {
                                LOG.debug("Receive new notification message. My id = "
                                        + self.getId());
                            }

                            /*
                             * We check for 28 bytes for backward compatibility
                             */
                            if (response.buffer.capacity() < 28) {
                                LOG.error("Got a short response: "
                                        + response.buffer.capacity());
                                continue;
                            }
                            boolean backCompatibility = (response.buffer.capacity() == 28);
                            response.buffer.clear();

                            // Instantiate Notification and set its attributes
                            Notification n = new Notification();
                            
                            // State of peer that sent this message
                            QuorumPeer.ServerState ackstate = QuorumPeer.ServerState.LOOKING;
                            switch (response.buffer.getInt()) {
                            case 0:
                                ackstate = QuorumPeer.ServerState.LOOKING;
                                break;
                            case 1:
                                ackstate = QuorumPeer.ServerState.FOLLOWING;
                                break;
                            case 2:
                                ackstate = QuorumPeer.ServerState.LEADING;
                                break;
                            case 3:
                                ackstate = QuorumPeer.ServerState.OBSERVING;
                                break;
                            default:
                                continue;
                            }
                            
                            n.leader = response.buffer.getLong();
                            n.zxid = response.buffer.getLong();
                            n.electionEpoch = response.buffer.getLong();
                            n.state = ackstate;
                            n.sid = response.sid;
                            if(!backCompatibility){
                                n.peerEpoch = response.buffer.getLong();
                            } else {
                                if(LOG.isInfoEnabled()){
                                    LOG.info("Backward compatibility mode, server id=" + n.sid);
                                }
                                n.peerEpoch = ZxidUtils.getEpochFromZxid(n.zxid);
                            }

                            /*
                             * Version added in 3.4.6
                             */

                            n.version = (response.buffer.remaining() >= 4) ? 
                                         response.buffer.getInt() : 0x0;

                            /*
                             * Print notification info
                             */
                            if(LOG.isInfoEnabled()){
                                printNotification(n);
                            }

                            /*
                             * If this server is looking, then send proposed leader
                             */

                            if(self.getPeerState() == QuorumPeer.ServerState.LOOKING){
                                recvqueue.offer(n);

                                /*
                                 * Send a notification back if the peer that sent this
                                 * message is also looking and its logical clock is
                                 * lagging behind.
                                 */
                                if((ackstate == QuorumPeer.ServerState.LOOKING)
                                        && (n.electionEpoch < logicalclock.get())){
                                    Vote v = getVote();
                                    ToSend notmsg = new ToSend(ToSend.mType.notification,
                                            v.getId(),
                                            v.getZxid(),
                                            logicalclock.get(),
                                            self.getPeerState(),
                                            response.sid,
                                            v.getPeerEpoch());
                                    sendqueue.offer(notmsg);
                                }
                            } else {
                                /*
                                 * If this server is not looking, but the one that sent the ack
                                 * is looking, then send back what it believes to be the leader.
                                 */
                                Vote current = self.getCurrentVote();
                                if(ackstate == QuorumPeer.ServerState.LOOKING){
                                    if(LOG.isDebugEnabled()){
                                        LOG.debug("Sending new notification. My id =  " +
                                                self.getId() + " recipient=" +
                                                response.sid + " zxid=0x" +
                                                Long.toHexString(current.getZxid()) +
                                                " leader=" + current.getId());
                                    }
                                    
                                    ToSend notmsg;
                                    if(n.version > 0x0) {
                                        notmsg = new ToSend(
                                                ToSend.mType.notification,
                                                current.getId(),
                                                current.getZxid(),
                                                current.getElectionEpoch(),
                                                self.getPeerState(),
                                                response.sid,
                                                current.getPeerEpoch());
                                        
                                    } else {
                                        Vote bcVote = self.getBCVote();
                                        notmsg = new ToSend(
                                                ToSend.mType.notification,
                                                bcVote.getId(),
                                                bcVote.getZxid(),
                                                bcVote.getElectionEpoch(),
                                                self.getPeerState(),
                                                response.sid,
                                                bcVote.getPeerEpoch());
                                    }
                                    sendqueue.offer(notmsg);
                                }
                            }
                        }
                    } catch (InterruptedException e) {
                        System.out.println("Interrupted Exception while waiting for new message" +
                                e.toString());
                    }
                }
                LOG.info("WorkerReceiver is down");
            }
        }


        /**
         * This worker simply dequeues a message to send and
         * and queues it on the manager's queue.
         */

        class WorkerSender extends ZooKeeperThread {
            volatile boolean stop;
            QuorumCnxManager manager;

            WorkerSender(QuorumCnxManager manager){
                super("WorkerSender");
                this.stop = false;
                this.manager = manager;
            }

            public void run() {
                while (!stop) {
                    try {
                        ToSend m = sendqueue.poll(3000, TimeUnit.MILLISECONDS);
                        if(m == null) continue;

                        process(m);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
                LOG.info("WorkerSender is down");
            }

            /**
             * Called by run() once there is a new message to send.
             *
             * @param m     message to send
             */
            void process(ToSend m) {
                ByteBuffer requestBuffer = buildMsg(m.state.ordinal(), 
                                                        m.leader,
                                                        m.zxid, 
                                                        m.electionEpoch, 
                                                        m.peerEpoch);
                manager.toSend(m.sid, requestBuffer);
            }
        }


        WorkerSender ws;
        WorkerReceiver wr;

        /**
         * Constructor of class Messenger.
         *
         * @param manager   Connection manager
         */
        Messenger(QuorumCnxManager manager) {

            this.ws = new WorkerSender(manager);

            Thread t = new Thread(this.ws,
                    "WorkerSender[myid=" + self.getId() + "]");
            t.setDaemon(true);
            t.start();

            this.wr = new WorkerReceiver(manager);

            t = new Thread(this.wr,
                    "WorkerReceiver[myid=" + self.getId() + "]");
            t.setDaemon(true);
            t.start();
        }

        /**
         * Stops instances of WorkerSender and WorkerReceiver
         */
        void halt(){
            this.ws.stop = true;
            this.wr.stop = true;
        }

    }

    // 表示当前参与选举的Server
    QuorumPeer self;

    Messenger messenger;

    // 逻辑时钟，一轮新的选举的标志
    AtomicLong logicalclock = new AtomicLong(); /* Election instance */

    // 记录当前Server的推荐情况
    long proposedLeader;
    long proposedZxid;
    long proposedEpoch;


    /**
     * Returns the current vlue of the logical clock counter
     */
    public long getLogicalClock(){
        return logicalclock.get();
    }

    /**
     * Constructor of FastLeaderElection. It takes two parameters, one
     * is the QuorumPeer object that instantiated this object, and the other
     * is the connection manager. Such an object should be created only once
     * by each peer during an instance of the ZooKeeper service.
     *
     * @param self  QuorumPeer that created this object
     * @param manager   Connection manager
     */
    public FastLeaderElection(QuorumPeer self, QuorumCnxManager manager){
        this.stop = false;
        this.manager = manager;
        starter(self, manager);
    }

    /**
     * This method is invoked by the constructor. Because it is a
     * part of the starting procedure of the object that must be on
     * any constructor of this class, it is probably best to keep as
     * a separate method. As we have a single constructor currently,
     * it is not strictly necessary to have it separate.
     *
     * @param self      QuorumPeer that created this object
     * @param manager   Connection manager
     */
    private void starter(QuorumPeer self, QuorumCnxManager manager) {
        this.self = self;
        proposedLeader = -1;
        proposedZxid = -1;

        sendqueue = new LinkedBlockingQueue<ToSend>();
        recvqueue = new LinkedBlockingQueue<Notification>();
        this.messenger = new Messenger(manager);
    }

    private void leaveInstance(Vote v) {
        if(LOG.isDebugEnabled()){
            LOG.debug("About to leave FLE instance: leader="
                + v.getId() + ", zxid=0x" +
                Long.toHexString(v.getZxid()) + ", my id=" + self.getId()
                + ", my state=" + self.getPeerState());
        }
        recvqueue.clear();
    }

    public QuorumCnxManager getCnxManager(){
        return manager;
    }

    volatile boolean stop;
    public void shutdown(){
        stop = true;
        LOG.debug("Shutting down connection manager");
        manager.halt();
        LOG.debug("Shutting down messenger");
        messenger.halt();
        LOG.debug("FLE is down");
    }


    /**
     * Send notifications to all peers upon a change in our vote
     */
    // 向集群中的所有其他Server广播其投票信息
    private void sendNotifications() {
        // self.getVotingView().values()获取到了所有可以进行选举投票的server
        for (QuorumServer server : self.getVotingView().values()) {
            // 获取当前遍历对象，即通知要发送的接收者的sid
            long sid = server.id;

            // 创建notification message数据结构
            ToSend notmsg = new ToSend(ToSend.mType.notification,
                    proposedLeader,
                    proposedZxid,
                    logicalclock.get(), // 当前选举的逻辑时钟
                    QuorumPeer.ServerState.LOOKING,
                    sid,    // 当前遍历对象的sid，即接收者的sid
                    proposedEpoch);
            if(LOG.isDebugEnabled()){
                LOG.debug("Sending Notification: " + proposedLeader + " (n.leader), 0x"  +
                      Long.toHexString(proposedZxid) + " (n.zxid), 0x" + Long.toHexString(logicalclock.get())  +
                      " (n.round), " + sid + " (recipient), " + self.getId() +
                      " (myid), 0x" + Long.toHexString(proposedEpoch) + " (n.peerEpoch)");
            }
            // 将当前数据结构 notmsg 写入到发送队列，并发送
            sendqueue.offer(notmsg);
        }
    }

    private void printNotification(Notification n){
        LOG.info("Notification: " + n.toString()
                + self.getPeerState() + " (my state)");
    }

    /**
     * Check if a pair (server id, zxid) succeeds our
     * current vote.
     *
     * @param id    Server identifier
     * @param zxid  Last zxid observed by the issuer of this vote
     */
    // 比较new选票与cur选票谁更适合做Leader，若new更适合，则返回true，否则返回false
    protected boolean totalOrderPredicate(long newId, long newZxid, long newEpoch, long curId, long curZxid, long curEpoch) {
        LOG.debug("id: " + newId + ", proposed id: " + curId + ", zxid: 0x" +
                Long.toHexString(newZxid) + ", proposed zxid: 0x" + Long.toHexString(curZxid));
        // 判断new主机的权重是否为0。注意，只有Observer的权重才是0
        // 即判断new主机是否是Observer
        if(self.getQuorumVerifier().getWeight(newId) == 0){
            return false;
        }
        
        /*
         * We return true if one of the following three cases hold:
         * 1- New epoch is higher
         * 2- New epoch is the same as current epoch, but new zxid is higher
         * 3- New epoch is the same as current epoch, new zxid is the same
         *  as current zxid, but server id is higher.
         */
        // Leader产生的比较规则
        return ((newEpoch > curEpoch) || 
                ((newEpoch == curEpoch) &&
                ((newZxid > curZxid) || ((newZxid == curZxid) && (newId > curId)))));
    }

    /**
     * Termination predicate. Given a set of votes, determines if
     * have sufficient to declare the end of the election round.
     *
     *  @param votes    Set of votes
     *  @param l        Identifier of the vote received last
     *  @param zxid     zxid of the the vote received last
     */
    protected boolean termPredicate(
            HashMap<Long, Vote> votes,
            Vote vote) {

        HashSet<Long> set = new HashSet<Long>();

        /*
         * First make the views consistent. Sometimes peers will have
         * different zxids for a server depending on timing.
         */
        // 遍历所有投票
        for (Map.Entry<Long,Vote> entry : votes.entrySet()) {
            if (vote.equals(entry.getValue())){
                set.add(entry.getKey());
            }
        }
        // 判断当前set集合中的元素数量是否过半（zk集群数量的一半）
        return self.getQuorumVerifier().containsQuorum(set);
    }

    /**
     * In the case there is a leader elected, and a quorum supporting
     * this leader, we have to check if the leader has voted and acked
     * that it is leading. We need this check to avoid that peers keep
     * electing over and over a peer that has crashed and it is no
     * longer leading.
     *
     * @param votes set of votes
     * @param   leader  leader id
     * @param   electionEpoch   epoch id
     */
    protected boolean checkLeader(
            HashMap<Long, Vote> votes,
            long leader,
            long electionEpoch){

        boolean predicate = true;

        /*
         * If everyone else thinks I'm the leader, I must be the leader.
         * The other two checks are just for the case in which I'm not the
         * leader. If I'm not the leader and I haven't received a message
         * from leader stating that it is leading, then predicate is false.
         */

        if(leader != self.getId()){
            if(votes.get(leader) == null) predicate = false;
            else if(votes.get(leader).getState() != ServerState.LEADING) predicate = false;
        } else if(logicalclock.get() != electionEpoch) {
            predicate = false;
        } 

        return predicate;
    }
    
    /**
     * This predicate checks that a leader has been elected. It doesn't
     * make a lot of sense without context (check lookForLeader) and it
     * has been separated for testing purposes.
     * 
     * @param recv  map of received votes 
     * @param ooe   map containing out of election votes (LEADING or FOLLOWING)
     * @param n     Notification
     * @return          
     */
    // 在当前选票所推荐的Leader支持率过半的前提下
    // 若Leader的状态没有问题，则返回true
    protected boolean ooePredicate(HashMap<Long,Vote> recv, 
                                    HashMap<Long,Vote> ooe, 
                                    Notification n) {
        
        return (termPredicate(recv, new Vote(n.version, 
                                             n.leader,
                                             n.zxid, 
                                             n.electionEpoch, 
                                             n.peerEpoch, 
                                             n.state))
                && checkLeader(ooe, n.leader, n.electionEpoch));
        
    }

    // 更新提案
    synchronized void updateProposal(long leader, long zxid, long epoch){
        if(LOG.isDebugEnabled()){
            LOG.debug("Updating proposal: " + leader + " (newleader), 0x"
                    + Long.toHexString(zxid) + " (newzxid), " + proposedLeader
                    + " (oldleader), 0x" + Long.toHexString(proposedZxid) + " (oldzxid)");
        }
        // 更新当前server对于leader的推荐信息
        proposedLeader = leader;
        proposedZxid = zxid;
        proposedEpoch = epoch;
    }

    synchronized Vote getVote(){
        return new Vote(proposedLeader, proposedZxid, proposedEpoch);
    }

    /**
     * A learning state can be either FOLLOWING or OBSERVING.
     * This method simply decides which one depending on the
     * role of the server.
     *
     * @return ServerState
     */
    private ServerState learningState(){
        if(self.getLearnerType() == LearnerType.PARTICIPANT){
            LOG.debug("I'm a participant: " + self.getId());
            return ServerState.FOLLOWING;
        }
        else{
            LOG.debug("I'm an observer: " + self.getId());
            return ServerState.OBSERVING;
        }
    }

    /**
     * 用于获取当前server的id
     * Returns the initial vote value of server identifier.
     *
     * @return long
     */
    private long getInitId(){
        if(self.getLearnerType() == LearnerType.PARTICIPANT)
            return self.getId();
        else return Long.MIN_VALUE;
    }

    /**
     * 获取当前server记录的zxid，即max-zxid
     * Returns initial last logged zxid.
     *
     * @return long
     */
    private long getInitLastLoggedZxid(){
        if(self.getLearnerType() == LearnerType.PARTICIPANT)
            return self.getLastLoggedZxid();
        else return Long.MIN_VALUE;
    }

    /**
     * 返回当前server的epoch
     * Returns the initial vote value of the peer epoch.
     *
     * @return long
     */
    private long getPeerEpoch(){
        if(self.getLearnerType() == LearnerType.PARTICIPANT)
        	try {
        		return self.getCurrentEpoch();
        	} catch(IOException e) {
        		RuntimeException re = new RuntimeException(e.getMessage());
        		re.setStackTrace(e.getStackTrace());
        		throw re;
        	}
        else return Long.MIN_VALUE;
    }

    /**
     * Starts a new round of leader election. Whenever our QuorumPeer
     * changes its state to LOOKING, this method is invoked, and it
     * sends notifications to all other peers.
     * 翻译：开启新一轮的Leader选举。无论何时，只要我们的QuorumPeer的
     * 状态变为了LOOKING，那么这个方法将被调用，并且它会发送notifications
     * 给所有其它的同级服务器。
     */
    // Observer会调用吗？答案是肯定的
    public Vote lookForLeader() throws InterruptedException {
        // --------------- 1、选举前的准备工作 -----------------
        // jmx，Java Management eXtensions，是一种分布式应用程序监控技术（Oracle提供的）
        try {
            self.jmxLeaderElectionBean = new LeaderElectionBean();
            MBeanRegistry.getInstance().register(
                    self.jmxLeaderElectionBean, self.jmxLocalPeerBean);
        } catch (Exception e) {
            LOG.warn("Failed to register with JMX", e);
            self.jmxLeaderElectionBean = null;
        }
        if (self.start_fle == 0) {
            // 记录选举的开始时间点，单位毫秒
            self.start_fle = Time.currentElapsedTime();
        }
        try {
            // 记录当前Server收到的其他Server发送的本轮投票
            // 后续票数统计就是统计的这个集合中的选票
            // key是投票者的sid，value则是选票
            HashMap<Long, Vote> recvset = new HashMap<Long, Vote>();

            // out of: 退出
            // 用于存放非法选票
            HashMap<Long, Vote> outofelection = new HashMap<Long, Vote>();

            // 注意这里的 notTimeout 是 notification timeout
            int notTimeout = finalizeWait;

            // ----------------- 2、将自己作为初始化Leader投出去 ----------------
            synchronized(this){
                // 使逻辑时钟加一
                logicalclock.incrementAndGet();
                // 将自己作为Leader，更新提案
                // getInitId()：获取当前Server的id
                // getInitLastLoggedZxid()：获取当前Server所记录的最大的zxid
                // getPeerEpoch()：获取当前Server所记录的epoch
                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
            }

            LOG.info("New election. My id =  " + self.getId() +
                    ", proposed zxid=0x" + Long.toHexString(proposedZxid));
            // 向集群中的所有其他Server广播其投票信息
            sendNotifications();

            // -------------- 3、比较自己的选票与接收到的其它Server的选票，谁更合适做Leader ----------
            /*
             * Loop in which we exchange notifications until we find a leader
             * 在循环中我们交换 notifications 直到我们找到一个 Leader
             * 循环条件：当前 Server 的状态为LOOKING，并且本次选举未结束
             */
            while ((self.getPeerState() == ServerState.LOOKING) &&
                    (!stop)){
                /*
                 * Remove next notification from queue, times out after 2 times the termination time
                 * 从队列中移除一个通知，超时后使终止时间扩大2倍
                 */
                Notification n = recvqueue.poll(notTimeout, TimeUnit.MILLISECONDS);
                /*
                 * Sends more notifications if haven't received enough.
                 * Otherwise processes new notification.
                 */
                if(n == null){
                    // 若当前主机的消息全部交付(delivered)，说明当前主机与集群没有失联
                    if(manager.haveDelivered()){
                        // 将自己的选票广播出去
                        sendNotifications(); // 没有失联，但n=null，选票没有收齐，需要重发

                    // 若当前主机与集群失联
                    } else {
                        // 让当前主机与所有其它主机进行连接
                        manager.connectAll(); // 虽然失联了，但不需要重发，只需要重连即可
                    }
                    /*
                     * Exponential backoff
                     */
                    int tmpTimeOut = notTimeout*2;
                    notTimeout = (tmpTimeOut < maxNotificationInterval?
                            tmpTimeOut : maxNotificationInterval);
                    LOG.info("Notification time out: " + notTimeout);
                }
                // 若n不为null，则验证n的合法性
                else if(validVoter(n.sid) && validVoter(n.leader)) {
                    switch (n.state) {
                    // region - LOOKING
                    case LOOKING:
                        // If notification > current, replace and send messages out
                        // n的epoch比当前主机的逻辑时钟大
                        if (n.electionEpoch > logicalclock.get()) {
                            // 将n的epoch作为当前主机的 logicalclock
                            logicalclock.set(n.electionEpoch);
                            // 清空recvset集合
                            recvset.clear();
                            // 因为以前的投票都作废了，所以又要开始新一轮的投票了，那么当前主机首先要看一下
                            // 自己与n比较，是否更适合做新一轮的Leader
                            if(totalOrderPredicate(n.leader, n.zxid, n.peerEpoch,
                                    getInitId(), getInitLastLoggedZxid(), getPeerEpoch())) {
                                updateProposal(n.leader, n.zxid, n.peerEpoch);
                            } else {
                                updateProposal(getInitId(), getInitLastLoggedZxid(), getPeerEpoch());
                            }
                            // 将新的选票广播出去
                            sendNotifications();
                            // n的epoch比当前主机的逻辑时钟小
                        } else if (n.electionEpoch < logicalclock.get()) {
                            if(LOG.isDebugEnabled()){
                                LOG.debug("Notification election epoch is smaller than logicalclock. n.electionEpoch = 0x"
                                        + Long.toHexString(n.electionEpoch)
                                        + ", logicalclock=0x" + Long.toHexString(logicalclock.get()));
                            }
                            break;
                            // n的epoch与当前主机的逻辑时钟相同
                        } else if (totalOrderPredicate(n.leader, n.zxid, n.peerEpoch,
                                proposedLeader, proposedZxid, proposedEpoch)) {
                            // 若通知n的选票更适合做Leader，则当前主机更新提案，并广播出去
                            updateProposal(n.leader, n.zxid, n.peerEpoch);
                            sendNotifications();
                        }
                        if(LOG.isDebugEnabled()){
                            LOG.debug("Adding vote: from=" + n.sid +
                                    ", proposed leader=" + n.leader +
                                    ", proposed zxid=0x" + Long.toHexString(n.zxid) +
                                    ", proposed election epoch=0x" + Long.toHexString(n.electionEpoch));
                        }

                        // new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch)在这里的作用是
                        // 将通知 notification 转化为选票 vote
                        // 将这个从 recvqueue 队列中取出的“选票”存放到 recvset 集合中，以备后面选票数量统计
                        recvset.put(n.sid, new Vote(n.leader, n.zxid, n.electionEpoch, n.peerEpoch));

                        // 4、判断当前选举是否可以结束了：当前的选票若在recvset中有过半的支持，则结束选举
                        if (termPredicate(recvset,
                                new Vote(proposedLeader, proposedZxid,
                                        logicalclock.get(), proposedEpoch))) {

                            // Verify if there is any change in the proposed leader
                            // 循环遍历剩下的通知n，判断是否存在更适合作Leader的通知
                            // 该while循环有两个出口：
                            // 一个出口是while循环条件处，该出口出去后，n为null，说明剩余的通知中没有比当前推荐
                            // 更适合作Leader的，那么我们当前的推荐就是最终的Leader
                            // 另一个出口是break，该出口出去后，n不是null，说明剩余的通知中存在比当前推荐更适合
                            // 作Leader的，那么，需要重新再统计
                            while((n = recvqueue.poll(finalizeWait,
                                    TimeUnit.MILLISECONDS)) != null){
                                if(totalOrderPredicate(n.leader, n.zxid, n.peerEpoch,
                                        proposedLeader, proposedZxid, proposedEpoch)){
                                    // 当前通知n（剩余的通知n之一）比当前推荐更适合，则将n再放回到recvqueue队列
                                    recvqueue.put(n);
                                    break;
                                }
                            }

                            /**
                             * 若 n 为 null，则说当前server所推荐的是最适合做Leader的
                             * This predicate is true once we don't read any new
                             * relevant message from the reception queue
                             */
                            if (n == null) {
                                // 修改当前主机的状态
                                self.setPeerState((proposedLeader == self.getId()) ?
                                        ServerState.LEADING: learningState());

                                // 形成最终选票
                                Vote endVote = new Vote(proposedLeader,
                                        proposedZxid,
                                        logicalclock.get(),
                                        proposedEpoch);
                                // 将recvqueue队列清空
                                leaveInstance(endVote);
                                return endVote;
                            }
                        }
                        break;
                    // endregion
                    // region - Not LOOKING
                    // -------------- 5、无需选举的情况 --------------
                    /*
                       前面已经校验了serverId，按理说不应该会有 case OBSERVING  这种情况有可能的原因是为了保证线程安全
                       Observer与Leader间的连接时通过心跳来维护的，这个心跳内容就是"通知"。当Observer发出了心跳(通知)后，
                       在Leader还未接收到之前，Leader主动出让了领导权，由LEADING状态变为了LOOKING状态，此时其就会调用lookForLeader()
                       方法，向外发送通知，并接收到很多来自其他server的通知，其中就包含先前未收到的Observer所发送的通知，
                       该通知的 n.state 为 OBSERVING。
                       若一个server能够接收到n.state为OBSERVING的通知，说明该Server是刚刚挂掉的Leader
                    */
                    case OBSERVING:
                        LOG.debug("Notification from observer: " + n.sid);
                        break;
                    /*
                       以下三种场景会接收到Follower或Leader发送来的通知：
                       场景一：当有新的主机(非Observer)要加入一个正常运行的集群时，其初始状态为LOOKING，其会调用lookForLeader()，
                            然后向所有集群中的主机发送通知。当Leader、Follower接收到该主机所发送的通知后，它们就会向其回复通知，此时
                            的通知状态就是FOLLOWING或LEADING
                       场景二：当Leader挂了，并不是所有Follower同时感知到的，会有个先后顺序。对于先感知到Leader挂了的主机，其状态马上
                            会变为LOOKING，然后向其他主机发送通知。其它主机由于还没有感知到Leader已经挂了，所以它们的状态仍为FOLLOWING。
                            所以它们在向这台主机回复的通知状态为FOLLOWING。
                       场景三：在本轮选举中已经选举出了新的Leader，但并不是所有participant同时都知道的，也会有个先后顺序。
                            新选举出的Leader的状态为LEADING，知道Leader选举出来的主机的状态为Following，但还不知道的主机状态仍为
                            LOOKING。此时的主机在向其它主机发送过通知后，接收到的通知就有LEADING、FOLLOWING状态的通知。
                    */
                    case FOLLOWING:
                    case LEADING:
                        /*
                         * Consider all notifications from the same epoch
                         * together.
                         * 一起考虑来自于同一个epoch的所有通知
                         * 注意：当前主机的状态为LOOKING，但现在处理的是来自于Leader或Follower的通知
                         * 现在处理的就是前述场景三的情况：新的Leader选举了出来，现在来通知当前主机
                         */
                        if(n.electionEpoch == logicalclock.get()){
                            // 注意：并不是这个通知就一定会加入"票箱"
                            // 若之前收到过该主机发送的通知，且其推荐的Leader也没有发生变化，那么该通知内容其实在票箱中已经存在。
                            // 只不过不同的是，可能以前那台主机的状态为LOOKING，现在为FOLLOWING或LEADING。
                            // 但向票箱中放入选票，并不关心投票者的状态，关心的是投票者的serverId与所推荐的Leader
                            recvset.put(n.sid, new Vote(n.leader, n.zxid,
                                    n.electionEpoch, n.peerEpoch));
                            // 判断当前通知所推荐的Leader在"票箱"中的支持率是否过半
                            // 若过半再判断Leader的状态是否正确。若正确，则退出选举，做收尾工作。
                            // 在这里我们真正关心的是什么？是通知发送者的状态码？不是！
                            // 我们关心的是通知所推荐的Leader到底是谁
                            if(ooePredicate(recvset, outofelection, n)) {
                                self.setPeerState((n.leader == self.getId()) ?
                                        ServerState.LEADING: learningState());
                                Vote endVote = new Vote(n.leader, n.zxid,
                                        n.electionEpoch, n.peerEpoch);
                                leaveInstance(endVote);
                                return endVote;
                            }
                        }
                        /*
                         * Before joining an established ensemble(确定的团队), verify
                         * a majority is following the same leader.
                         * 在加入到一个确定团队之前，确认大多数人跟随的是同一个Leader，
                         * 即以下代码用于处理前述场景一的情况：新的Server加入到已经存在的集群中
                         */
                        // 只要代码可以运行到这里，那么，这些通知一定要放入到outofelection集合。
                        // 为什么？因为这些通知的状态不是LEADING就是FOLLOWING
                        outofelection.put(n.sid, new Vote(n.version, n.leader, n.zxid,
                                n.electionEpoch, n.peerEpoch, n.state));
                        // 通过判断我们可以知道，当前通知所推荐的Leader在ooe中是否过半。
                        // 注意：ooe中的通知不是来自于Follower，就是来自于Leader，即都来自于那个确定的 zk 集群
                        if(ooePredicate(outofelection, outofelection, n)) {
                            synchronized(this){
                                // 同步Leader的逻辑时钟
                                logicalclock.set(n.electionEpoch);
                                self.setPeerState((n.leader == self.getId()) ?
                                        ServerState.LEADING: learningState());
                            }
                            Vote endVote = new Vote(n.leader, n.zxid,
                                    n.electionEpoch, n.peerEpoch);
                            leaveInstance(endVote);
                            return endVote;
                        }
                        break;
                    default:
                        LOG.warn("Notification state unrecognized: {} (n.state), {} (n.sid)",
                                n.state, n.sid);
                        break;
                    // endregion
                }
                    /*
                     * Only proceed if the vote comes from a replica in the
                     * voting view for a replica in the voting view.
                     */

                } else {
                    if (!validVoter(n.leader)) {
                        LOG.warn("Ignoring notification for non-cluster member sid {} from sid {}", n.leader, n.sid);
                    }
                    if (!validVoter(n.sid)) {
                        LOG.warn("Ignoring notification for sid {} from non-quorum member sid {}", n.leader, n.sid);
                    }
                }
            }
            return null;
        } finally {
            try {
                if(self.jmxLeaderElectionBean != null){
                    MBeanRegistry.getInstance().unregister(
                            self.jmxLeaderElectionBean);
                }
            } catch (Exception e) {
                LOG.warn("Failed to unregister with JMX", e);
            }
            self.jmxLeaderElectionBean = null;
            LOG.debug("Number of connection processing threads: {}",
                    manager.getConnectionThreadCount());
        }
    }

    /**
     * Check if a given sid is represented in either the current or
     * the next voting view
     *
     * @param sid     Server identifier
     * @return boolean
     */
    // 只要被判别的主机出现在QuorumServer中，其就是合法的
    private boolean validVoter(long sid) {
        return self.getVotingView().containsKey(sid);
    }
}
